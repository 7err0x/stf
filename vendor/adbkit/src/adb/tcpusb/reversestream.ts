import * as Net from 'net';
import { EventEmitter } from 'events';
import d from 'debug';
import Bluebird from 'bluebird';
import Packet from './packet';
import Socket from './socket';
import ReverseForwardManager from './reverseforwardmanager';

const debug = d('adb:tcpusb:reversestream');

type StreamState = 'OPENING' | 'ESTABLISHED' | 'CLOSING' | 'CLOSED';

export default class ReverseStream extends EventEmitter {
  private state: StreamState = 'OPENING';
  private remoteId = 0;
  private needAck = false;
  private timeoutTimer: NodeJS.Timeout | null = null;
  private pendingBuffer: Buffer = Buffer.alloc(0);

  constructor(
    private readonly socket: Socket,
    public readonly localId: number,
    private readonly localSpec: string,
    private readonly tcpSocket: Net.Socket,
    private readonly manager: ReverseForwardManager,
  ) {
    super();
  }

  public start(): void {
    this.tcpSocket.pause();
    const payload = Buffer.from(`${this.localSpec}\0`);
    debug(`O:A_OPEN localId=${this.localId} spec=${this.localSpec}`);
    this.socket.write(Packet.assemble(Packet.A_OPEN, this.localId, 0, payload));

    this.timeoutTimer = setTimeout(() => {
      if (this.state === 'OPENING') {
        debug(`ReverseStream localId=${this.localId} timed out waiting for A_OKAY`);
        this.end();
      }
    }, 10000);
  }

  public handle(packet: Packet): Bluebird<boolean> {
    return Bluebird.try(() => {
      switch (packet.command) {
        case Packet.A_OKAY:
          return this._handleOkayPacket(packet);
        case Packet.A_WRTE:
          return this._handleWritePacket(packet);
        case Packet.A_CLSE:
          return this._handleClosePacket(packet);
        default:
          throw new Error(`Unexpected packet command ${packet.command} for ReverseStream`);
      }
    }).catch((err) => {
      debug(`ReverseStream localId=${this.localId} error:`, err);
      this.end();
      return false;
    });
  }

  private _handleOkayPacket(packet: Packet): boolean {
    debug(`I:A_OKAY localId=${this.localId} remoteId=${packet.arg0}`);
    if (this.state === 'CLOSED') {
      return false;
    }

    if (this.state === 'OPENING') {
      if (this.timeoutTimer) {
        clearTimeout(this.timeoutTimer);
        this.timeoutTimer = null;
      }
      this.remoteId = packet.arg0;
      this.state = 'ESTABLISHED';
      this._attachTcpListeners();
      this.tcpSocket.resume();
      return true;
    }

    if (this.state === 'ESTABLISHED') {
      this.needAck = false;
      this._flushPendingBuffer();
      return true;
    }

    return false;
  }

  private _handleWritePacket(packet: Packet): boolean {
    debug(`I:A_WRTE localId=${this.localId} remoteId=${packet.arg0}`);
    if (this.state !== 'ESTABLISHED') {
      return false;
    }

    if (packet.data && packet.data.length > 0) {
      this.tcpSocket.write(packet.data);
    }
    debug(`O:A_OKAY localId=${this.localId} remoteId=${this.remoteId}`);
    return this.socket.write(Packet.assemble(Packet.A_OKAY, this.localId, this.remoteId, null));
  }

  private _handleClosePacket(packet: Packet): boolean {
    debug(`I:A_CLSE localId=${this.localId} remoteId=${packet.arg0}`);
    this.end();
    return true;
  }

  private _attachTcpListeners(): void {
    this.tcpSocket.on('data', (chunk: Buffer) => {
      this._onTcpData(chunk);
    });

    this.tcpSocket.on('end', () => {
      debug(`TCP socket ended localId=${this.localId}`);
      this.end();
    });

    this.tcpSocket.on('error', (err) => {
      debug(`TCP socket error localId=${this.localId}:`, err);
      this.end();
    });
  }

  private _onTcpData(chunk: Buffer): void {
    this.pendingBuffer = Buffer.concat([this.pendingBuffer, chunk]);
    this._flushPendingBuffer();
  }

  private _flushPendingBuffer(): void {
    if (this.needAck || this.state !== 'ESTABLISHED' || this.pendingBuffer.length === 0) {
      return;
    }

    const maxPayload = this.socket.maxPayload || 4096;
    const chunkSize = Math.min(this.pendingBuffer.length, maxPayload);
    const toSend = this.pendingBuffer.slice(0, chunkSize);
    this.pendingBuffer = this.pendingBuffer.slice(chunkSize);

    debug(`O:A_WRTE localId=${this.localId} remoteId=${this.remoteId} bytes=${toSend.length}`);
    this.socket.write(Packet.assemble(Packet.A_WRTE, this.localId, this.remoteId, toSend));
    this.needAck = true;

    if (this.pendingBuffer.length > 0) {
      this.tcpSocket.pause();
    } else {
      this.tcpSocket.resume();
    }
  }

  public end(): void {
    if (this.state === 'CLOSED') {
      return;
    }
    this.state = 'CLOSED';

    if (this.timeoutTimer) {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = null;
    }

    debug(`O:A_CLSE localId=${this.localId} remoteId=${this.remoteId}`);
    try {
      this.socket.write(Packet.assemble(Packet.A_CLSE, this.localId, this.remoteId, null));
    } catch (err) {}

    try {
      this.tcpSocket.destroy();
    } catch (err) {}

    this.manager.removeStream(this.localId);
    this.emit('end');
  }
}
