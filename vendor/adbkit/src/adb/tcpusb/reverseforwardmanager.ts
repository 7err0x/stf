import * as Net from 'net';
import d from 'debug';
import Bluebird from 'bluebird';
import Client from '../client';
import Protocol from '../protocol';
import Socket from './socket';
import ReverseStream from './reversestream';
import SocketOptions from '../../SocketOptions';

const debug = d('adb:tcpusb:reverseforwardmanager');

interface ForwardEntry {
  deviceSpec: string;
  originalLocal: string;
  rewrittenPort: number;
  server: Net.Server;
}

export default class ReverseForwardManager {
  private forwards: Map<string, ForwardEntry> = new Map();
  private streams: Map<number, ReverseStream> = new Map();

  constructor(
    private readonly socket: Socket,
    private readonly client: Client,
    private readonly serial: string,
    private readonly options: SocketOptions = {},
  ) {}

  public get maxForwards(): number {
    return this.options.reverseMaxForwards || 32;
  }

  public get maxStreams(): number {
    return this.options.reverseMaxStreams || 64;
  }

  public handleForward(deviceSpec: string, localSpec: string): Bluebird<void> {
    return Bluebird.try(() => {
      if (!deviceSpec.startsWith('tcp:') || !localSpec.startsWith('tcp:')) {
        throw new Error('reverse: only tcp:<port> specs are supported by the tcpusb bridge');
      }

      if (this.forwards.size >= this.maxForwards) {
        throw new Error(`Maximum reverse forwards limit (${this.maxForwards}) reached`);
      }

      if (this.forwards.has(deviceSpec)) {
        return this.handleKillForward(deviceSpec);
      }
      return Bluebird.resolve();
    }).then(() => {
      return this._allocateServer();
    }).then(({ server, port }) => {
      const rewrittenService = `reverse:forward:${deviceSpec};tcp:${port}`;
      debug(`Registering rewritten service on adbd: ${rewrittenService}`);
      return this._executeReverseCommand(rewrittenService)
        .then(() => {
          const entry: ForwardEntry = {
            deviceSpec,
            originalLocal: localSpec,
            rewrittenPort: port,
            server,
          };
          this.forwards.set(deviceSpec, entry);

          server.on('connection', (tcpSocket: Net.Socket) => {
            this.openReverseStream(localSpec, tcpSocket);
          });

          debug(`Reverse forward established: ${deviceSpec} -> 127.0.0.1:${port} (client requested ${localSpec})`);
        })
        .catch((err) => {
          server.close();
          throw err;
        });
    });
  }

  public handleKillForward(deviceSpec: string): Bluebird<void> {
    const entry = this.forwards.get(deviceSpec);
    const serviceString = `reverse:killforward:${deviceSpec}`;
    return this._executeReverseCommand(serviceString)
      .catch((err) => {
        debug(`Ignored error killing forward ${deviceSpec}: ${err.message}`);
      })
      .finally(() => {
        if (entry) {
          entry.server.close();
          this.forwards.delete(deviceSpec);
          debug(`Killed reverse forward listener for ${deviceSpec}`);
        }
      });
  }

  public handleKillForwardAll(): Bluebird<void> {
    return this._executeReverseCommand('reverse:killforward-all')
      .catch((err) => {
        debug(`Ignored error killing all forwards: ${err.message}`);
      })
      .finally(() => {
        for (const entry of this.forwards.values()) {
          entry.server.close();
        }
        this.forwards.clear();
        debug('Killed all reverse forward listeners');
      });
  }

  public handleListForward(): Bluebird<Buffer> {
    return this._executeReverseCommandWithOutput('reverse:list-forward').then((rawOutput) => {
      const lines = rawOutput.toString('utf-8').split('\n');
      const rewrittenLines = lines.map((line) => {
        const parts = line.trim().split(/\s+/);
        if (parts.length >= 3) {
          const transport = parts[0];
          const devSpec = parts[1];
          const locSpec = parts[2];
          const match = locSpec.match(/^tcp:(\d+)$/);
          if (match) {
            const portNum = parseInt(match[1], 10);
            for (const entry of this.forwards.values()) {
              if (entry.rewrittenPort === portNum) {
                return `${transport} ${devSpec} ${entry.originalLocal}`;
              }
            }
          }
        }
        return line;
      });
      return Buffer.from(rewrittenLines.join('\n'), 'utf-8');
    });
  }

  public openReverseStream(localSpec: string, tcpSocket: Net.Socket): void {
    if (this.streams.size >= this.maxStreams) {
      debug(`Max reverse streams limit (${this.maxStreams}) reached, rejecting connection`);
      tcpSocket.destroy();
      return;
    }

    const ourLocalId = this.socket.remoteId.next();
    const stream = new ReverseStream(this.socket, ourLocalId, localSpec, tcpSocket, this);
    this.streams.set(ourLocalId, stream);
    debug(`Opened ReverseStream ourLocalId=${ourLocalId} localSpec=${localSpec}`);
    stream.start();
  }

  public getStreamByLocalId(id: number): ReverseStream | undefined {
    return this.streams.get(id);
  }

  public removeStream(id: number): void {
    this.streams.delete(id);
  }

  public end(): void {
    debug('Ending ReverseForwardManager');
    this.handleKillForwardAll();

    for (const stream of this.streams.values()) {
      stream.end();
    }
    this.streams.clear();
  }

  private _allocateServer(): Bluebird<{ server: Net.Server; port: number }> {
    return new Bluebird((resolve, reject) => {
      const server = Net.createServer({ allowHalfOpen: true });
      server.unref();

      server.on('error', (err) => {
        reject(err);
      });

      if (this.options.reversePortRange) {
        let minPort = 0;
        let maxPort = 0;
        if (Array.isArray(this.options.reversePortRange)) {
          [minPort, maxPort] = this.options.reversePortRange;
        } else if (typeof this.options.reversePortRange === 'string') {
          const match = this.options.reversePortRange.match(/^(\d+)-(\d+)$/);
          if (match) {
            minPort = parseInt(match[1], 10);
            maxPort = parseInt(match[2], 10);
          }
        }

        if (minPort > 0 && maxPort >= minPort) {
          let currentPort = minPort;
          const tryListen = () => {
            if (currentPort > maxPort) {
              return reject(new Error(`Exhausted reverse port range ${minPort}-${maxPort}`));
            }
            const p = currentPort++;
            const errorHandler = (err: any) => {
              if (err.code === 'EADDRINUSE') {
                server.removeListener('error', errorHandler);
                tryListen();
              } else {
                reject(err);
              }
            };
            server.once('error', errorHandler);
            server.listen(p, '127.0.0.1', () => {
              server.removeListener('error', errorHandler);
              resolve({ server, port: p });
            });
          };
          tryListen();
          return;
        }
      }

      server.listen(0, '127.0.0.1', () => {
        const address = server.address() as Net.AddressInfo;
        resolve({ server, port: address.port });
      });
    });
  }

  private _executeReverseCommand(serviceString: string): Bluebird<void> {
    return this.client
      .getDevice(this.serial)
      .transport()
      .then((transport) => {
        transport.write(Protocol.encodeData(serviceString));
        return transport.parser.readAscii(4).then((reply) => {
          switch (reply) {
            case Protocol.OKAY:
              return;
            case Protocol.FAIL:
              return transport.parser.readError().then((err: Error) => {
                throw err;
              });
            default:
              return transport.parser.unexpected(reply, 'OKAY or FAIL');
          }
        }).finally(() => {
          transport.end();
        });
      });
  }

  private _executeReverseCommandWithOutput(serviceString: string): Bluebird<Buffer> {
    return this.client
      .getDevice(this.serial)
      .transport()
      .then((transport) => {
        transport.write(Protocol.encodeData(serviceString));
        return transport.parser.readAscii(4).then((reply) => {
          switch (reply) {
            case Protocol.OKAY:
              return transport.parser.readAll();
            case Protocol.FAIL:
              return transport.parser.readError().then((err: Error) => {
                throw err;
              });
            default:
              return transport.parser.unexpected(reply, 'OKAY or FAIL');
          }
        }).finally(() => {
          transport.end();
        });
      });
  }
}
