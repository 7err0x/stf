import chai from 'chai';
import sinon from 'sinon';
import sinonChai from 'sinon-chai';
import Bluebird from 'bluebird';
import * as Net from 'net';
import ReverseForwardManager from '../../src/adb/tcpusb/reverseforwardmanager';
import Packet from '../../src/adb/tcpusb/packet';
import Protocol from '../../src/adb/protocol';

const { expect } = chai;
chai.use(sinonChai);

describe('ReverseForwardManager', () => {
  let mockSocket: any;
  let mockClient: any;
  let mockDevice: any;
  let mockTransport: any;

  beforeEach(() => {
    mockSocket = {
      remoteId: {
        next: sinon.stub().returns(1),
      },
      maxPayload: 4096,
      write: sinon.stub().returns(true),
    };

    mockTransport = {
      write: sinon.stub(),
      parser: {
        readAscii: sinon.stub().resolves(Protocol.OKAY),
        readAll: sinon.stub().resolves(Buffer.from('host-1 tcp:48080 tcp:7001\n')),
        readError: sinon.stub().resolves(new Error('Failed')),
      },
      end: sinon.stub(),
    };

    mockDevice = {
      transport: sinon.stub().resolves(mockTransport),
    };

    mockClient = {
      getDevice: sinon.stub().returns(mockDevice),
    };
  });

  it('should allocate loopback listener and register rewritten reverse:forward on device', (done) => {
    const manager = new ReverseForwardManager(mockSocket, mockClient, 'device-123', {
      reverseForwards: true,
      reverseMaxForwards: 10,
    });

    manager.handleForward('tcp:48080', 'tcp:48080')
      .then(() => {
        expect(mockClient.getDevice).to.have.been.calledWith('device-123');
        expect(mockTransport.write).to.have.been.calledOnce;
        const writtenData = mockTransport.write.firstCall.args[0];
        expect(writtenData.toString()).to.include('reverse:forward:tcp:48080;tcp:');
        manager.end();
        done();
      })
      .catch(done);
  });

  it('should reject non-tcp forward specs', (done) => {
    const manager = new ReverseForwardManager(mockSocket, mockClient, 'device-123', {
      reverseForwards: true,
    });

    manager.handleForward('localabstract:test', 'tcp:48080')
      .then(() => {
        done(new Error('Should have failed'));
      })
      .catch((err) => {
        expect(err.message).to.include('only tcp:<port> specs are supported');
        done();
      });
  });

  it('should rewrite list-forward output with original local ports', (done) => {
    const manager = new ReverseForwardManager(mockSocket, mockClient, 'device-123', {
      reverseForwards: true,
    });

    manager.handleForward('tcp:48080', 'tcp:48080')
      .then(() => {
        // Mock list-forward output returning the assigned rewritten port
        const entry = (manager as any).forwards.get('tcp:48080');
        mockTransport.parser.readAll.resolves(Buffer.from(`host-1 tcp:48080 tcp:${entry.rewrittenPort}\n`));
        return manager.handleListForward();
      })
      .then((output) => {
        expect(output.toString()).to.equal('host-1 tcp:48080 tcp:48080\n');
        manager.end();
        done();
      })
      .catch(done);
  });
});
