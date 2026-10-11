const assert = require('node:assert/strict');
const { test } = require('node:test');
const { PassThrough } = require('node:stream');
const ffmpegModule = require('../dist/ffmpeg');
const portsModule = require('../dist/ports');
const { LivePrebuffer } = require('../dist/livePrebuffer');
const { StreamingDelegate } = require('../dist/streamingDelegate');

test('uses the SSRCs announced to HomeKit for both FFmpeg RTP outputs', async () => {
  let nextSsrc = 111;
  const hap = {
    CameraController: { generateSynchronisationSource: () => nextSsrc++ },
    SRTPCryptoSuites: { AES_CM_128_HMAC_SHA1_80: 0 },
    H264Profile: { HIGH: 2, MAIN: 1 },
    H264Level: { LEVEL4_0: 2, LEVEL3_2: 1 },
  };
  const log = { info() {}, warn() {}, error() {} };
  const cameraConfig = { name: 'Test', liveViewRtmp: true, enableAudio: true };
  const reolink = { getRtmpUrl: () => 'rtmp://example.invalid/stream' };
  const delegate = new StreamingDelegate(hap, log, cameraConfig, reolink, 'ffmpeg', false);
  const prepareRequest = {
    sessionID: 'session-1',
    targetAddress: '127.0.0.1',
    video: { port: 50000, srtpCryptoSuite: 0, srtp_key: Buffer.alloc(16), srtp_salt: Buffer.alloc(14) },
    audio: { port: 50001, srtpCryptoSuite: 0, srtp_key: Buffer.alloc(16), srtp_salt: Buffer.alloc(14) },
  };
  let response;
  const originalReservePorts = portsModule.reservePorts;
  portsModule.reservePorts = async () => [40000, 40001];
  try {
    await delegate.prepareStream(prepareRequest, (error, value) => {
      assert.equal(error, undefined);
      response = value;
    });
  } finally {
    portsModule.reservePorts = originalReservePorts;
  }
  assert.equal(response.video.ssrc, 111);
  assert.equal(response.audio.ssrc, 112);

  const startRequest = {
    sessionID: 'session-1',
    video: { pt: 99, ssrc: 999, mtu: 1200, max_bit_rate: 2000, profile: 2, level: 2, fps: 15 },
    audio: { pt: 110, ssrc: 998, sample_rate: 16, max_bit_rate: 24, channel: 1 },
  };
  const argsSeen = [];
  const OriginalFfmpegProcess = ffmpegModule.FfmpegProcess;
  const originalAcquire = LivePrebuffer.prototype.acquire;
  ffmpegModule.FfmpegProcess = class {
    constructor(_path, args) {
      argsSeen.push(args);
      this.process = { stdin: new PassThrough() };
      this.exited = Promise.resolve();
    }
  };
  LivePrebuffer.prototype.acquire = () => {};
  try {
    const pending = delegate.pendingSessions.get('session-1');
    await delegate.startStreamAsync(startRequest, pending);
  } finally {
    ffmpegModule.FfmpegProcess = OriginalFfmpegProcess;
    LivePrebuffer.prototype.acquire = originalAcquire;
  }
  const args = argsSeen[0];
  assert.ok(args);
  const ssrcs = args.flatMap((arg, index) => arg === '-ssrc' ? [args[index + 1]] : []);
  assert.deepEqual(ssrcs, ['111', '112']);
});
