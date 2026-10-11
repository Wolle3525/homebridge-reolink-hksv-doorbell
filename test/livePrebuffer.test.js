const assert = require('node:assert/strict');
const { test } = require('node:test');
const { Writable } = require('node:stream');
const { LivePrebuffer } = require('../dist/livePrebuffer');

function tsPacket(pid, streamId, randomAccess = false) {
  const packet = Buffer.alloc(188, 0xff);
  packet[0] = 0x47;
  packet[1] = 0x40 | (pid >> 8);
  packet[2] = pid & 0xff;
  packet[3] = randomAccess ? 0x30 : 0x10;
  const payloadOffset = randomAccess ? 6 : 4;
  if (randomAccess) {
    packet[4] = 1;
    packet[5] = 0x40;
  }
  if (streamId !== undefined) {
    packet.set([0, 0, 1, streamId], payloadOffset);
  }
  return packet;
}

test('replays PAT/PMT before the latest video keyframe, ignoring audio random access', () => {
  const prebuffer = new LivePrebuffer('', '', true, {}, '', false);
  const pat1 = tsPacket(0);
  const pmt1 = tsPacket(0x1000);
  const video1 = tsPacket(0x100, 0xe0, true);
  const pat2 = tsPacket(0);
  const pmt2 = tsPacket(0x1000);
  const audio = tsPacket(0x101, 0xc0, true);
  const video2 = tsPacket(0x100, 0xe0, true);
  const feed = (packets) => {
    const bytes = Buffer.concat(packets);
    for (let offset = 0; offset < bytes.length; offset += 97) {
      const chunk = bytes.subarray(offset, offset + 97);
      const startByte = prebuffer.totalBytes;
      prebuffer.totalBytes += chunk.length;
      prebuffer.scanForKeyframes(chunk, startByte);
      prebuffer.backlog.push({ chunk, at: 0, startByte });
      prebuffer.backlogBytes += chunk.length;
    }
  };

  feed([pat1, pmt1, video1]);
  assert.deepEqual(prebuffer.getBacklogSinceLastKeyframe(), Buffer.concat([pat1, pmt1, video1]));

  feed([pat2, pmt2, audio]);
  assert.deepEqual(prebuffer.getBacklogSinceLastKeyframe(), Buffer.concat([pat1, pmt1, video1, pat2, pmt2, audio]));

  feed([video2]);
  assert.deepEqual(prebuffer.getBacklogSinceLastKeyframe(), Buffer.concat([pat2, pmt2, audio, video2]));

  while (prebuffer.backlog[0].startByte < 4 * 188) {
    prebuffer.backlog.shift();
  }
  assert.equal(prebuffer.getBacklogSinceLastKeyframe().length, 0);
});

test('does not replay bytes from a stopped MPEG-TS connection', () => {
  const prebuffer = new LivePrebuffer('', '', true, {}, '', false);
  const stale = tsPacket(0);
  prebuffer.backlog.push({ chunk: stale, at: Date.now(), startByte: 0 });
  prebuffer.backlogBytes = stale.length;
  prebuffer.ensureRunning = () => {};

  const writes = [];
  const sink = new Writable({ write(chunk, _encoding, callback) {
    writes.push(Buffer.from(chunk));
    callback();
  } });
  prebuffer.acquire(sink);
  assert.equal(writes.length, 0);

  prebuffer.process = { killed: false };
  const liveSink = new Writable({ write(chunk, _encoding, callback) {
    writes.push(Buffer.from(chunk));
    callback();
  } });
  prebuffer.acquire(liveSink);
  assert.deepEqual(writes, [stale]);
});

test('disables h264_metadata only for an opted-out RTSP camera', async () => {
  const log = { info() {}, warn() {}, error() {} };
  const cases = [
    { isRtmp: false, rtspH264Metadata: false, expectedFilter: false },
    { isRtmp: false, rtspH264Metadata: true, expectedFilter: true },
    { isRtmp: true, rtspH264Metadata: false, expectedFilter: true },
  ];

  for (const { isRtmp, rtspH264Metadata, expectedFilter } of cases) {
    const prebuffer = new LivePrebuffer(process.execPath, 'unused-source', isRtmp, log, 'test', false, rtspH264Metadata);
    prebuffer.spawnProcess();
    const proc = prebuffer.process;
    assert.ok(proc);
    const filterIndex = proc.spawnargs.indexOf('-bsf:v');
    assert.equal(filterIndex >= 0, expectedFilter);
    if (expectedFilter) {
      assert.equal(proc.spawnargs[filterIndex + 1], 'h264_metadata=level=auto,h264_mp4toannexb');
    }
    await new Promise((resolve) => proc.once('close', resolve));
  }
});
