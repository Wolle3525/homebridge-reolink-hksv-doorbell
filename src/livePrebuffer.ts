import { spawn } from 'node:child_process';
import type { ChildProcessWithoutNullStreams } from 'node:child_process';
import type { Writable } from 'node:stream';
import type { Logger } from 'homebridge';

const IDLE_TIMEOUT_MS = 60000;
const MIN_STABLE_MS = 3000;
const BASE_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const TS_PACKET_SIZE = 188;
// How much recently-seen output to *keep available* so a newly-joining session can be handed a
// running start instead of whatever byte happens to be flowing right now - this needs to reliably
// span at least one full GOP, since SPS/PPS are inserted before IDR frames. This window is
// intentionally generous (matching Scrypted's rebroadcast plugin, which keeps 10s) because it no
// longer determines how much gets replayed: see getBacklogSinceLastKeyframe(). A wide window
// mainly guards against unusually long GOP configurations; it doesn't cost a longer catch-up burst.
const BACKLOG_MAX_AGE_MS = 8000;
// Absolute safety net regardless of age, in case of an unexpectedly high-bitrate source - not the
// normal way this trims.
const BACKLOG_MAX_BYTES = 6 * 1024 * 1024;

interface BacklogEntry {
  chunk: Buffer;
  at: number;
  /** Byte offset of this chunk's first byte within the connection's continuous output stream. */
  startByte: number;
}

/**
 * Keeps a single background ffmpeg process connected to the camera's RTSP/RTMP source and fans
 * its raw MPEG-TS output out to any number of live-view sessions, instead of every session
 * opening its own fresh connection.
 *
 * This mirrors what mature NVR-style implementations (e.g. Scrypted's rebroadcast) do, and for
 * good reason: establishing a brand new RTSP session and waiting for ffmpeg to analyze it takes
 * several real seconds on this class of camera even with tuned analyzeduration settings - often
 * long enough that HomeKit's own patience for the stream to start runs out before a single frame
 * gets through. Reusing one already-flowing, already-analyzed connection means a new live-view
 * session just has to pick up mid-stream, which ffmpeg does almost instantly since it doesn't
 * need to renegotiate anything - just find the next PAT/PMT and SPS, both of which repeat
 * constantly in a running MPEG-TS stream.
 *
 * A newly-joining session is handed the backlog starting at the tables before the latest keyframe
 * rather than the whole backlog window - found by scanning MPEG-TS packet headers for
 * the random_access_indicator flag (matching what Scrypted's rebroadcast plugin does via NAL
 * parsing, adapted here to the TS container ffmpeg wraps this output in). Without this, a session
 * joining a connection that's been kept warm for a while gets the *entire* multi-second window
 * dumped on it at once, which its own ffmpeg then races through far faster than real time before
 * settling to live - visible in HomeKit as a blurry fast-forward instant rather than a clean start.
 */
export class LivePrebuffer {
  private process: ChildProcessWithoutNullStreams | undefined;
  private readonly sinks = new Set<Writable>();
  private idleTimer: NodeJS.Timeout | undefined;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private consecutiveFailures = 0;
  private readonly backlog: BacklogEntry[] = [];
  private backlogBytes = 0;
  private totalBytes = 0;
  /** Leftover bytes (< one TS packet) carried over between stdout chunks so packet parsing stays aligned regardless of how the OS happens to chunk the pipe. */
  private tsCarry: Buffer = Buffer.alloc(0);
  /** Absolute byte offset of the latest PAT packet, which the TS muxer emits before video keyframes. */
  private lastPatBytePos = -1;
  /** Absolute byte offset of the PAT/PMT preceding the latest video keyframe, or -1 if none was found. */
  private lastReplayStartBytePos = -1;

  constructor(
    private readonly ffmpegPath: string,
    private readonly sourceUrl: string,
    private readonly isRtmp: boolean,
    private readonly log: Logger,
    private readonly label: string,
    private readonly debug: boolean,
    private readonly rtspH264Metadata = true,
  ) {}

  /** Registers a session's ffmpeg stdin as a destination for the prebuffered stream, starting the background connection if it isn't already running. */
  acquire(sink: Writable): void {
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = undefined;
    }
    // Without a handler here, a write to a sink whose process already exited (e.g. the session's
    // ffmpeg crashed but hasn't been released() yet) throws an unhandled 'error' that would crash
    // the whole child bridge process rather than just that one session.
    sink.on('error', () => this.sinks.delete(sink));
    // Hand the new sink the backlog from the tables before the last video keyframe, so it gets a
    // running start (PAT/PMT followed by a keyframe with SPS/PPS) before it starts receiving live
    // chunks. This happens
    // synchronously with no await in between, so no live chunk can slip in between the backlog
    // replay and the sink being added to `sinks` below - no gap, no duplicate delivery.
    // A stopped or reconnecting source may still have bytes from its previous
    // MPEG-TS timeline in the backlog. Never prepend those to a new connection.
    const replay = this.process && !this.process.killed
      ? this.getBacklogSinceLastKeyframe()
      : Buffer.alloc(0);
    if (replay.length > 0) {
      sink.write(replay);
    }
    this.sinks.add(sink);
    this.ensureRunning();
  }

  /**
   * Returns the backlog trimmed to start at the PAT before the latest video keyframe. Falls back
   * to the full backlog if no keyframe has been located yet. If the required PAT has already been
   * trimmed, wait for the next one instead of replaying an undecodable fragment.
   */
  private getBacklogSinceLastKeyframe(): Buffer {
    if (this.backlog.length === 0) {
      return Buffer.alloc(0);
    }
    if (this.lastReplayStartBytePos < 0) {
      return Buffer.concat(this.backlog.map((entry) => entry.chunk), this.backlogBytes);
    }
    if (this.lastReplayStartBytePos < this.backlog[0].startByte) {
      return Buffer.alloc(0);
    }
    const parts: Buffer[] = [];
    let total = 0;
    for (const entry of this.backlog) {
      if (entry.startByte + entry.chunk.length <= this.lastReplayStartBytePos) {
        continue;
      }
      const sliceStart = Math.max(0, this.lastReplayStartBytePos - entry.startByte);
      const piece = sliceStart > 0 ? entry.chunk.subarray(sliceStart) : entry.chunk;
      parts.push(piece);
      total += piece.length;
    }
    return Buffer.concat(parts, total);
  }

  /**
   * Scans a chunk of MPEG-TS output for PAT packets and video keyframes. TS packets are 188 bytes,
   * but stdout delivers arbitrary byte chunks with no relation to that boundary, so bytes from an
   * incomplete trailing packet are carried over (via tsCarry) and prepended to the next chunk.
   *
   * The random_access_indicator can also occur on audio packets, so check the PES stream ID for
   * video. FFmpeg's TS muxer writes PAT/PMT before each video keyframe; replay must include those
   * tables so a new demuxer can identify the video PID and its SPS/PPS-bearing IDR frame.
   */
  private scanForKeyframes(chunk: Buffer, chunkStartByte: number): void {
    const buf = this.tsCarry.length > 0 ? Buffer.concat([this.tsCarry, chunk]) : chunk;
    const bufStartByte = chunkStartByte - this.tsCarry.length;
    let offset = 0;
    while (offset + TS_PACKET_SIZE <= buf.length) {
      if (buf[offset] !== 0x47) {
        // Lost sync with the 188-byte packet grid - shouldn't normally happen against ffmpeg's own
        // mpegts output, but resync defensively rather than misreading adaptation field flags from
        // the wrong byte offset for the rest of the stream.
        offset += 1;
        continue;
      }
      const payloadUnitStart = (buf[offset + 1] & 0x40) !== 0;
      const pid = ((buf[offset + 1] & 0x1f) << 8) | buf[offset + 2];
      if (payloadUnitStart && pid === 0) {
        this.lastPatBytePos = bufStartByte + offset;
      }
      const adaptationFieldControl = (buf[offset + 3] & 0x30) >> 4;
      if (payloadUnitStart && adaptationFieldControl === 3) {
        const adaptationFieldLength = buf[offset + 4];
        if (adaptationFieldLength > 0 && adaptationFieldLength <= 183 &&
            (buf[offset + 5] & 0x40) !== 0) {
          const payloadOffset = offset + 5 + adaptationFieldLength;
          if (payloadOffset + 4 <= offset + TS_PACKET_SIZE &&
              buf[payloadOffset] === 0 && buf[payloadOffset + 1] === 0 && buf[payloadOffset + 2] === 1 &&
              (buf[payloadOffset + 3] & 0xf0) === 0xe0 && this.lastPatBytePos >= 0) {
            this.lastReplayStartBytePos = this.lastPatBytePos;
          }
        }
      }
      offset += TS_PACKET_SIZE;
    }
    this.tsCarry = buf.subarray(offset);
  }

  /** Unregisters a session. The background connection is kept warm for a short idle window in case another session starts soon, then torn down. */
  release(sink: Writable): void {
    this.sinks.delete(sink);
    if (this.sinks.size === 0 && !this.idleTimer) {
      this.idleTimer = setTimeout(() => this.stop(), IDLE_TIMEOUT_MS).unref();
    }
  }

  private ensureRunning(): void {
    if (this.process && !this.process.killed) {
      return;
    }
    // A reconnect is already scheduled (with backoff) after a recent failure - let it run on its
    // own timer rather than spawning immediately, or a burst of acquire() calls while the source
    // is unreachable would spawn ffmpeg processes as fast as Node can schedule them.
    if (this.reconnectTimer) {
      return;
    }

    this.spawnProcess();
  }

  private spawnProcess(): void {
    const args: string[] = ['-hide_banner', '-loglevel', this.debug ? 'verbose' : 'error'];
    args.push('-analyzeduration', '2000000', '-probesize', '1000000');
    if (this.isRtmp) {
      args.push('-i', this.sourceUrl);
    } else {
      args.push('-rtsp_transport', 'tcp', '-i', this.sourceUrl);
    }
    // Keep the existing SPS level rewrite by default, including for RTMP. Some RTSP cameras send
    // SPS data that h264_metadata cannot parse; for those cameras, skip the filter so it cannot
    // discard video packets. The MPEG-TS muxer handles Annex B conversion when needed.
    args.push('-map', '0', '-codec', 'copy');
    if (this.isRtmp || this.rtspH264Metadata) {
      args.push('-bsf:v', 'h264_metadata=level=auto,h264_mp4toannexb');
    }
    args.push('-f', 'mpegts', 'pipe:1');

    // Any previously buffered backlog belongs to the old connection - once it's gone, replaying
    // its (now stale, discontinuous) bytes to a session joining against the new connection would
    // do more harm than good, so start the new connection with a clean slate. The byte-offset
    // tracking and keyframe scan state reset the same way, since they're only meaningful relative
    // to a single connection's continuous output stream.
    this.backlog.length = 0;
    this.backlogBytes = 0;
    this.totalBytes = 0;
    this.tsCarry = Buffer.alloc(0);
    this.lastPatBytePos = -1;
    this.lastReplayStartBytePos = -1;

    this.log.info(`[${this.label}] Starting live view prebuffer connection`);
    const proc = spawn(this.ffmpegPath, args, { env: process.env });
    this.process = proc;
    const startedAt = Date.now();

    proc.stdout.on('data', (chunk: Buffer) => {
      const now = Date.now();
      const startByte = this.totalBytes;
      this.totalBytes += chunk.length;
      this.scanForKeyframes(chunk, startByte);

      this.backlog.push({ chunk, at: now, startByte });
      this.backlogBytes += chunk.length;
      while (
        this.backlog.length > 1 &&
        (now - this.backlog[0].at > BACKLOG_MAX_AGE_MS || this.backlogBytes > BACKLOG_MAX_BYTES)
      ) {
        const dropped = this.backlog.shift();
        if (dropped) {
          this.backlogBytes -= dropped.chunk.length;
        }
      }
      for (const sink of this.sinks) {
        if (!sink.destroyed) {
          sink.write(chunk);
        }
      }
    });

    proc.stderr.on('data', (chunk: Buffer) => {
      if (this.debug) {
        this.log.info(`[${this.label} prebuffer] ${chunk.toString().trim()}`);
      }
    });

    proc.once('error', (error) => {
      this.log.error(`[${this.label}] Live view prebuffer failed to start: ${error.message}`);
    });

    proc.once('close', () => {
      if (this.process === proc) {
        this.process = undefined;
      }
      if (this.sinks.size === 0) {
        return;
      }
      // Unexpected death while sessions are still watching: reconnect so they aren't left
      // hanging - but with escalating backoff, otherwise a source that's actually unreachable
      // (camera offline, network down) causes ffmpeg to be respawned in a tight loop as fast as
      // Node can schedule it, pegging CPU and burning file descriptors/ports on every attempt.
      // A connection that stayed up for a while before dying is treated as an unrelated, fresh
      // failure rather than compounding the backoff from before.
      if (Date.now() - startedAt >= MIN_STABLE_MS) {
        this.consecutiveFailures = 0;
      }
      const delay = Math.min(BASE_RECONNECT_DELAY_MS * 2 ** this.consecutiveFailures, MAX_RECONNECT_DELAY_MS);
      this.consecutiveFailures += 1;
      this.log.warn(`[${this.label}] Live view prebuffer connection lost, reconnecting in ${delay}ms`);
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = undefined;
        if (this.sinks.size > 0) {
          this.spawnProcess();
        }
      }, delay).unref();
    });
  }

  private stop(): void {
    this.idleTimer = undefined;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.consecutiveFailures = 0;
    if (this.process) {
      this.log.info(`[${this.label}] Stopping idle live view prebuffer connection`);
      this.process.kill('SIGTERM');
      this.process = undefined;
    }
  }
}
