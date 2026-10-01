import { FrameCodec } from './pkg/cmsh.js';

const failed = () => new Error('cmsh:StreamClosed');
/** Network-independent framing over read(maximum, deadline), write(bytes,
 * deadline), close(). The provider must cancel pending I/O when closed.
 * Deadlines are whole-frame budgets from the injected monotonic clock.
 */
export class FramedStream {
  #io; #codec; #maximum; #timeout; #clock; #last; #state = 'Open';
  #frames = []; #reading = false; #writing = false;
  constructor(io, { maximumBytes, timeoutMs, clock = () => performance.now() }) {
    if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1 || maximumBytes > 1024 * 1024
        || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60_000) throw failed();
    this.#codec = new FrameCodec(maximumBytes);
    this.#io = io; this.#maximum = maximumBytes; this.#timeout = timeoutMs;
    this.#clock = clock; this.#last = clock();
  }
  get state() { return this.#state; }
  #now() {
    const now = this.#clock();
    if (!Number.isFinite(now) || now < this.#last) { this.cancel(); throw failed(); }
    this.#last = now; return now;
  }
  async sendFrame(bytes) {
    if (this.#state !== 'Open' || this.#writing) throw failed();
    this.#writing = true;
    try {
      if (!(bytes instanceof Uint8Array) || bytes.length > this.#maximum) throw failed();
      const end = this.#now() + this.#timeout;
      await this.#io.write(this.#codec.encode(bytes), this.#timeout);
      if (this.#state !== 'Open' || this.#now() >= end) throw failed();
    } catch { this.cancel(); throw failed(); }
    finally { this.#writing = false; }
  }
  async nextFrame() {
    if (this.#state !== 'Open' || this.#reading) throw failed();
    this.#reading = true;
    try {
      const end = this.#now() + this.#timeout;
      while (!this.#frames.length) {
        const remaining = Math.ceil(end - this.#now());
        if (remaining <= 0) throw failed();
        const maximum = Math.min(65536, this.#maximum + 4);
        const chunk = await this.#io.read(maximum, remaining);
        if (this.#state !== 'Open' || this.#now() >= end || !(chunk instanceof Uint8Array) || chunk.length > maximum) throw failed();
        if (!chunk.length) { this.#codec.finish(); this.#end('Closed'); return null; }
        this.#frames = this.#codec.push(chunk);
      }
      return this.#frames.shift();
    } catch { this.cancel(); throw failed(); }
    finally { this.#reading = false; }
  }
  finish() {
    if (this.#reading || this.#writing || this.#state !== 'Open') { this.cancel(); throw failed(); }
    // Each send already flushed; releasing the transport ends the byte stream.
    this.#end('Closed');
  }
  cancel() { this.#end('Failed'); }
  #end(state) {
    if (this.#state === 'Closed' || this.#state === 'Failed') return;
    this.#state = state; this.#frames = [];
    try { this.#io.close(); } catch { /* dependency details stay private */ }
    this.#codec.free();
  }
}
