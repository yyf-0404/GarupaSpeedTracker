/** Wall-clock slots are skipped while busy; tasks never overlap or accumulate. */
export class AlignedPoller {
    private busy = false;
    private nextAt: number;
    constructor(
        private callback: (scheduledAt: number) => Promise<void>,
        readonly intervalMs: number,
        private phaseMs = 0,
        now = Date.now(),
    ) {
        if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) throw new Error("Invalid polling interval");
        this.nextAt = this.nextSlot(now);
    }
    private nextSlot(now: number): number {
        return (Math.floor((now - this.phaseMs) / this.intervalMs) + 1) * this.intervalMs + this.phaseMs;
    }
    async tick(now = Date.now()): Promise<void> {
        if (now < this.nextAt) return;
        // Attribute an attempt to the current slot, never backfill missed requests.
        const scheduledAt = this.nextSlot(now) - this.intervalMs;
        this.nextAt = scheduledAt + this.intervalMs;
        if (this.busy) return;
        this.busy = true;
        try {
            await this.callback(scheduledAt);
        } finally {
            this.busy = false;
        }
    }
}
