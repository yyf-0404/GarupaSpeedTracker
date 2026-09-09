import { logger } from "@/logger";

/** Auxiliary DB work cannot hold up Top sampling or create an unbounded backlog. */
export class ThrottledTask {
    private entries = new Map<string, { slot: number; busy: boolean }>();
    run(key: string, intervalMs: number, action: () => Promise<void>, now = Date.now()): void {
        const slot = Math.floor(now / intervalMs);
        const previous = this.entries.get(key);
        if (previous?.busy || previous?.slot === slot) return;
        const entry = { slot, busy: true };
        this.entries.set(key, entry);
        void action()
            .catch(() => {
                entry.slot = -1;
                logger("ranking", `auxiliary write failed scope=${key}; retrying on a later sample`);
            })
            .finally(() => {
                entry.busy = false;
            });
        if (this.entries.size > 64) {
            for (const [oldKey, old] of this.entries) {
                if (oldKey !== key && !old.busy) {
                    this.entries.delete(oldKey);
                    break;
                }
            }
        }
    }
}
