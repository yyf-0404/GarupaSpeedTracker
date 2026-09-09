import { randomUUID } from "node:crypto";
import { MongoClient } from "mongodb";
import { MONGODB_DB, MONGODB_RANKING_PLAYERS_COLLECTION, MONGODB_URI, TOP_OUTBOX_DIR, TOP_OUTBOX_MAX_BYTES } from "@/config";
import { logger } from "@/logger";
import {
    applyTopEvent,
    encodeTopObservation,
    type GapReason,
    parseTopScope,
    type TopObservation,
    type TopScope,
    type TopState,
    topScopeKey,
    validateTopRanking,
} from "@/storage/topHistoryCodec";
import { TopHistoryStore } from "@/storage/topHistoryStore";
import { TopOutbox } from "@/storage/topOutbox";
import type { RankingUser, RankingUserRaw } from "@/types/rankingUser";

interface QueuedObservation extends TopObservation {
    users?: RankingUser[];
}
export function topFailureReason(error: unknown): GapReason {
    const message = String(error).toLowerCase();
    if (/timeout|timed out|econnaborted/.test(message)) return "timeout";
    if (/invalid|validat|reject|mismatch/.test(message)) return "rejected";
    if (/http|status code/.test(message)) return "http_error";
    return "fetch_error";
}
function profileKey(user: RankingUser): string {
    return JSON.stringify([user.uid, user.name, user.introduction, user.rank, user.sid, user.strained, user.degrees]);
}

class TopHistoryService {
    private client = new MongoClient(MONGODB_URI, {
        maxPoolSize: 3,
        serverSelectionTimeoutMS: 5000,
        connectTimeoutMS: 5000,
        socketTimeoutMS: 10000,
        retryWrites: false,
    });
    readonly store = new TopHistoryStore(this.client.db(MONGODB_DB));
    readonly outbox = new TopOutbox<QueuedObservation>(TOP_OUTBOX_DIR, TOP_OUTBOX_MAX_BYTES);
    private initialized?: Promise<void>;
    private started = false;
    private timer?: NodeJS.Timeout;
    private draining = false;
    private states = new Map<string, TopState>();
    private firstCaptured = new Set<string>();
    private profiles = new Map<string, string>();
    private lastStorageFailureAt: number | null = null;
    private lastLogAt = 0;

    start(): void {
        if (this.started) return;
        this.started = true;
        this.timer = setInterval(() => {
            void this.drain();
        }, 5000);
        this.timer.unref();
        void this.drain();
    }
    async close(): Promise<void> {
        if (this.timer) clearInterval(this.timer);
        while (this.draining) await new Promise((resolve) => setTimeout(resolve, 20));
        await this.client.close();
    }
    private ready(): Promise<void> {
        this.initialized ??= this.store.ensureIndexes().catch((error) => {
            this.initialized = undefined;
            throw error;
        });
        return this.initialized;
    }
    /** Only fetch/validation failures become GAP. Disk/DB failures are storage failures. */
    async capture<T>(
        scope: TopScope,
        fetch: () => Promise<T | undefined>,
        usersOf: (raw: T) => RankingUserRaw[],
        options: { intervalMs: number; scheduledAt?: number; effectiveAt?: number },
    ): Promise<T | undefined> {
        this.start();
        if (!(await this.outbox.hasCapacity())) {
            logger("topHistory", "outbox full; skipping sampling until pending observations are committed");
            return undefined;
        }
        const key = topScopeKey(scope);
        let raw: T | undefined;
        let ranking: TopObservation["ranking"];
        let users: RankingUser[] | undefined;
        let reason: GapReason | undefined;
        let at = Date.now();
        try {
            raw = await fetch();
            at = Date.now();
            if (raw === undefined) reason = "unavailable";
            else {
                const source = usersOf(raw);
                if (!Array.isArray(source)) throw new Error("Invalid Top response");
                ranking = source.map((user) => [user.uid, user.point]);
                validateTopRanking(ranking);
                users = source.map(({ point, tier, ...user }) => user);
            }
        } catch (error) {
            at = Date.now();
            reason = topFailureReason(error);
            raw = undefined;
            logger("topHistory", `sampling failed scope=${key} reason=${reason}`);
        }
        const observation: QueuedObservation = {
            id: `l:${key}:${options.scheduledAt ?? randomUUID()}`,
            scope: key,
            at,
            ...options,
            effectiveAt: options.effectiveAt !== undefined && at >= options.effectiveAt ? options.effectiveAt : undefined,
            ...(reason ? { reason } : { ranking, users, forceFull: !this.firstCaptured.has(key) }),
        };
        await this.outbox.enqueue(observation);
        if (!reason) this.firstCaptured.add(key);
        void this.drain();
        return raw;
    }
    private async writeProfiles(item: QueuedObservation): Promise<void> {
        const { server } = parseTopScope(item.scope);
        const collection = this.store.db.collection(MONGODB_RANKING_PLAYERS_COLLECTION);
        for (const user of item.users ?? []) {
            const key = `${server}/${user.uid}`;
            const fingerprint = profileKey(user);
            if (this.profiles.get(key) === fingerprint) continue;
            const old = await collection.findOne({ server, uid: user.uid });
            if (!old || (Number(old.updatedAt ?? 0) <= item.at && profileKey(old as unknown as RankingUser) !== fingerprint)) {
                await collection.updateOne({ server, uid: user.uid }, { $set: { ...user, server, updatedAt: item.at } }, { upsert: true });
            }
            // Bounded metadata cache; eviction only causes a read, never data loss.
            if (this.profiles.size >= 1000) this.profiles.delete(this.profiles.keys().next().value!);
            this.profiles.set(key, fingerprint);
        }
    }
    async drain(): Promise<void> {
        if (this.draining) return;
        this.draining = true;
        try {
            await this.outbox.init();
            if (!this.outbox.size) return;
            await this.ready();
            for (let count = 0; count < 500; count++) {
                const pending = await this.outbox.first();
                if (!pending) break;
                const { item, name } = pending;
                // An acknowledged write may have lost its reply, or a crash may replay a file.
                if (await this.store.events.findOne({ _id: item.id }, { projection: { _id: 1 } })) {
                    this.states.delete(item.scope);
                } else {
                    const state = this.states.get(item.scope) ?? (await this.store.loadState(item.scope));
                    const event = encodeTopObservation(item, state);
                    await this.writeProfiles(item);
                    await this.store.events.insertOne(event, { writeConcern: { w: 1, j: true } });
                    this.states.set(item.scope, applyTopEvent(state, event));
                    if (this.states.size > 32) this.states.delete(this.states.keys().next().value!);
                }
                await this.outbox.acknowledge(name);
                this.lastStorageFailureAt = null;
            }
        } catch {
            this.lastStorageFailureAt = Date.now();
            if (Date.now() - this.lastLogAt > 30000) {
                this.lastLogAt = Date.now();
                logger("topHistory", `storage unavailable; retaining outbox (${this.outbox.size} observations, ${this.outbox.bytes} bytes)`);
            }
        } finally {
            this.draining = false;
        }
    }
    async latest(scope: TopScope) {
        await this.outbox.init();
        const state = await this.store.loadState(topScopeKey(scope));
        return {
            scope,
            ranking: state.ranking,
            lastAttemptAt: state.lastAt || null,
            lastSuccessAt: state.lastSuccessAt,
            lastFullAt: state.lastFullAt,
            failed: state.gap,
            stale: state.gap || !state.lastSuccessAt || Date.now() - state.lastSuccessAt > (state.lastIntervalMs ?? 10000) * 3,
            pendingObservations: this.outbox.size,
            lastStorageFailureAt: this.lastStorageFailureAt,
            users: await this.store.users(scope.server, state.ranking?.map(([uid]) => uid) ?? []),
        };
    }
    async compatible(scope: TopScope, legacy: () => Promise<{ points: Array<{ time: number; uid: number; value: number }>; users: RankingUser[] }>) {
        const key = topScopeKey(scope);
        const migrated = (await this.store.meta.findOne({ _id: key }))?.legacyMigrated;
        const points = await this.store.points(key, !migrated);
        if (!migrated) {
            const old = await legacy();
            if (!points.length) return old;
            const combined = old.points.filter((point) => point.time < points[0].time).concat(points);
            return {
                points: combined,
                users: await this.store.users(
                    scope.server,
                    combined.map((point) => point.uid),
                ),
            };
        }
        return {
            points,
            users: await this.store.users(
                scope.server,
                points.map((point) => point.uid),
            ),
        };
    }
}
export const topHistoryService = new TopHistoryService();
