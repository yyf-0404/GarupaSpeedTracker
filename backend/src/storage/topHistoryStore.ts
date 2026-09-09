import type { Collection, Db, Filter } from "mongodb";
import { MONGODB_RANKING_PLAYERS_COLLECTION, MONGODB_TOP_EVENTS_COLLECTION, MONGODB_TOP_HISTORY_META_COLLECTION } from "@/config";
import type { RankingUser } from "@/types/rankingUser";
import { applyTopEvent, emptyTopState, type TopEvent, type TopRanking, type TopState } from "./topHistoryCodec";

export interface TopHistoryMeta {
    _id: string;
    legacyMigrated?: boolean;
    sourceHash?: string;
    migratedAt?: number;
    rows?: number;
    events?: number;
}
export interface TopSample {
    at: number;
    scheduledAt?: number;
    type: "FULL" | "PATCH" | "SAME" | "GAP";
    ranking?: TopRanking;
    reason?: string;
    missing?: [number, number];
    effectiveAt?: number;
}
export type TopBaseline = { at: number; ranking: TopRanking | null; gap: boolean } | null;
export type TopHistoryEntry = { baseline: TopBaseline } | { sample: TopSample };
const names = ["FULL", "PATCH", "SAME", "GAP"] as const;

export class TopHistoryStore {
    readonly events: Collection<TopEvent>;
    readonly meta: Collection<TopHistoryMeta>;
    constructor(readonly db: Db) {
        this.events = db.collection<TopEvent>(MONGODB_TOP_EVENTS_COLLECTION);
        this.meta = db.collection<TopHistoryMeta>(MONGODB_TOP_HISTORY_META_COLLECTION);
    }
    async ensureIndexes(): Promise<void> {
        await this.events.createIndex({ s: 1, t: 1, _id: 1 }, { name: "scope_time" });
        await this.events.createIndex({ s: 1, t: -1, _id: -1 }, { name: "full_checkpoint", partialFilterExpression: { k: 0 } });
    }
    async hasHistory(scope: string): Promise<boolean> {
        return !!(await this.events.findOne({ s: scope }, { projection: { _id: 1 } }));
    }
    private async checkpoint(scope: string, at: number): Promise<TopEvent | null> {
        return this.events.findOne({ s: scope, k: 0, t: { $lte: at } }, { sort: { t: -1, _id: -1 } });
    }
    /** Load the latest checkpoint and replay its tail; no whole-period cache. */
    async loadState(scope: string): Promise<TopState> {
        let state = emptyTopState();
        const full = await this.checkpoint(scope, Number.MAX_SAFE_INTEGER);
        const cursor = this.events.find({ s: scope, ...(full ? { t: { $gte: full.t } } : {}) }).sort({ t: 1, _id: 1 });
        try {
            for await (const event of cursor) {
                if (full && event.t === full.t && event._id < full._id) continue;
                state = applyTopEvent(state, event);
            }
        } finally {
            await cursor.close();
        }
        return state;
    }
    /** Yield a baseline followed by every observation in the range, without a row limit. */
    async *history(scope: string, from: number, to: number): AsyncGenerator<TopHistoryEntry, void, unknown> {
        const full = await this.checkpoint(scope, from - 1);
        let state = emptyTopState();
        let baseline: TopBaseline = null;
        let started = false;
        const cursor = this.events.find({ s: scope, t: { $gte: full?.t ?? from, $lte: to } }).sort({ t: 1, _id: 1 });
        try {
            for await (const event of cursor) {
                if (full && event.t === full.t && event._id < full._id) continue;
                if (!started && event.t >= from) {
                    started = true;
                    yield { baseline };
                }
                state = applyTopEvent(state, event);
                if (event.t < from) {
                    baseline = { at: event.t, ranking: state.ranking, gap: state.gap };
                    continue;
                }
                yield {
                    sample: {
                        at: event.t,
                        scheduledAt: event.a,
                        type: names[event.k],
                        ...(event.k === 3 ? { reason: event.r } : { ranking: state.ranking ?? [] }),
                        ...(event.g ? { missing: event.g } : {}),
                        ...(event.e !== undefined ? { effectiveAt: event.e } : {}),
                    },
                };
            }
            if (!started) yield { baseline };
        } finally {
            await cursor.close();
        }
    }
    /** Compatibility API expands valid observations only. Final corrections replace endAt. */
    async points(scope: string, liveOnly = false): Promise<Array<{ time: number; uid: number; value: number }>> {
        let state = emptyTopState();
        const points: Array<{ time: number; uid: number; value: number }> = [];
        const filter: Filter<TopEvent> = { s: scope, ...(liveOnly ? { _id: /^l:/ } : {}) };
        const cursor = this.events.find(filter).sort({ t: 1, _id: 1 });
        try {
            for await (const event of cursor) {
                state = applyTopEvent(state, event);
                if (event.k === 3 || !state.ranking) continue;
                const time = event.e ?? event.t;
                // Post-end observations are ordered after regular samples.
                while (points.length && points[points.length - 1].time >= time) points.pop();
                for (const [uid, value] of state.ranking) points.push({ time, uid, value });
            }
        } finally {
            await cursor.close();
        }
        return points;
    }
    async users(server: number, uids: number[]): Promise<RankingUser[]> {
        if (!uids.length) return [];
        const docs = await this.db
            .collection(MONGODB_RANKING_PLAYERS_COLLECTION)
            .find({ server, uid: { $in: [...new Set(uids)] } })
            .toArray();
        return docs.map(({ _id, server: _server, updatedAt, ...user }) => user as unknown as RankingUser);
    }
}
