/** Independent sampling events; FULL is a checkpoint, never a storage bucket. */
export type TopKind = "event" | "monthly";
export interface TopScope {
    server: number;
    kind: TopKind;
    periodId: number;
}
export type TopRanking = Array<[number, number]>;
export type GapReason = "timeout" | "http_error" | "rejected" | "unavailable" | "fetch_error" | "missing";
export interface TopObservation {
    id: string;
    scope: string;
    at: number;
    scheduledAt?: number;
    intervalMs: number;
    ranking?: TopRanking;
    reason?: GapReason;
    effectiveAt?: number;
    forceFull?: boolean;
}
/** Compact field names reduce per-event BSON overhead. 0=FULL,1=PATCH,2=SAME,3=GAP. */
export interface TopEvent {
    _id: string;
    s: string;
    t: number;
    k: 0 | 1 | 2 | 3;
    d?: TopRanking;
    a?: number;
    i?: number;
    r?: GapReason;
    e?: number;
    g?: [number, number];
}
export interface TopState {
    ranking: TopRanking | null;
    lastAt: number;
    lastSuccessAt: number | null;
    lastFullAt: number | null;
    lastScheduledAt?: number;
    lastIntervalMs?: number;
    gap: boolean;
}
export const emptyTopState = (): TopState => ({ ranking: null, lastAt: 0, lastSuccessAt: null, lastFullAt: null, gap: true });
export const topScopeKey = ({ server, kind, periodId }: TopScope): string => `${server}/${kind === "event" ? "e" : "m"}/${periodId}`;
export const parseTopScope = (key: string): TopScope => {
    const match = /^(\d+)\/(e|m)\/(\d+)$/.exec(key);
    if (!match || Number(match[3]) < 1) throw new Error("Invalid Top scope");
    return { server: Number(match[1]), kind: match[2] === "e" ? "event" : "monthly", periodId: Number(match[3]) };
};
export function validateTopRanking(value: unknown): asserts value is TopRanking {
    if (!Array.isArray(value) || value.length > 10) throw new Error("Invalid Top cardinality");
    const uids = new Set<number>();
    for (const row of value) {
        if (
            !Array.isArray(row) ||
            row.length !== 2 ||
            !Number.isSafeInteger(row[0]) ||
            row[0] < 1 ||
            !Number.isSafeInteger(row[1]) ||
            row[1] < 0 ||
            uids.has(row[0])
        )
            throw new Error("Invalid Top player or score");
        uids.add(row[0]);
    }
}
export function encodeTopObservation(observation: TopObservation, previous: TopState): TopEvent {
    if (!Number.isSafeInteger(observation.at) || observation.at < previous.lastAt) throw new Error("Out-of-order Top observation");
    const event: TopEvent = { _id: observation.id, s: observation.scope, t: observation.at, k: 3 };
    if (observation.scheduledAt !== undefined) {
        event.a = observation.scheduledAt;
        event.i = observation.intervalMs;
    }
    if (observation.effectiveAt !== undefined) {
        event.e = observation.effectiveAt;
        event.i = observation.intervalMs;
    }
    if (previous.lastScheduledAt !== undefined && observation.scheduledAt !== undefined) {
        const firstMissing = previous.lastScheduledAt + (previous.lastIntervalMs ?? observation.intervalMs);
        const lastMissing = observation.scheduledAt - observation.intervalMs;
        if (firstMissing <= lastMissing) event.g = [firstMissing, lastMissing];
    }
    if (observation.reason) {
        event.r = observation.reason;
        return event;
    }
    validateTopRanking(observation.ranking);
    const ranking = observation.ranking;
    const changedOrder =
        !previous.ranking || ranking.length !== previous.ranking.length || ranking.some(([uid], index) => previous.ranking?.[index][0] !== uid);
    const dueFull = previous.lastFullAt === null || Math.floor(observation.at / 3_600_000) !== Math.floor(previous.lastFullAt / 3_600_000);
    if (observation.forceFull || observation.effectiveAt !== undefined || previous.gap || event.g || dueFull || changedOrder) {
        event.k = 0;
        event.d = ranking.map((row) => [...row]);
    } else {
        const patches: TopRanking = ranking.flatMap(([, score], index) =>
            previous.ranking?.[index][1] === score ? [] : [[index + 1, score] as [number, number]],
        );
        if (!patches.length) event.k = 2;
        else if (patches.length === ranking.length) {
            event.k = 0;
            event.d = ranking.map((row) => [...row]);
        } else {
            event.k = 1;
            event.d = patches;
        }
    }
    return event;
}
export function applyTopEvent(previous: TopState, event: TopEvent): TopState {
    const state: TopState = { ...previous, ranking: previous.ranking?.map((row) => [...row]) ?? null, lastAt: event.t };
    if (event.a !== undefined) state.lastScheduledAt = event.a;
    if (event.i !== undefined) state.lastIntervalMs = event.i;
    if (event.e !== undefined) delete state.lastScheduledAt;
    if (event.k === 3) {
        state.gap = true;
        return state;
    }
    if (event.k === 0) {
        validateTopRanking(event.d);
        state.ranking = event.d.map((row) => [...row]);
        state.lastFullAt = event.t;
    } else {
        if (!state.ranking || state.gap) throw new Error("Top event has no valid FULL checkpoint");
        if (event.k === 1) {
            const seen = new Set<number>();
            for (const [rank, score] of event.d ?? []) {
                if (!Number.isInteger(rank) || rank < 1 || rank > state.ranking.length || !Number.isSafeInteger(score) || score < 0 || seen.has(rank))
                    throw new Error("Invalid Top patch");
                seen.add(rank);
                state.ranking[rank - 1][1] = score;
            }
        } else if (event.k !== 2) throw new Error("Unknown Top event type");
    }
    state.lastSuccessAt = event.t;
    state.gap = false;
    return state;
}
