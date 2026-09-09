import Router from "@koa/router";
import { TOP_HISTORY_V2_ENABLED } from "@/config";
import { topHistoryService } from "@/services/topHistoryService";
import { type TopScope, topScopeKey } from "@/storage/topHistoryCodec";
import { createTopHistoryResponse } from "./topHistoryResponse";
import { validationError } from "./utils";

export const topHistoryRouter = new Router();
const integer = (value: unknown, field: string, min = 0, max = Number.MAX_SAFE_INTEGER): number => {
    const n = typeof value === "string" && value.trim() ? Number(value) : typeof value === "number" ? value : NaN;
    if (!Number.isSafeInteger(n) || n < min || n > max) throw validationError(field, `invalid ${field}`);
    return n;
};
const scopeOf = (query: Record<string, unknown>): TopScope => {
    if (query.kind !== "event" && query.kind !== "monthly") throw validationError("kind", "kind must be event or monthly");
    return { server: integer(query.server, "server", 0, 3), kind: query.kind, periodId: integer(query.periodId, "periodId", 1) };
};
topHistoryRouter.get("/top/latest", async (ctx) => {
    if (!TOP_HISTORY_V2_ENABLED) ctx.throw(404, "Top history V2 is disabled");
    ctx.body = await topHistoryService.latest(scopeOf(ctx.query));
});
topHistoryRouter.get("/top/history", async (ctx) => {
    if (!TOP_HISTORY_V2_ENABLED) ctx.throw(404, "Top history V2 is disabled");
    const scope = scopeOf(ctx.query);
    const key = topScopeKey(scope);
    const to = integer(ctx.query.to ?? Date.now(), "to");
    const from = integer(ctx.query.from ?? 0, "from", 0, to);
    const body = await createTopHistoryResponse(scope, from, to, topHistoryService.store.history(key, from, to));
    ctx.type = "application/json";
    ctx.body = body;
});
