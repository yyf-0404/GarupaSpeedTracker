import { Readable } from "node:stream";
import type { Context } from "koa";
import { logger } from "@/logger";
import { loggerMiddleware } from "./routerLogger";

jest.mock("@/logger", () => ({ logger: jest.fn() }));

it("logs a streaming history response without serializing or consuming it", async () => {
    const stream = Readable.from(["history"]);
    Object.assign(stream, {
        toJSON: () => {
            throw new Error("stream must not be serialized");
        },
    });
    const ctx = { ip: "127.0.0.1", method: "GET", url: "/api/top/history", status: 200, body: stream } as unknown as Context;
    await loggerMiddleware(ctx, async () => {});
    expect(logger).toHaveBeenLastCalledWith("Response", expect.stringMatching(/^200 \d+ms stream$/), "success");
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    expect(chunks).toEqual(["history"]);
});
