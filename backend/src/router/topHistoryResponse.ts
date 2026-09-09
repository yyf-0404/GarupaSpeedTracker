import { Readable } from "node:stream";
import type { TopScope } from "@/storage/topHistoryCodec";
import type { TopHistoryEntry } from "@/storage/topHistoryStore";

/** One complete JSON response; backpressure bounds memory independently of history length. */
export async function createTopHistoryResponse(
    scope: TopScope,
    from: number,
    to: number,
    entries: AsyncGenerator<TopHistoryEntry, void, unknown>,
): Promise<Readable> {
    // Resolve the checkpoint before sending HTTP headers so initial DB errors stay ordinary errors.
    const first = await entries.next();
    if (first.done || !("baseline" in first.value)) {
        await entries.return();
        throw new Error("Top history baseline is missing");
    }
    const header = JSON.stringify({ scope, from, to, baseline: first.value.baseline }).slice(0, -1);
    async function* body() {
        try {
            yield `${header},"samples":[`;
            let buffer = "";
            let separator = "";
            for await (const entry of entries) {
                if (!("sample" in entry)) throw new Error("Unexpected Top history baseline");
                buffer += separator + JSON.stringify(entry.sample);
                separator = ",";
                if (buffer.length >= 65536) {
                    yield buffer;
                    buffer = "";
                }
            }
            yield `${buffer}]}`;
        } finally {
            await entries.return();
        }
    }
    const stream = Readable.from(body(), { objectMode: false, highWaterMark: 65536 });
    // Also close a prefetched cursor when the client disconnects before reading the body.
    stream.once("close", () => {
        void entries.return().catch(() => {});
    });
    return stream;
}
