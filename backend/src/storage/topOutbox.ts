import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

/** Each file is an immutable observation, fsynced before acknowledging collection. */
export class TopOutbox<T extends { id: string; at: number }> {
    private loaded?: Promise<void>;
    private queue: Promise<unknown> = Promise.resolve();
    private files = new Map<string, number>();
    constructor(
        readonly directory: string,
        private maxBytes = 64 * 1024 * 1024,
        private maxFiles = 50000,
    ) {}
    init(): Promise<void> {
        this.loaded ??= (async () => {
            await fs.mkdir(this.directory, { recursive: true, mode: 0o700 });
            for (const name of await fs.readdir(this.directory)) {
                if (name.endsWith(".json")) this.files.set(name, (await fs.stat(path.join(this.directory, name))).size);
            }
        })();
        return this.loaded;
    }
    get size(): number {
        return this.files.size;
    }
    get bytes(): number {
        return [...this.files.values()].reduce((sum, size) => sum + size, 0);
    }
    async hasCapacity(): Promise<boolean> {
        await this.init();
        return this.size < this.maxFiles && this.bytes < this.maxBytes - 65536;
    }
    async enqueue(item: T): Promise<void> {
        const run = this.queue
            .catch(() => {})
            .then(async () => {
                await this.init();
                const name = `${String(item.at).padStart(16, "0")}-${createHash("sha256").update(item.id).digest("hex").slice(0, 32)}.json`;
                const data = JSON.stringify(item);
                if (this.files.has(name)) {
                    if ((await fs.readFile(path.join(this.directory, name), "utf8")) !== data) throw new Error("Outbox identity conflict");
                    return;
                }
                if (this.size >= this.maxFiles || this.bytes + Buffer.byteLength(data) > this.maxBytes) throw new Error("Top outbox is full");
                const temporary = path.join(this.directory, `.${randomUUID()}.tmp`);
                const file = await fs.open(temporary, "wx", 0o600);
                try {
                    await file.writeFile(data);
                    await file.sync();
                } finally {
                    await file.close();
                }
                await fs.rename(temporary, path.join(this.directory, name));
                const dir = await fs.open(this.directory, "r");
                try {
                    await dir.sync();
                } finally {
                    await dir.close();
                }
                this.files.set(name, Buffer.byteLength(data));
            });
        this.queue = run;
        return run;
    }
    async first(): Promise<{ name: string; item: T } | undefined> {
        await this.init();
        const name = [...this.files.keys()].sort()[0];
        if (!name) return undefined;
        return { name, item: JSON.parse(await fs.readFile(path.join(this.directory, name), "utf8")) as T };
    }
    async acknowledge(name: string): Promise<void> {
        await fs.unlink(path.join(this.directory, name));
        this.files.delete(name);
        // A crash can at worst replay an acknowledged event; MongoDB _id deduplicates it.
    }
}
