jest.mock("@/config", () => ({
    TOP_HISTORY_V2_ENABLED: true,
    GARUPA_HEALTH_CACHE_MS: 30_000,
    EVENT_TOP_POLL_INTERVAL_MS: 10_000,
    MONTHLY_TOP_POLL_INTERVAL_MS: 10_000,
    BORDER_PERSIST_INTERVAL_MS: 60_000,
}));
jest.mock("@/logger", () => ({ logger: jest.fn() }));
jest.mock("@/api/garupa", () => ({
    checkGarupaGameStatus: jest.fn().mockResolvedValue(true),
    getGarupaFallbackClientVersion: () => "9.4.4",
    getGarupaPackageUrl: () => "https://example.invalid/version",
    getGarupaServerCount: () => 4,
    getGarupaServerIds: () => [3],
    getGarupaStatusUnavailabilityThreshold: () => 1,
    getGarupaStatusPollIntervalMs: () => 1000,
    waitUntilGarupaAvailable: jest.fn(),
    fetchEventRanking: jest.fn(),
    fetchMonthlyRanking: jest.fn(),
}));
jest.mock("@/storage/dataBaseAdapter/mongodb", () => ({
    database: {
        ready: jest.fn().mockResolvedValue(undefined),
        collection: () => ({
            find: async () => ({ toArray: async () => [] }),
            updateOne: jest.fn().mockResolvedValue(undefined),
        }),
    },
}));
jest.mock("@/storage/downloader", () => ({
    downloader: { download: jest.fn().mockResolvedValue({ results: [{ version: "9.4.4" }] }) },
}));
jest.mock("@/services/eventInfoService", () => ({
    eventInfoService: {
        getEventDetail: jest.fn().mockResolvedValue(undefined),
        getEventType: jest.fn().mockResolvedValue("festival"),
    },
}));
jest.mock("@/services/monthlyRankingInfoService", () => ({
    monthlyRankingInfoService: { getMonthlyRankingDetail: jest.fn().mockResolvedValue(undefined) },
}));
// Isolate the disk queue; exercise both ranking services and their shared
// availability/status handling without writing observations or contacting a DB.
jest.mock("@/services/topHistoryService", () => ({
    topHistoryService: {
        capture: jest.fn(async (_scope: unknown, fetch: () => Promise<unknown>) => {
            try {
                return await fetch();
            } catch {
                return undefined;
            }
        }),
    },
}));

import { checkGarupaGameStatus, fetchEventRanking, fetchMonthlyRanking, waitUntilGarupaAvailable } from "@/api/garupa";
import { garupaService } from "@/services/garupaService";
import { statusService } from "@/services/statusService";

jest.spyOn(garupaService, "start").mockImplementation(() => {});
const { eventRankingService } = require("@/services/eventRankingService") as typeof import("@/services/eventRankingService");
const { monthlyRankingService } = require("@/services/monthlyRankingService") as typeof import("@/services/monthlyRankingService");
const taskState = (id: string) => statusService.snapshot().components.find((component) => component.id === id)?.status;

beforeEach(() => {
    jest.mocked(checkGarupaGameStatus).mockResolvedValue(true);
    statusService.record("eventRankingTask:3", "unknown", "game");
    statusService.record("monthlyRankingTask:3", "unknown", "game");
});

it("reports V2 event and monthly collection success independently", async () => {
    jest.mocked(fetchEventRanking).mockResolvedValue({ eventPointTopUsers: [] });
    jest.mocked(fetchMonthlyRanking).mockResolvedValue({ monthlyRankingPointTopUsers: [], monthlyRankingPointBorderUsers: [] });

    await eventRankingService.refreshServer(3, 300);
    expect(taskState("eventRankingTask:3")).toBe("operational");
    expect(taskState("monthlyRankingTask:3")).toBe("unknown");

    await monthlyRankingService.refreshServer(3, 17);
    expect(taskState("monthlyRankingTask:3")).toBe("operational");
});

it("reports failed V2 collection and the next successful sample", async () => {
    jest.mocked(fetchEventRanking).mockRejectedValueOnce(new Error("request failed"));
    await eventRankingService.refreshServer(3, 300);
    expect(taskState("eventRankingTask:3")).toBe("degraded");

    jest.mocked(fetchEventRanking).mockResolvedValue({ eventPointTopUsers: [] });
    await eventRankingService.refreshServer(3, 300);
    expect(taskState("eventRankingTask:3")).toBe("operational");
});

it("lets a V2 sample finish while recovery is pending and keeps the failure visible", async () => {
    await garupaService.runWithAvailability(3, async () => undefined);
    let recover!: () => void;
    jest.mocked(waitUntilGarupaAvailable).mockImplementation(
        () =>
            new Promise<void>((resolve) => {
                recover = resolve;
            }),
    );
    jest.mocked(checkGarupaGameStatus).mockResolvedValue(false);
    jest.mocked(fetchMonthlyRanking).mockRejectedValueOnce(new Error("request failed"));
    const pending = monthlyRankingService.refreshServer(3, 17);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        const outcome = await Promise.race([
            pending.then(() => "finished"),
            new Promise<string>((resolve) => {
                timer = setTimeout(() => resolve("blocked"), 200);
            }),
        ]);
        expect(outcome).toBe("finished");
        expect(taskState("monthlyRankingTask:3")).toBe("degraded");
    } finally {
        clearTimeout(timer);
        recover?.();
        await pending;
    }
});
