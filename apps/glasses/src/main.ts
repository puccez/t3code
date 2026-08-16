import {
  AudioInputSource,
  CreateStartUpPageContainer,
  OsEventTypeList,
  RebuildPageContainer,
  TextContainerProperty,
  waitForEvenAppBridge,
} from "@evenrealities/even_hub_sdk";
import {
  ApprovalRequestId,
  type ClientOrchestrationCommand,
  CommandId,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  type OrchestrationProjectShell,
  type OrchestrationShellStreamItem,
  type OrchestrationThread,
  type OrchestrationThreadActivity,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { makeWsRpcProtocolClient, type WsRpcProtocolClient } from "@t3tools/client-runtime/rpc";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as Socket from "effect/unstable/socket/Socket";

const statusEl = document.getElementById("status")!;
function reportStatus(text: string) {
  statusEl.textContent = text;
  console.log(`[t3-glasses] ${text}`);
}

// ---------------------------------------------------------------------------
// Auth: pairing token (companion form or #token=…) → bearer → per-connection
// WebSocket ticket. The bearer lives in bridge localStorage — the only storage
// that survives Even App restarts — with browser localStorage as fallback for
// bridge-less tabs and as migration path for older sessions.
// ---------------------------------------------------------------------------

const BEARER_KEY = "t3glasses.bearer";

let bridgeStorage: Bridge | null = null;

async function storeGet(key: string): Promise<string> {
  if (bridgeStorage) {
    const fromBridge = await bridgeStorage.getLocalStorage(key).catch(() => "");
    if (fromBridge) return fromBridge;
  }
  return localStorage.getItem(key) ?? "";
}

async function storeSet(key: string, value: string): Promise<void> {
  if (bridgeStorage) await bridgeStorage.setLocalStorage(key, value).catch(() => false);
  if (value === "") localStorage.removeItem(key);
  else localStorage.setItem(key, value);
}

class NotPairedError extends Error {
  constructor(message = "Non collegato") {
    super(message);
  }
}

async function exchangePairingToken(pairToken: string): Promise<string> {
  const res = await fetch("/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      subject_token: pairToken,
      subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
      requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      scope: "orchestration:read orchestration:operate terminal:operate review:write relay:read",
      client_label: "T3 Glasses",
      client_device_type: "mobile",
    }),
  });
  if (!res.ok) throw new Error(`Token exchange failed: ${res.status}`);
  const json = (await res.json()) as { access_token: string };
  await storeSet(BEARER_KEY, json.access_token);
  return json.access_token;
}

async function getBearer(): Promise<string> {
  const stored = await storeGet(BEARER_KEY);
  if (stored) return stored;
  const pairToken = new URLSearchParams(location.hash.slice(1)).get("token");
  if (!pairToken) throw new NotPairedError();
  return exchangePairingToken(pairToken);
}

async function getSocketUrl(): Promise<string> {
  const bearer = await getBearer();
  const res = await fetch("/api/auth/websocket-ticket", {
    method: "POST",
    headers: { Authorization: `Bearer ${bearer}` },
  });
  if (res.status === 401) {
    await storeSet(BEARER_KEY, "");
    throw new NotPairedError("Sessione scaduta — ripeti il pairing");
  }
  if (!res.ok) throw new Error(`WebSocket ticket failed: ${res.status}`);
  const { ticket } = (await res.json()) as { ticket: string };
  const wsProto = location.protocol === "https:" ? "wss" : "ws";
  return `${wsProto}://${location.host}/ws?wsTicket=${encodeURIComponent(ticket)}`;
}

// ---------------------------------------------------------------------------
// Glasses UI state. One screen at a time on the 576×288 canvas.
// ---------------------------------------------------------------------------

type Screen =
  | { kind: "list" }
  | { kind: "thread"; threadId: string }
  | { kind: "reader"; threadId: string; page: number }
  | { kind: "recording"; threadId: string }
  | { kind: "transcribing"; threadId: string }
  | { kind: "preview"; threadId: string; text: string }
  | { kind: "sending"; threadId: string };

const state = {
  projects: new Map<string, OrchestrationProjectShell>(),
  threads: new Map<string, OrchestrationThreadShell>(),
  screen: { kind: "list" } as Screen,
  listedThreadIds: [] as string[],
  cursor: 0,
  actionCursor: 0,
  synchronized: false,
  detail: null as { threadId: string; thread: OrchestrationThread | null } | null,
};

let rpcClient: WsRpcProtocolClient | null = null;
let audioChunks: Uint8Array[] = [];

function badge(t: OrchestrationThreadShell): string {
  if (t.hasPendingApprovals || t.hasPendingUserInput) return "!";
  if (t.latestTurn?.state === "running" || t.backgroundLiveness === "working") return ">";
  if (t.latestTurn?.state === "error") return "x";
  return "·";
}

function visibleThreads(): OrchestrationThreadShell[] {
  const rank = (t: OrchestrationThreadShell) =>
    t.hasPendingApprovals || t.hasPendingUserInput ? 0 : t.latestTurn?.state === "running" ? 1 : 2;
  return [...state.threads.values()]
    .filter((t) => t.archivedAt === null)
    .sort((a, b) => rank(a) - rank(b) || b.updatedAt.localeCompare(a.updatedAt))
    .slice(0, 19);
}

function threadStatusLine(t: OrchestrationThreadShell): string {
  if (t.hasPendingApprovals) return "! In attesa di approvazione";
  if (t.hasPendingUserInput) return "! Serve il tuo input";
  const turn = t.latestTurn;
  if (turn?.state === "running") {
    const p = t.planProgress;
    return p ? `> [${p.completedSteps}/${p.totalSteps}] ${p.step}` : "> Turn in corso...";
  }
  if (turn?.state === "completed") return "Turn completato";
  if (turn?.state === "error") return "x Turn in errore";
  if (turn?.state === "interrupted") return "Turn interrotto";
  return "Nessun turn";
}

// Minimal port of apps/web/src/session-logic.ts derivePendingApprovals —
// worth upstreaming into client-runtime so every surface shares it.
type PendingApproval = { requestId: ApprovalRequestId; detail?: string };

function derivePendingApprovals(
  activities: readonly OrchestrationThreadActivity[],
): PendingApproval[] {
  const open = new Map<string, PendingApproval>();
  for (const activity of activities) {
    const payload =
      activity.payload && typeof activity.payload === "object"
        ? (activity.payload as Record<string, unknown>)
        : null;
    const requestId = payload && typeof payload.requestId === "string" ? payload.requestId : null;
    if (!requestId) continue;
    if (activity.kind === "approval.requested") {
      const detail = typeof payload?.detail === "string" ? payload.detail : undefined;
      open.set(requestId, {
        requestId: ApprovalRequestId.make(requestId),
        ...(detail !== undefined ? { detail } : {}),
      });
    } else if (
      activity.kind === "approval.resolved" ||
      activity.kind === "provider.approval.respond.failed"
    ) {
      open.delete(requestId);
    }
  }
  return [...open.values()];
}

function currentPendingApprovals(threadId: string): PendingApproval[] {
  const detail = state.detail;
  if (!detail || detail.threadId !== threadId || !detail.thread) return [];
  return derivePendingApprovals(detail.thread.activities);
}

function lastAssistantText(threadId: string): string | null {
  const detail = state.detail;
  if (!detail || detail.threadId !== threadId || !detail.thread) return null;
  const message = [...detail.thread.messages].reverse().find((m) => m.role === "assistant");
  return message?.text ?? null;
}

function threadActions(
  t: OrchestrationThreadShell,
): { label: string; id: "approve" | "deny" | "dictate" | "read" | "interrupt" }[] {
  const actions: ReturnType<typeof threadActions> = [];
  if (currentPendingApprovals(t.id).length > 0) {
    actions.push({ label: "Approva richiesta", id: "approve" });
    actions.push({ label: "Nega richiesta", id: "deny" });
  }
  actions.push({ label: "Detta follow-up", id: "dictate" });
  if (lastAssistantText(t.id) !== null) actions.push({ label: "Leggi risposta", id: "read" });
  if (t.latestTurn?.state === "running")
    actions.push({ label: "Interrompi turn", id: "interrupt" });
  return actions;
}

// ---------------------------------------------------------------------------
// Screen builders. All screens are a single full-bleed text container so the
// event-capture rules stay trivial (exactly one capture container per page).
// ---------------------------------------------------------------------------

const LIST_ROWS = 7;

function textPage(containerName: string, content: string): RebuildPageContainer {
  return new RebuildPageContainer({
    containerTotalNum: 1,
    textObject: [
      new TextContainerProperty({
        xPosition: 0,
        yPosition: 0,
        width: 576,
        height: 288,
        borderWidth: 0,
        paddingLength: 8,
        containerID: 1,
        containerName,
        isEventCapture: 1,
        content: content.slice(0, 1000),
      }),
    ],
  });
}

function listItemLabel(t: OrchestrationThreadShell): string {
  const project = state.projects.get(t.projectId)?.title ?? "?";
  return `${badge(t)} ${project} · ${t.title}`.slice(0, 52);
}

function buildListPage(): RebuildPageContainer {
  const threads = visibleThreads();
  state.listedThreadIds = threads.map((t) => t.id);
  if (state.cursor >= threads.length) state.cursor = Math.max(0, threads.length - 1);
  const top = Math.max(
    0,
    Math.min(state.cursor - Math.floor(LIST_ROWS / 2), threads.length - LIST_ROWS),
  );
  const rows = threads.slice(top, top + LIST_ROWS).map((t, i) => {
    const marker = top + i === state.cursor ? ">" : " ";
    return `${marker} ${listItemLabel(t)}`;
  });
  return textPage("threads", rows.length > 0 ? rows.join("\n") : "(nessun thread)");
}

function summarySnippet(threadId: string): string | null {
  const detail = state.detail;
  if (!detail || detail.threadId !== threadId) return "(caricamento dettagli…)";
  if (!detail.thread) return "(caricamento dettagli…)";
  const lastActivity = detail.thread.activities.at(-1);
  const source = lastActivity?.summary ?? lastAssistantText(threadId);
  if (!source) return null;
  return inlineMarkdownToPlain(source).replace(/\s+/g, " ").slice(0, 110);
}

function buildThreadPage(threadId: string): RebuildPageContainer {
  const t = state.threads.get(threadId);
  if (!t) return textPage("thread", "Thread non trovato\n\n2x tap: indietro");
  const project = state.projects.get(t.projectId)?.title ?? "?";
  const actions = threadActions(t);
  if (state.actionCursor >= actions.length) state.actionCursor = 0;
  const snippet = summarySnippet(threadId);
  const lines = [
    `${project} · ${t.title}`.slice(0, 60),
    threadStatusLine(t),
    ...(snippet ? [snippet] : []),
    "",
    ...actions.map((a, i) => `${i === state.actionCursor ? ">" : " "} ${a.label}`),
    "",
    "2x tap: indietro",
  ];
  return textPage("thread", lines.join("\n"));
}

// Reader: assistant markdown → plain display lines → pages by line count.
// The display fits ~10 lines of ~48 wrapped chars (proportional font, so 48
// monospace-budgeted chars never auto-wrap and line counting stays exact).
// 8 body lines + blank + footer = 10.
const READER_WRAP_COLS = 48;
const READER_BODY_LINES = 8;

function inlineMarkdownToPlain(text: string): string {
  return text
    .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/(\*\*|__)([^*_]+)\1/g, "$2")
    .replace(/`([^`]*)`/g, "$1");
}

function markdownToLines(text: string): string[] {
  const lines: string[] = [];
  let inFence = false;
  for (const raw of text.split("\n")) {
    if (/^\s*(```|~~~)/.test(raw)) {
      inFence = !inFence;
      continue;
    }
    if (inFence) {
      lines.push(`  ${raw.trimEnd()}`);
      continue;
    }
    lines.push(inlineMarkdownToPlain(raw.replace(/^#{1,6}\s+/, "").trimEnd()));
  }
  const collapsed: string[] = [];
  for (const line of lines) {
    if (line === "" && collapsed.at(-1) === "") continue;
    collapsed.push(line);
  }
  while (collapsed[0] === "") collapsed.shift();
  while (collapsed.at(-1) === "") collapsed.pop();
  return collapsed;
}

function wrapLine(line: string, cols: number): string[] {
  if (line.length <= cols) return [line];
  const baseIndent = line.match(/^\s*/)?.[0] ?? "";
  const contIndent = `${baseIndent}  `;
  const maxWord = cols - contIndent.length;
  const words = line
    .trim()
    .split(/\s+/)
    .flatMap((word) => {
      if (word.length <= maxWord) return [word];
      const parts: string[] = [];
      for (let i = 0; i < word.length; i += maxWord) parts.push(word.slice(i, i + maxWord));
      return parts;
    });
  const out: string[] = [];
  let current = baseIndent;
  for (const word of words) {
    const candidate = current.trim() === "" ? `${current}${word}` : `${current} ${word}`;
    if (candidate.length > cols && current.trim() !== "") {
      out.push(current);
      current = `${contIndent}${word}`;
    } else {
      current = candidate;
    }
  }
  if (current.trim() !== "") out.push(current);
  return out;
}

function readerPages(threadId: string): string[] {
  const text = lastAssistantText(threadId) ?? "";
  const lines = markdownToLines(text).flatMap((line) => wrapLine(line, READER_WRAP_COLS));
  const pages: string[] = [];
  for (let i = 0; i < lines.length; ) {
    if (lines[i] === "") {
      i += 1;
      continue;
    }
    pages.push(lines.slice(i, i + READER_BODY_LINES).join("\n"));
    i += READER_BODY_LINES;
  }
  return pages.length > 0 ? pages : ["(nessun testo)"];
}

function buildReaderPage(threadId: string, page: number): RebuildPageContainer {
  const pages = readerPages(threadId);
  const clamped = Math.max(0, Math.min(page, pages.length - 1));
  const header = `— ${clamped + 1}/${pages.length} · swipe: pagine · 2x tap: indietro —`;
  return textPage("reader", `${pages[clamped]}\n\n${header}`);
}

function buildScreen(): RebuildPageContainer {
  switch (state.screen.kind) {
    case "list":
      return buildListPage();
    case "thread":
      return buildThreadPage(state.screen.threadId);
    case "reader":
      return buildReaderPage(state.screen.threadId, state.screen.page);
    case "recording":
      return textPage("rec", "* REC — detta il prompt\n\ntap: fine dettatura\n2x tap: annulla");
    case "transcribing":
      return textPage("stt", "Trascrizione in corso…");
    case "preview": {
      const text = state.screen.text || "(trascrizione vuota — 2x tap per annullare)";
      return textPage("preview", `"${text}"\n\ntap: invia · 2x tap: annulla`);
    }
    case "sending":
      return textPage("send", "Invio del prompt…");
  }
}

// ---------------------------------------------------------------------------
// Render loop: coalesce state changes into one rebuild at a time.
// ---------------------------------------------------------------------------

type Bridge = Awaited<ReturnType<typeof waitForEvenAppBridge>>;

let renderQueued = false;
let rendering = Promise.resolve();

function scheduleRender(bridge: Bridge) {
  if (renderQueued) return;
  renderQueued = true;
  rendering = rendering.then(async () => {
    await new Promise((r) => setTimeout(r, 120));
    renderQueued = false;
    const ok = await bridge.rebuildPageContainer(buildScreen());
    if (!ok) console.warn("[t3-glasses] rebuildPageContainer failed");
  });
}

// ---------------------------------------------------------------------------
// Dictation: G2 mic → PCM chunks → STT sidecar → preview.
// ---------------------------------------------------------------------------

function sttBiasPrompt(): string {
  const names = [
    ...[...state.projects.values()].map((p) => p.title),
    ...visibleThreads().map((t) => t.title),
  ];
  return `T3 Code, thread, deploy, commit, PR, ${names.join(", ")}`.slice(0, 600);
}

async function startDictation(bridge: Bridge, threadId: string) {
  audioChunks = [];
  const ok = await bridge.audioControl(true, AudioInputSource.Glasses);
  if (!ok) {
    reportStatus("audioControl failed — mic non disponibile");
    return;
  }
  state.screen = { kind: "recording", threadId };
  scheduleRender(bridge);
}

async function stopDictation(bridge: Bridge, threadId: string, cancelled: boolean) {
  await bridge.audioControl(false);
  const chunks = audioChunks;
  audioChunks = [];
  if (cancelled) {
    state.screen = { kind: "thread", threadId };
    scheduleRender(bridge);
    return;
  }
  state.screen = { kind: "transcribing", threadId };
  scheduleRender(bridge);
  try {
    const body = new Blob(chunks as BlobPart[]);
    const res = await fetch(`/stt?language=it&prompt=${encodeURIComponent(sttBiasPrompt())}`, {
      method: "POST",
      body,
    });
    if (!res.ok) throw new Error(`stt ${res.status}`);
    const { text } = (await res.json()) as { text: string };
    state.screen = { kind: "preview", threadId, text };
  } catch (err) {
    reportStatus(`stt error: ${String(err)}`);
    state.screen = { kind: "thread", threadId };
  }
  scheduleRender(bridge);
}

// ---------------------------------------------------------------------------
// Orchestration commands.
// ---------------------------------------------------------------------------

function newCommandBase() {
  return {
    commandId: CommandId.make(crypto.randomUUID()),
    createdAt: new Date().toISOString(),
  };
}

async function dispatchOrchestration(command: ClientOrchestrationCommand): Promise<void> {
  if (!rpcClient) throw new Error("not connected");
  await Effect.runPromise(
    rpcClient[ORCHESTRATION_WS_METHODS.dispatchCommand](command).pipe(Effect.asVoid, Effect.orDie),
  );
}

async function sendFollowUp(bridge: Bridge, threadId: string, text: string) {
  const t = state.threads.get(threadId);
  if (!t) return;
  state.screen = { kind: "sending", threadId };
  scheduleRender(bridge);
  try {
    await dispatchOrchestration({
      type: "thread.turn.start",
      ...newCommandBase(),
      threadId: t.id,
      message: {
        messageId: MessageId.make(crypto.randomUUID()),
        role: "user",
        text,
        attachments: [],
      },
      runtimeMode: t.runtimeMode,
      interactionMode: t.interactionMode,
    });
    reportStatus("prompt inviato");
  } catch (err) {
    reportStatus(`dispatch error: ${String(err)}`);
  }
  state.screen = { kind: "thread", threadId };
  scheduleRender(bridge);
}

async function interruptTurn(threadId: string) {
  const t = state.threads.get(threadId);
  if (!t) return;
  await dispatchOrchestration({
    type: "thread.turn.interrupt",
    ...newCommandBase(),
    threadId: t.id,
    turnId: t.latestTurn?.turnId,
  });
  reportStatus("interrupt inviato");
}

// ---------------------------------------------------------------------------
// Shell subscription → state.
// ---------------------------------------------------------------------------

function onShellItem(item: OrchestrationShellStreamItem, bridge: Bridge) {
  switch (item.kind) {
    case "snapshot":
      state.projects = new Map(item.snapshot.projects.map((p) => [p.id, p]));
      state.threads = new Map(item.snapshot.threads.map((t) => [t.id, t]));
      break;
    case "project-upserted":
      state.projects.set(item.project.id, item.project);
      break;
    case "project-removed":
      state.projects.delete(item.projectId);
      break;
    case "thread-upserted":
      state.threads.set(item.thread.id, item.thread);
      break;
    case "thread-removed":
      state.threads.delete(item.threadId);
      break;
    case "synchronized":
      state.synchronized = true;
      break;
  }
  reportStatus(`connected · ${state.threads.size} threads · ${state.projects.size} projects`);
  scheduleRender(bridge);
}

const runShellSubscription = (socketUrl: string, bridge: Bridge) =>
  Effect.gen(function* () {
    const socketLayer = Socket.layerWebSocket(socketUrl, { openTimeout: "15 seconds" }).pipe(
      Layer.provide(Socket.layerWebSocketConstructorGlobal),
    );
    const protocolLayer = Layer.effect(
      RpcClient.Protocol,
      RpcClient.makeProtocolSocket({ retryTransientErrors: false }),
    ).pipe(Layer.provide(Layer.mergeAll(socketLayer, RpcSerialization.layerJson)));
    const protocolContext = yield* Layer.build(protocolLayer);
    const client = yield* makeWsRpcProtocolClient.pipe(Effect.provide(protocolContext));
    rpcClient = client;
    reportStatus("connected, subscribing to shell…");
    yield* client[ORCHESTRATION_WS_METHODS.subscribeShell]({}).pipe(
      Stream.runForEach((item) => Effect.sync(() => onShellItem(item, bridge))),
    );
  });

// ---------------------------------------------------------------------------
// Thread detail subscription: full messages/activities/checkpoints for the
// thread on screen. Live events arrive as raw orchestration events; instead of
// replaying them client-side we resubscribe (debounced) for a fresh snapshot.
// ---------------------------------------------------------------------------

let detailController: AbortController | null = null;
let detailRefreshTimer: ReturnType<typeof setTimeout> | null = null;

function stopThreadDetail() {
  detailController?.abort();
  detailController = null;
  if (detailRefreshTimer !== null) {
    clearTimeout(detailRefreshTimer);
    detailRefreshTimer = null;
  }
  state.detail = null;
}

type ThreadDetailItem =
  | { kind: "synchronized" }
  | { kind: "snapshot"; snapshot: { thread: OrchestrationThread } }
  | { kind: "event"; event: unknown };

function onDetailItem(bridge: Bridge, threadId: string, item: ThreadDetailItem) {
  if (item.kind === "snapshot") {
    state.detail = { threadId, thread: item.snapshot.thread };
    scheduleRender(bridge);
  } else if (item.kind === "event" && detailRefreshTimer === null) {
    detailRefreshTimer = setTimeout(() => {
      detailRefreshTimer = null;
      if (state.detail?.threadId === threadId) ensureThreadDetail(bridge, threadId, true);
    }, 500);
  }
}

function ensureThreadDetail(bridge: Bridge, threadId: string, force = false) {
  if (!force && state.detail?.threadId === threadId && detailController) return;
  detailController?.abort();
  if (state.detail?.threadId !== threadId) state.detail = { threadId, thread: null };
  const t = state.threads.get(threadId);
  if (!t || !rpcClient) return;
  const controller = new AbortController();
  detailController = controller;
  const client = rpcClient;
  const subscription = client[ORCHESTRATION_WS_METHODS.subscribeThread]({ threadId: t.id }).pipe(
    Stream.runForEach((item) =>
      Effect.sync(() => {
        if (controller.signal.aborted) return;
        onDetailItem(bridge, threadId, item);
      }),
    ),
  );
  Effect.runPromise(Effect.scoped(subscription), { signal: controller.signal }).catch(() => {});
}

async function respondApproval(bridge: Bridge, threadId: string, decision: "accept" | "decline") {
  const approval = currentPendingApprovals(threadId)[0];
  const t = state.threads.get(threadId);
  if (!approval || !t) return;
  await dispatchOrchestration({
    type: "thread.approval.respond",
    ...newCommandBase(),
    threadId: t.id,
    requestId: approval.requestId,
    decision,
  });
  reportStatus(`approvazione: ${decision}`);
  ensureThreadDetail(bridge, threadId, true);
}

// ---------------------------------------------------------------------------
// Input events from the glasses touchpad.
// ---------------------------------------------------------------------------

function wireInput(bridge: Bridge) {
  bridge.onEvenHubEvent((event) => {
    if (event.audioEvent?.audioPcm && state.screen.kind === "recording") {
      audioChunks.push(event.audioEvent.audioPcm);
      return;
    }
    const source = event.textEvent ?? event.sysEvent ?? event.listEvent;
    if (!source) return;
    // Zero-valued fields are omitted on the wire (proto3-style), and
    // CLICK_EVENT is 0 — a present event with no eventType IS a click.
    const eventType = source.eventType ?? OsEventTypeList.CLICK_EVENT;
    const screen = state.screen;

    switch (screen.kind) {
      case "list":
        if (eventType === OsEventTypeList.SCROLL_TOP_EVENT) {
          state.cursor = Math.max(0, state.cursor - 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.SCROLL_BOTTOM_EVENT) {
          state.cursor = Math.min(state.listedThreadIds.length - 1, state.cursor + 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.CLICK_EVENT) {
          const threadId = state.listedThreadIds[state.cursor];
          if (threadId) {
            state.screen = { kind: "thread", threadId };
            state.actionCursor = 0;
            ensureThreadDetail(bridge, threadId);
            scheduleRender(bridge);
          }
        }
        break;

      case "thread": {
        const t = state.threads.get(screen.threadId);
        const actions = t ? threadActions(t) : [];
        if (eventType === OsEventTypeList.SCROLL_TOP_EVENT) {
          state.actionCursor = Math.max(0, state.actionCursor - 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.SCROLL_BOTTOM_EVENT) {
          state.actionCursor = Math.min(actions.length - 1, state.actionCursor + 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.CLICK_EVENT) {
          const action = actions[state.actionCursor];
          if (action?.id === "dictate") void startDictation(bridge, screen.threadId);
          else if (action?.id === "read") {
            state.screen = { kind: "reader", threadId: screen.threadId, page: 0 };
            scheduleRender(bridge);
          } else if (action?.id === "approve" || action?.id === "deny") {
            void respondApproval(
              bridge,
              screen.threadId,
              action.id === "approve" ? "accept" : "decline",
            ).catch((err) => reportStatus(`approval error: ${String(err)}`));
          } else if (action?.id === "interrupt")
            void interruptTurn(screen.threadId).catch((err) =>
              reportStatus(`interrupt error: ${String(err)}`),
            );
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          state.screen = { kind: "list" };
          stopThreadDetail();
          scheduleRender(bridge);
        }
        break;
      }

      case "reader": {
        const pages = readerPages(screen.threadId);
        if (eventType === OsEventTypeList.SCROLL_TOP_EVENT) {
          state.screen = { ...screen, page: Math.max(0, screen.page - 1) };
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.SCROLL_BOTTOM_EVENT) {
          state.screen = { ...screen, page: Math.min(pages.length - 1, screen.page + 1) };
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          state.screen = { kind: "thread", threadId: screen.threadId };
          scheduleRender(bridge);
        }
        break;
      }

      case "recording":
        if (eventType === OsEventTypeList.CLICK_EVENT) {
          void stopDictation(bridge, screen.threadId, false);
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          void stopDictation(bridge, screen.threadId, true);
        }
        break;

      case "preview":
        if (eventType === OsEventTypeList.CLICK_EVENT && screen.text) {
          void sendFollowUp(bridge, screen.threadId, screen.text);
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          state.screen = { kind: "thread", threadId: screen.threadId };
          scheduleRender(bridge);
        }
        break;

      case "transcribing":
      case "sending":
        break;
    }
  });
}

// ---------------------------------------------------------------------------
// Companion page (phone side): pairing form + connection state.
// ---------------------------------------------------------------------------

// Null-tolerant lookups: a stale cached index.html without these elements must
// not take down the glasses UI, which lives in this same module.
const pairStateEl = document.getElementById("pair-state");
const pairTokenEl = document.getElementById("pair-token") as HTMLInputElement | null;
const pairConnectEl = document.getElementById("pair-connect") as HTMLButtonElement | null;
const pairDisconnectEl = document.getElementById("pair-disconnect") as HTMLButtonElement | null;

// Accepts a bare pairing token or a full pairing URL (#token=… / ?token=…).
function extractPairingToken(raw: string): string {
  try {
    const url = new URL(raw);
    const fromHash = new URLSearchParams(url.hash.slice(1)).get("token");
    return fromHash ?? url.searchParams.get("token") ?? raw;
  } catch {
    return raw;
  }
}

async function refreshPairingUi() {
  if (!pairStateEl || !pairDisconnectEl) return;
  const paired = (await storeGet(BEARER_KEY)) !== "";
  pairStateEl.textContent = paired
    ? `Collegato a ${location.host}`
    : "Non collegato — incolla il pairing token";
  pairDisconnectEl.disabled = !paired;
}

function wireCompanionPage() {
  if (!pairStateEl || !pairTokenEl || !pairConnectEl || !pairDisconnectEl) return;
  const stateEl = pairStateEl;
  const tokenEl = pairTokenEl;
  const connectEl = pairConnectEl;
  pairConnectEl.addEventListener("click", () => {
    const raw = tokenEl.value.trim();
    if (!raw) return;
    connectEl.disabled = true;
    stateEl.textContent = "Pairing in corso…";
    exchangePairingToken(extractPairingToken(raw)).then(
      () => location.reload(),
      (err) => {
        stateEl.textContent = `Pairing fallito: ${String(err)}`;
        connectEl.disabled = false;
      },
    );
  });
  pairDisconnectEl.addEventListener("click", () => {
    void storeSet(BEARER_KEY, "").then(() => location.reload());
  });
}

// ---------------------------------------------------------------------------
// Boot.
// ---------------------------------------------------------------------------

async function main() {
  reportStatus("waiting for Even bridge…");
  const bridge = await waitForEvenAppBridge();
  bridgeStorage = bridge;
  void refreshPairingUi();
  const created = await bridge.createStartUpPageContainer(
    new CreateStartUpPageContainer({
      containerTotalNum: 1,
      textObject: [
        new TextContainerProperty({
          xPosition: 0,
          yPosition: 0,
          width: 576,
          height: 288,
          borderWidth: 0,
          paddingLength: 8,
          containerID: 1,
          containerName: "boot",
          isEventCapture: 1,
          content: "T3 Glasses\nConnessione al server…",
        }),
      ],
    }),
  );
  if (created !== 0) {
    // A dev reload (or background restore) lands here: the host already has a
    // startup page from this webview's previous life, so rebuild over it.
    const rebuilt = await bridge.rebuildPageContainer(buildScreen());
    if (!rebuilt) {
      reportStatus(`createStartUpPageContainer failed: ${created}, rebuild failed too`);
      return;
    }
  }
  wireInput(bridge);

  reportStatus("authenticating…");
  let socketUrl: string;
  try {
    socketUrl = await getSocketUrl();
  } catch (err) {
    if (err instanceof NotPairedError) {
      reportStatus(`${err.message} — usa il modulo di pairing qui sotto`);
      await bridge.rebuildPageContainer(
        textPage(
          "pair",
          "T3 Glasses — non collegato\n\nApri T3 Glasses sul telefono e incolla\nil pairing token per collegarti.",
        ),
      );
      void refreshPairingUi();
      return;
    }
    throw err;
  }
  void refreshPairingUi();
  reportStatus("opening websocket…");
  await Effect.runPromise(Effect.scoped(runShellSubscription(socketUrl, bridge))).catch((err) => {
    reportStatus(`connection lost: ${String(err)}`);
  });
}

wireCompanionPage();
main().catch((err) => reportStatus(`fatal: ${String(err)}`));
