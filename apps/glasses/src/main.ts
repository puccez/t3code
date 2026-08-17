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
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  type OrchestrationProjectShell,
  type OrchestrationShellStreamItem,
  type OrchestrationThread,
  type OrchestrationThreadActivity,
  type OrchestrationThreadShell,
  ThreadId,
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
const HOME_MODE_KEY = "t3glasses.homeMode";

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
// The thread screen is a terminal-style live tail (Even Terminal look):
// content first, actions behind a tap.
// ---------------------------------------------------------------------------

type HomeMode = "lista" | "project" | "focus";

type DictationTarget = { kind: "thread"; threadId: string } | { kind: "new"; projectId: string };

type Screen =
  | { kind: "list" }
  | { kind: "projects" }
  | { kind: "projectThreads"; projectId: string }
  | { kind: "projectPick" }
  | { kind: "thread"; threadId: string; scroll: number }
  | { kind: "actions"; threadId: string }
  | { kind: "reader"; threadId: string; page: number }
  | { kind: "recording"; target: DictationTarget }
  | { kind: "transcribing"; target: DictationTarget }
  | { kind: "preview"; target: DictationTarget; text: string }
  | { kind: "sending"; target: DictationTarget };

type ListRow = { kind: "new" } | { kind: "thread"; threadId: string };

const state = {
  projects: new Map<string, OrchestrationProjectShell>(),
  threads: new Map<string, OrchestrationThreadShell>(),
  screen: { kind: "list" } as Screen,
  listedRows: [] as ListRow[],
  listedProjectIds: [] as string[],
  cursor: 0,
  actionCursor: 0,
  synchronized: false,
  homeMode: "lista" as HomeMode,
  homeApplied: false,
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
// Text shaping: markdown → plain display lines, wrapped and truncated so line
// counting stays exact on the glasses (proportional font, 48-col budget).
// ---------------------------------------------------------------------------

const WRAP_COLS = 48;
const ROW_CHARS = 46;

// The glasses LVGL font has no emoji or pictographs — they render as boxes.
function stripUnrenderable(text: string): string {
  return text
    .replace(/[\u{1F000}-\u{1FFFF}\u{2190}-\u{2BFF}\u{FE00}-\u{FE0F}\u{200D}]/gu, "")
    .replace(/ {2,}/g, " ");
}

function oneLine(text: string): string {
  return stripUnrenderable(text).replace(/\s+/g, " ").trim();
}

function truncateRow(text: string): string {
  const flat = oneLine(text);
  return flat.length > ROW_CHARS ? `${flat.slice(0, ROW_CHARS - 1)}…` : flat;
}

function inlineMarkdownToPlain(text: string): string {
  return text
    .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/(\*\*|__)([^*_]+)\1/g, "$2")
    .replace(/`([^`]*)`/g, "$1");
}

function markdownToLines(text: string): string[] {
  const lines: string[] = [];
  let inFence = false;
  for (const raw of stripUnrenderable(text).split("\n")) {
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

function wrapMarkdown(text: string): string[] {
  return markdownToLines(text).flatMap((line) => wrapLine(line, WRAP_COLS));
}

// ---------------------------------------------------------------------------
// Screen builders. All screens are a single full-bleed text container so the
// event-capture rules stay trivial (exactly one capture container per page).
// The 288px panel fits 10 lines.
// ---------------------------------------------------------------------------

const LIST_ROWS = 9;
const TAIL_ROWS = 8;

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

function cursorRows(labels: string[], cursor: number, rows: number): string[] {
  const top = Math.max(0, Math.min(cursor - Math.floor(rows / 2), labels.length - rows));
  return labels
    .slice(top, top + rows)
    .map((label, i) => `${top + i === cursor ? ">" : " "} ${label}`);
}

function listItemLabel(t: OrchestrationThreadShell): string {
  const project = state.projects.get(t.projectId)?.title ?? "?";
  return truncateRow(`${badge(t)} ${project} · ${t.title}`);
}

function threadListRows(threads: OrchestrationThreadShell[]): ListRow[] {
  return [...threads.map((t): ListRow => ({ kind: "thread", threadId: t.id })), { kind: "new" }];
}

function clampCursor(length: number) {
  if (state.cursor >= length) state.cursor = Math.max(0, length - 1);
}

function buildListPage(): RebuildPageContainer {
  const rows = threadListRows(visibleThreads());
  state.listedRows = rows;
  clampCursor(rows.length);
  const labels = rows.map((row) => {
    if (row.kind === "new") return "+ Nuovo thread";
    const t = state.threads.get(row.threadId);
    return t ? listItemLabel(t) : "?";
  });
  return textPage("threads", cursorRows(labels, state.cursor, LIST_ROWS).join("\n"));
}

function sortedProjects(): OrchestrationProjectShell[] {
  return [...state.projects.values()].sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
}

function projectThreadsOf(projectId: string): OrchestrationThreadShell[] {
  return visibleThreads().filter((t) => t.projectId === projectId);
}

function projectBadge(projectId: string): string {
  const threads = projectThreadsOf(projectId);
  if (threads.some((t) => t.hasPendingApprovals || t.hasPendingUserInput)) return "!";
  if (threads.some((t) => t.latestTurn?.state === "running" || t.backgroundLiveness === "working"))
    return ">";
  return "·";
}

function buildProjectsPage(): RebuildPageContainer {
  const projects = sortedProjects();
  state.listedProjectIds = projects.map((p) => p.id);
  clampCursor(projects.length);
  if (projects.length === 0) return textPage("projects", "(nessun project)");
  const labels = projects.map((p) =>
    truncateRow(`${projectBadge(p.id)} ${p.title} (${projectThreadsOf(p.id).length})`),
  );
  return textPage("projects", cursorRows(labels, state.cursor, LIST_ROWS).join("\n"));
}

function buildProjectThreadsPage(projectId: string): RebuildPageContainer {
  const project = state.projects.get(projectId);
  const rows = threadListRows(projectThreadsOf(projectId));
  state.listedRows = rows;
  clampCursor(rows.length);
  const labels = rows.map((row) => {
    if (row.kind === "new") return "+ Nuovo thread qui";
    const t = state.threads.get(row.threadId);
    return t ? truncateRow(`${badge(t)} ${t.title}`) : "?";
  });
  const header = truncateRow(`— ${project?.title ?? "?"} · 2x tap: project —`);
  return textPage(
    "pthreads",
    [header, ...cursorRows(labels, state.cursor, LIST_ROWS - 1)].join("\n"),
  );
}

function buildProjectPickPage(): RebuildPageContainer {
  const projects = sortedProjects();
  state.listedProjectIds = projects.map((p) => p.id);
  clampCursor(projects.length);
  if (projects.length === 0) return textPage("newpick", "(nessun project)\n\n2x tap: indietro");
  const labels = projects.map((p) => truncateRow(p.title));
  const header = "Nuovo thread — scegli il project";
  return textPage(
    "newpick",
    [header, ...cursorRows(labels, state.cursor, LIST_ROWS - 1)].join("\n"),
  );
}

// Thread screen: terminal-style live tail (Even Terminal look). Chronological
// merge of one-line activity summaries and message text; newest at the bottom.
function tailLines(threadId: string): string[] {
  const detail = state.detail;
  if (!detail || detail.threadId !== threadId || !detail.thread) return ["(caricamento attività…)"];
  const entries: { at: string; lines: string[] }[] = [];
  for (const m of detail.thread.messages) {
    if (!m.text) continue;
    if (m.role === "user") entries.push({ at: m.createdAt, lines: [truncateRow(`» ${m.text}`)] });
    else entries.push({ at: m.createdAt, lines: wrapMarkdown(m.text) });
  }
  for (const a of detail.thread.activities) {
    entries.push({ at: a.createdAt, lines: [truncateRow(`· ${a.summary}`)] });
  }
  entries.sort((a, b) => a.at.localeCompare(b.at));
  const lines = entries.flatMap((e) => e.lines);
  return lines.length > 0 ? lines : ["(nessuna attività)"];
}

function maxTailScroll(threadId: string): number {
  return Math.max(0, tailLines(threadId).length - TAIL_ROWS);
}

function buildThreadPage(threadId: string, scroll: number): RebuildPageContainer {
  const t = state.threads.get(threadId);
  if (!t) return textPage("thread", "Thread non trovato\n\n2x tap: indietro");
  const lines = tailLines(threadId);
  const clamped = Math.max(0, Math.min(scroll, Math.max(0, lines.length - TAIL_ROWS)));
  const end = lines.length - clamped;
  const window = lines.slice(Math.max(0, end - TAIL_ROWS), end);
  const header = truncateRow(`${badge(t)} ${t.title}`);
  const footer =
    clamped > 0
      ? truncateRow(`— storico (-${clamped}) · tap: azioni · 2x: indietro —`)
      : truncateRow(`— ${threadStatusLine(t)} · tap: azioni —`);
  return textPage("thread", [header, ...window, footer].join("\n"));
}

function buildActionsPage(threadId: string): RebuildPageContainer {
  const t = state.threads.get(threadId);
  if (!t) return textPage("actions", "Thread non trovato\n\n2x tap: indietro");
  const actions = threadActions(t);
  if (state.actionCursor >= actions.length) state.actionCursor = 0;
  const lines = [
    truncateRow(`${badge(t)} ${t.title}`),
    truncateRow(threadStatusLine(t)),
    "",
    ...actions.map((a, i) => `${i === state.actionCursor ? ">" : " "} ${a.label}`),
    "",
    "2x tap: indietro",
  ];
  return textPage("actions", lines.join("\n"));
}

// Reader: full assistant text paginated by line count (drill-down).
const READER_BODY_LINES = 8;

function readerPages(threadId: string): string[] {
  const lines = wrapMarkdown(lastAssistantText(threadId) ?? "");
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
    case "projects":
      return buildProjectsPage();
    case "projectThreads":
      return buildProjectThreadsPage(state.screen.projectId);
    case "projectPick":
      return buildProjectPickPage();
    case "thread":
      return buildThreadPage(state.screen.threadId, state.screen.scroll);
    case "actions":
      return buildActionsPage(state.screen.threadId);
    case "reader":
      return buildReaderPage(state.screen.threadId, state.screen.page);
    case "recording": {
      const target = state.screen.target;
      const where =
        target.kind === "new"
          ? `nuovo thread in ${state.projects.get(target.projectId)?.title ?? "?"}`
          : "follow-up";
      return textPage(
        "rec",
        `* REC — detta il prompt (${where})\n\ntap: fine dettatura\n2x tap: annulla`,
      );
    }
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
// Home Mode: the entry layout of the glasses, set from the companion page.
// lista = cross-project by activity, project = hierarchical, focus = jump to
// the most recently active thread.
// ---------------------------------------------------------------------------

function lastActiveThreadId(): string | null {
  const threads = [...state.threads.values()]
    .filter((t) => t.archivedAt === null)
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  return threads[0]?.id ?? null;
}

function homeScreen(): Screen {
  if (state.homeMode === "project") return { kind: "projects" };
  if (state.homeMode === "focus") {
    const threadId = lastActiveThreadId();
    if (threadId) return { kind: "thread", threadId, scroll: 0 };
  }
  return { kind: "list" };
}

function goHome(bridge: Bridge) {
  const screen = homeScreen();
  state.screen = screen;
  state.cursor = 0;
  state.actionCursor = 0;
  if (screen.kind === "thread") ensureThreadDetail(bridge, screen.threadId);
  else stopThreadDetail();
  scheduleRender(bridge);
}

function backFromThread(threadId: string): Screen {
  if (state.homeMode === "project") {
    const t = state.threads.get(threadId);
    if (t) return { kind: "projectThreads", projectId: t.projectId };
  }
  return { kind: "list" };
}

function dictationBack(target: DictationTarget): Screen {
  if (target.kind === "thread") return { kind: "thread", threadId: target.threadId, scroll: 0 };
  if (state.homeMode === "project") return { kind: "projectThreads", projectId: target.projectId };
  return { kind: "list" };
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

async function startDictation(bridge: Bridge, target: DictationTarget) {
  audioChunks = [];
  const ok = await bridge.audioControl(true, AudioInputSource.Glasses);
  if (!ok) {
    reportStatus("audioControl failed — mic non disponibile");
    return;
  }
  state.screen = { kind: "recording", target };
  scheduleRender(bridge);
}

async function stopDictation(bridge: Bridge, target: DictationTarget, cancelled: boolean) {
  await bridge.audioControl(false);
  const chunks = audioChunks;
  audioChunks = [];
  if (cancelled) {
    state.screen = dictationBack(target);
    scheduleRender(bridge);
    return;
  }
  state.screen = { kind: "transcribing", target };
  scheduleRender(bridge);
  try {
    const body = new Blob(chunks as BlobPart[]);
    const res = await fetch(`/stt?language=it&prompt=${encodeURIComponent(sttBiasPrompt())}`, {
      method: "POST",
      body,
    });
    if (!res.ok) throw new Error(`stt ${res.status}`);
    const { text } = (await res.json()) as { text: string };
    state.screen = { kind: "preview", target, text };
  } catch (err) {
    reportStatus(`stt error: ${String(err)}`);
    state.screen = dictationBack(target);
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
  state.screen = { kind: "sending", target: { kind: "thread", threadId } };
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
  state.screen = { kind: "thread", threadId, scroll: 0 };
  scheduleRender(bridge);
}

// New thread from the glasses: project defaults, falling back to the most
// recent thread of the same project, then contract defaults.
async function createThreadAndStart(bridge: Bridge, projectId: string, text: string) {
  const project = state.projects.get(projectId);
  const donor = [...state.threads.values()]
    .filter((t) => t.projectId === projectId)
    .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt))[0];
  const modelSelection = project?.defaultModelSelection ?? donor?.modelSelection;
  if (!project || !modelSelection) {
    reportStatus("nessun modello di default per il project");
    state.screen = dictationBack({ kind: "new", projectId });
    scheduleRender(bridge);
    return;
  }
  state.screen = { kind: "sending", target: { kind: "new", projectId } };
  scheduleRender(bridge);
  const threadId = ThreadId.make(crypto.randomUUID());
  const runtimeMode = donor?.runtimeMode ?? DEFAULT_RUNTIME_MODE;
  const interactionMode = donor?.interactionMode ?? DEFAULT_PROVIDER_INTERACTION_MODE;
  try {
    await dispatchOrchestration({
      type: "thread.create",
      ...newCommandBase(),
      threadId,
      projectId: project.id,
      title: "New thread",
      modelSelection,
      runtimeMode,
      interactionMode,
      branch: null,
      worktreePath: null,
    });
    await dispatchOrchestration({
      type: "thread.turn.start",
      ...newCommandBase(),
      threadId,
      message: {
        messageId: MessageId.make(crypto.randomUUID()),
        role: "user",
        text,
        attachments: [],
      },
      runtimeMode,
      interactionMode,
    });
    reportStatus("nuovo thread avviato");
    state.screen = { kind: "thread", threadId, scroll: 0 };
    ensureThreadDetail(bridge, threadId);
  } catch (err) {
    reportStatus(`create error: ${String(err)}`);
    state.screen = dictationBack({ kind: "new", projectId });
  }
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
      // Apply the Home Mode entry layout once threads are known.
      if (!state.homeApplied) {
        state.homeApplied = true;
        const home = homeScreen();
        if (state.screen.kind === "list" && home.kind !== "list") {
          state.screen = home;
          if (home.kind === "thread") ensureThreadDetail(bridge, home.threadId);
        }
      }
      break;
    case "project-upserted":
      state.projects.set(item.project.id, item.project);
      break;
    case "project-removed":
      state.projects.delete(item.projectId);
      break;
    case "thread-upserted":
      state.threads.set(item.thread.id, item.thread);
      // A thread we created from the glasses becomes subscribable only now.
      if (
        state.detail?.threadId === item.thread.id &&
        state.detail.thread === null &&
        !detailController
      )
        ensureThreadDetail(bridge, item.thread.id, true);
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
  detailController = null;
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

function openThread(bridge: Bridge, threadId: string) {
  state.screen = { kind: "thread", threadId, scroll: 0 };
  state.actionCursor = 0;
  ensureThreadDetail(bridge, threadId);
  scheduleRender(bridge);
}

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
      case "projectThreads":
        if (eventType === OsEventTypeList.SCROLL_TOP_EVENT) {
          state.cursor = Math.max(0, state.cursor - 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.SCROLL_BOTTOM_EVENT) {
          state.cursor = Math.min(state.listedRows.length - 1, state.cursor + 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.CLICK_EVENT) {
          const row = state.listedRows[state.cursor];
          if (!row) break;
          if (row.kind === "thread") openThread(bridge, row.threadId);
          else if (screen.kind === "projectThreads")
            void startDictation(bridge, { kind: "new", projectId: screen.projectId });
          else {
            state.screen = { kind: "projectPick" };
            state.cursor = 0;
            scheduleRender(bridge);
          }
        } else if (
          eventType === OsEventTypeList.DOUBLE_CLICK_EVENT &&
          screen.kind === "projectThreads"
        ) {
          state.screen = { kind: "projects" };
          state.cursor = 0;
          scheduleRender(bridge);
        }
        break;

      case "projects":
      case "projectPick":
        if (eventType === OsEventTypeList.SCROLL_TOP_EVENT) {
          state.cursor = Math.max(0, state.cursor - 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.SCROLL_BOTTOM_EVENT) {
          state.cursor = Math.min(state.listedProjectIds.length - 1, state.cursor + 1);
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.CLICK_EVENT) {
          const projectId = state.listedProjectIds[state.cursor];
          if (!projectId) break;
          if (screen.kind === "projects") {
            state.screen = { kind: "projectThreads", projectId };
            state.cursor = 0;
            scheduleRender(bridge);
          } else {
            void startDictation(bridge, { kind: "new", projectId });
          }
        } else if (
          eventType === OsEventTypeList.DOUBLE_CLICK_EVENT &&
          screen.kind === "projectPick"
        ) {
          state.screen = { kind: "list" };
          scheduleRender(bridge);
        }
        break;

      case "thread": {
        if (eventType === OsEventTypeList.SCROLL_TOP_EVENT) {
          state.screen = {
            ...screen,
            scroll: Math.min(maxTailScroll(screen.threadId), screen.scroll + 1),
          };
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.SCROLL_BOTTOM_EVENT) {
          state.screen = { ...screen, scroll: Math.max(0, screen.scroll - 1) };
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.CLICK_EVENT) {
          state.screen = { kind: "actions", threadId: screen.threadId };
          state.actionCursor = 0;
          scheduleRender(bridge);
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          state.screen = backFromThread(screen.threadId);
          stopThreadDetail();
          scheduleRender(bridge);
        }
        break;
      }

      case "actions": {
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
          if (action?.id === "dictate")
            void startDictation(bridge, { kind: "thread", threadId: screen.threadId });
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
          state.screen = { kind: "thread", threadId: screen.threadId, scroll: 0 };
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
          state.screen = { kind: "thread", threadId: screen.threadId, scroll: 0 };
          scheduleRender(bridge);
        }
        break;
      }

      case "recording":
        if (eventType === OsEventTypeList.CLICK_EVENT) {
          void stopDictation(bridge, screen.target, false);
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          void stopDictation(bridge, screen.target, true);
        }
        break;

      case "preview":
        if (eventType === OsEventTypeList.CLICK_EVENT && screen.text) {
          if (screen.target.kind === "thread")
            void sendFollowUp(bridge, screen.target.threadId, screen.text);
          else void createThreadAndStart(bridge, screen.target.projectId, screen.text);
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          state.screen = dictationBack(screen.target);
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
// Companion page (phone side): pairing form, Home Mode setting, state.
// ---------------------------------------------------------------------------

// Null-tolerant lookups: a stale cached index.html without these elements must
// not take down the glasses UI, which lives in this same module.
const pairStateEl = document.getElementById("pair-state");
const pairTokenEl = document.getElementById("pair-token") as HTMLInputElement | null;
const pairConnectEl = document.getElementById("pair-connect") as HTMLButtonElement | null;
const pairDisconnectEl = document.getElementById("pair-disconnect") as HTMLButtonElement | null;
const homeModeEls = Array.from(
  document.querySelectorAll<HTMLInputElement>('input[name="homeMode"]'),
);

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

function refreshSettingsUi() {
  for (const el of homeModeEls) el.checked = el.value === state.homeMode;
}

function parseHomeMode(raw: string): HomeMode {
  return raw === "project" || raw === "focus" ? raw : "lista";
}

function wireCompanionPage() {
  if (pairStateEl && pairTokenEl && pairConnectEl && pairDisconnectEl) {
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
  for (const el of homeModeEls) {
    el.addEventListener("change", () => {
      if (!el.checked) return;
      state.homeMode = parseHomeMode(el.value);
      void storeSet(HOME_MODE_KEY, state.homeMode);
      // Re-enter the home layout only from list-level screens; never yank the
      // user out of a thread, dictation, or preview.
      const kind = state.screen.kind;
      if (bridgeStorage && (kind === "list" || kind === "projects" || kind === "projectThreads"))
        goHome(bridgeStorage);
    });
  }
}

// ---------------------------------------------------------------------------
// Boot.
// ---------------------------------------------------------------------------

async function main() {
  reportStatus("waiting for Even bridge…");
  const bridge = await waitForEvenAppBridge();
  bridgeStorage = bridge;
  // #home=<mode> overrides for this session only — handy for dev and for
  // trying a mode from a QR without touching the stored setting.
  const homeOverride = new URLSearchParams(location.hash.slice(1)).get("home");
  state.homeMode = homeOverride
    ? parseHomeMode(homeOverride)
    : parseHomeMode(await storeGet(HOME_MODE_KEY));
  refreshSettingsUi();
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
