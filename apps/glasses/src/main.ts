import {
  AudioInputSource,
  CreateStartUpPageContainer,
  OsEventTypeList,
  RebuildPageContainer,
  TextContainerProperty,
  waitForEvenAppBridge,
} from "@evenrealities/even_hub_sdk";
import {
  type ClientOrchestrationCommand,
  CommandId,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  type OrchestrationProjectShell,
  type OrchestrationShellStreamItem,
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
// Auth: pairing token (#token=…) → bearer → per-connection WebSocket ticket.
// Dev-only storage; on-device this moves to bridge.setLocalStorage, the only
// persistence that survives Even App restarts.
// ---------------------------------------------------------------------------

const BEARER_KEY = "t3glasses.bearer";

async function getBearer(): Promise<string> {
  const stored = localStorage.getItem(BEARER_KEY);
  if (stored) return stored;
  const pairToken = new URLSearchParams(location.hash.slice(1)).get("token");
  if (!pairToken) throw new Error("Not paired: open with #token=<pairing token>");
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
  localStorage.setItem(BEARER_KEY, json.access_token);
  return json.access_token;
}

async function getSocketUrl(): Promise<string> {
  const bearer = await getBearer();
  const res = await fetch("/api/auth/websocket-ticket", {
    method: "POST",
    headers: { Authorization: `Bearer ${bearer}` },
  });
  if (res.status === 401) {
    localStorage.removeItem(BEARER_KEY);
    throw new Error("Session expired: pair again with #token=<pairing token>");
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

function threadActions(t: OrchestrationThreadShell): { label: string; id: string }[] {
  const actions = [{ label: "Detta follow-up", id: "dictate" }];
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

function buildThreadPage(threadId: string): RebuildPageContainer {
  const t = state.threads.get(threadId);
  if (!t) return textPage("thread", "Thread non trovato\n\n2x tap: indietro");
  const project = state.projects.get(t.projectId)?.title ?? "?";
  const actions = threadActions(t);
  if (state.actionCursor >= actions.length) state.actionCursor = 0;
  const lines = [
    `${project} · ${t.title}`.slice(0, 60),
    threadStatusLine(t),
    "",
    ...actions.map((a, i) => `${i === state.actionCursor ? ">" : " "} ${a.label}`),
    "",
    "2x tap: indietro",
  ];
  return textPage("thread", lines.join("\n"));
}

function buildScreen(): RebuildPageContainer {
  switch (state.screen.kind) {
    case "list":
      return buildListPage();
    case "thread":
      return buildThreadPage(state.screen.threadId);
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
          else if (action?.id === "interrupt")
            void interruptTurn(screen.threadId).catch((err) =>
              reportStatus(`interrupt error: ${String(err)}`),
            );
        } else if (eventType === OsEventTypeList.DOUBLE_CLICK_EVENT) {
          state.screen = { kind: "list" };
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
// Boot.
// ---------------------------------------------------------------------------

async function main() {
  reportStatus("waiting for Even bridge…");
  const bridge = await waitForEvenAppBridge();
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
    reportStatus(`createStartUpPageContainer failed: ${created}`);
    return;
  }
  wireInput(bridge);

  reportStatus("authenticating…");
  const socketUrl = await getSocketUrl();
  reportStatus("opening websocket…");
  await Effect.runPromise(Effect.scoped(runShellSubscription(socketUrl, bridge))).catch((err) => {
    reportStatus(`connection lost: ${String(err)}`);
  });
}

main().catch((err) => reportStatus(`fatal: ${String(err)}`));
