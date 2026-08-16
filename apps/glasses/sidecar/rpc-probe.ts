// Dev harness: drive the T3 server RPC the same way the glasses app does,
// from Node. Used for supervised tests that the glasses UI can't trigger yet
// (project/thread creation) and to verify dispatch paths end to end.
//   node sidecar/rpc-probe.ts <pairing-token> [server-origin]
import {
  CommandId,
  MessageId,
  ORCHESTRATION_WS_METHODS,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { makeWsRpcProtocolClient } from "@t3tools/client-runtime/rpc";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as Socket from "effect/unstable/socket/Socket";

const pairToken = process.argv[2];
const positionalOrigin = process.argv[3]?.startsWith("--") ? undefined : process.argv[3];
const origin = positionalOrigin ?? "http://localhost:13773";
if (!pairToken) {
  console.error("usage: node sidecar/rpc-probe.ts <pairing-token> [server-origin]");
  process.exit(1);
}

const SCRATCH_TITLE = "glasses-test";
const SCRATCH_ROOT = `${process.env.HOME}/Projects/evenrealities/scratch-glasses-test`;

async function auth(): Promise<string> {
  const tokenRes = await fetch(`${origin}/oauth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
      subject_token: pairToken!,
      subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
      requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      scope: "orchestration:read orchestration:operate terminal:operate review:write relay:read",
      client_label: "T3 Glasses probe",
      client_device_type: "bot",
    }),
  });
  if (!tokenRes.ok) throw new Error(`token exchange ${tokenRes.status}`);
  const { access_token } = (await tokenRes.json()) as { access_token: string };
  const ticketRes = await fetch(`${origin}/api/auth/websocket-ticket`, {
    method: "POST",
    headers: { Authorization: `Bearer ${access_token}` },
  });
  if (!ticketRes.ok) throw new Error(`ws ticket ${ticketRes.status}`);
  const { ticket } = (await ticketRes.json()) as { ticket: string };
  return `${origin.replace("http", "ws")}/ws?wsTicket=${encodeURIComponent(ticket)}`;
}

const base = () => ({
  commandId: CommandId.make(crypto.randomUUID()),
  createdAt: new Date().toISOString(),
});

const program = (socketUrl: string) =>
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

    const maybeSnapshot = yield* client[ORCHESTRATION_WS_METHODS.subscribeShell]({}).pipe(
      Stream.filter((item) => item.kind === "snapshot"),
      Stream.take(1),
      Stream.runHead,
    );
    if (Option.isNone(maybeSnapshot)) throw new Error("no shell snapshot received");
    const snapshot = maybeSnapshot.value.snapshot;
    console.log(
      `snapshot: ${snapshot.projects.length} projects, ${snapshot.threads.length} threads`,
    );

    const donorThread = snapshot.threads.find((t) => t.modelSelection);
    if (!donorThread) throw new Error("no existing thread to copy modelSelection from");
    // Prefer a Claude Code instance: that CLI is installed and authenticated here.
    const claudeSelection = snapshot.projects.find(
      (p) => p.defaultModelSelection?.instanceId === "claudeAgent",
    )?.defaultModelSelection;
    const modelSelection = claudeSelection ?? donorThread.modelSelection;
    console.log(
      `model=${JSON.stringify(modelSelection)} runtimeMode=${donorThread.runtimeMode} interactionMode=${donorThread.interactionMode}`,
    );

    let project = snapshot.projects.find((p) => p.title === SCRATCH_TITLE);
    if (!project) {
      const projectId = ProjectId.make(crypto.randomUUID());
      const result = yield* client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
        type: "project.create",
        ...base(),
        projectId,
        title: SCRATCH_TITLE,
        workspaceRoot: SCRATCH_ROOT,
        createWorkspaceRootIfMissing: true,
        defaultModelSelection: modelSelection,
      });
      console.log("project.create →", JSON.stringify(result));
      project = { id: projectId } as typeof project & object;
    } else {
      console.log(`project "${SCRATCH_TITLE}" already exists: ${project.id}`);
    }

    // --follow <threadId> reuses an existing thread instead of creating one;
    // --text overrides the prompt.
    const followIdx = process.argv.indexOf("--follow");
    const textIdx = process.argv.indexOf("--text");
    const rmIdx = process.argv.indexOf("--runtime-mode");
    const runtimeMode =
      rmIdx > -1
        ? (process.argv[rmIdx + 1] as typeof donorThread.runtimeMode)
        : donorThread.runtimeMode;
    const promptText =
      textIdx > -1 ? process.argv[textIdx + 1]! : "Rispondi soltanto con la parola: ciao";

    let threadId: ThreadId;
    if (followIdx > -1) {
      threadId = ThreadId.make(process.argv[followIdx + 1]!);
      console.log(`following existing thread ${threadId}`);
    } else {
      threadId = ThreadId.make(crypto.randomUUID());
      const createResult = yield* client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
        type: "thread.create",
        ...base(),
        threadId,
        projectId: project!.id,
        title: "Test dagli occhiali",
        modelSelection,
        runtimeMode,
        interactionMode: donorThread.interactionMode,
        branch: null,
        worktreePath: null,
      });
      console.log("thread.create →", JSON.stringify(createResult));
    }

    const turnResult = yield* client[ORCHESTRATION_WS_METHODS.dispatchCommand]({
      type: "thread.turn.start",
      ...base(),
      threadId,
      message: {
        messageId: MessageId.make(crypto.randomUUID()),
        role: "user",
        text: promptText,
        attachments: [],
      },
      runtimeMode,
      interactionMode: donorThread.interactionMode,
      createdAt: new Date().toISOString(),
      commandId: CommandId.make(crypto.randomUUID()),
    });
    console.log("thread.turn.start →", JSON.stringify(turnResult));
    console.log(`threadId: ${threadId}`);
  });

const socketUrl = await auth();
await Effect.runPromise(Effect.scoped(program(socketUrl)));
console.log("probe done");
process.exit(0);
