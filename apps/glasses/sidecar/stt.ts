// STT sidecar: receives raw PCM from the glasses app, forwards it to Groq
// Whisper, and returns the transcript. Runs next to the dev server so the
// API key never reaches the client. Start with:
//   node --env-file=.env.local sidecar/stt.ts
import { createServer } from "node:http";

const GROQ_API_KEY = process.env.GROQ_API_KEY;
if (!GROQ_API_KEY) {
  console.error(
    "[stt] GROQ_API_KEY missing — start with: node --env-file=.env.local sidecar/stt.ts",
  );
  process.exit(1);
}
const PORT = Number(process.env.GLASSES_STT_PORT ?? 5176);
const GROQ_URL = "https://api.groq.com/openai/v1/audio/transcriptions";
const MODEL = process.env.GLASSES_STT_MODEL ?? "whisper-large-v3-turbo";

// Input is what the G2 mic delivers: PCM 16 kHz, signed 16-bit LE, mono.
const SAMPLE_RATE = 16000;

function wavFromPcm(pcm: Buffer): Buffer {
  const header = Buffer.alloc(44);
  const byteRate = SAMPLE_RATE * 2;
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(1, 22); // mono
  header.writeUInt32LE(SAMPLE_RATE, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([header, pcm]);
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);
  if (req.method === "GET" && url.pathname === "/stt/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, model: MODEL }));
    return;
  }
  if (req.method !== "POST" || url.pathname !== "/stt") {
    res.writeHead(404).end();
    return;
  }
  try {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk as Buffer);
    const pcm = Buffer.concat(chunks);
    if (pcm.length < SAMPLE_RATE / 5) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ text: "" }));
      return;
    }
    const form = new FormData();
    form.append(
      "file",
      new Blob([new Uint8Array(wavFromPcm(pcm))], { type: "audio/wav" }),
      "dictation.wav",
    );
    form.append("model", MODEL);
    const prompt = url.searchParams.get("prompt");
    if (prompt) form.append("prompt", prompt.slice(0, 800));
    const language = url.searchParams.get("language");
    if (language) form.append("language", language);
    const started = Date.now();
    const groqRes = await fetch(GROQ_URL, {
      method: "POST",
      headers: { Authorization: `Bearer ${GROQ_API_KEY}` },
      body: form,
    });
    if (!groqRes.ok) {
      const detail = await groqRes.text();
      console.error(`[stt] groq ${groqRes.status}: ${detail.slice(0, 300)}`);
      res.writeHead(502, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: `groq ${groqRes.status}` }));
      return;
    }
    const { text } = (await groqRes.json()) as { text: string };
    console.log(
      `[stt] ${(pcm.length / (SAMPLE_RATE * 2)).toFixed(1)}s audio → ${Date.now() - started}ms → "${text.slice(0, 80)}"`,
    );
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ text: text.trim() }));
  } catch (err) {
    console.error("[stt] error:", err);
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: String(err) }));
  }
});

server.listen(PORT, () => console.log(`[stt] listening on :${PORT} (model ${MODEL})`));
