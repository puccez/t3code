import { defineConfig } from "vite";
import { DEV_PROXIED_PATH_PREFIXES } from "@t3tools/shared/devProxy";

// Single-origin dev, same contract as apps/web: the glasses webview only ever
// talks to its own origin and Vite forwards backend paths to the T3 server.
const serverPort = process.env.T3CODE_PORT ?? "13773";
const devProxyTarget = `http://localhost:${serverPort}`;

export default defineConfig({
  server: {
    port: Number(process.env.GLASSES_PORT ?? 5175),
    host: true,
    proxy: {
      ...Object.fromEntries(
        DEV_PROXIED_PATH_PREFIXES.map((prefix) => [
          prefix,
          {
            target: devProxyTarget,
            changeOrigin: true,
            ...(prefix === "/ws" ? { ws: true } : {}),
          },
        ]),
      ),
      "/stt": {
        target: `http://localhost:${process.env.GLASSES_STT_PORT ?? 5176}`,
        changeOrigin: true,
      },
    },
  },
});
