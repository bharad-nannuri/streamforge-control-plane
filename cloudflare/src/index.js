import { Container, getContainer } from "@cloudflare/containers";

export class StreamForgeContainer extends Container {
  defaultPort = 8080;
  sleepAfter = "20m";
  enableInternet = false;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/_edge/health") {
      return Response.json({
        edge: "healthy",
        runtime: "streamforge-cloudflare-container",
        stateModel: "ephemeral-demo"
      });
    }

    const instance = getContainer(env.STREAMFORGE, "portfolio-demo");
    return instance.fetch(request);
  }
};
