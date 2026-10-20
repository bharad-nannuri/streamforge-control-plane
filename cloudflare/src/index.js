import { Container, getContainer } from "@cloudflare/containers";

export class StreamForgeContainer extends Container {
  defaultPort = 8080;
  requiredPorts = [8080];
  sleepAfter = "20m";
  enableInternet = false;

  async fetch(request) {
    // StreamForge has a deliberately heavy cold start (Postgres + Kafka + four
    // Spring services). Wait for the public control-plane port instead of
    // forwarding the first request while the runtime is still booting.
    await this.startAndWaitForPorts({
      ports: [8080],
      cancellationOptions: {
        instanceGetTimeoutMS: 15000,
        portReadyTimeoutMS: 180000,
        waitInterval: 500
      }
    });

    return this.containerFetch(request);
  }
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
