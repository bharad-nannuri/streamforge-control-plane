import { Container, getContainer } from "@cloudflare/containers";

const landingPage = () => new Response(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Engineering Labs — Bharad Nannuri</title>
  <meta name="description" content="Interactive engineering labs for distributed systems, resilience, high availability, and disaster recovery.">
  <style>
    :root{--bg:#070a0f;--panel:#0f151e;--line:#222f3f;--text:#f3f6fa;--muted:#8d9bad;--gold:#f2b84b;--blue:#5aa9ff}
    *{box-sizing:border-box}body{margin:0;min-height:100vh;background:radial-gradient(circle at 15% -10%,#15233a 0,transparent 32%),radial-gradient(circle at 88% 0,rgba(242,184,75,.09),transparent 26%),var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}
    main{max-width:1120px;margin:0 auto;padding:72px 24px}.ey{font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:#7d8ba0;font-weight:800}.hero h1{font-size:clamp(44px,7vw,78px);line-height:.96;letter-spacing:-.05em;margin:13px 0 18px}.hero p{max-width:720px;color:#aab6c5;line-height:1.7;font-size:16px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:46px}.card{display:block;text-decoration:none;color:inherit;border:1px solid var(--line);background:linear-gradient(180deg,#111924,#0c121a);border-radius:20px;padding:28px;transition:.2s ease;min-height:250px}.card:hover{transform:translateY(-3px);border-color:#40536b}.tag{display:inline-flex;border:1px solid #2b394c;border-radius:999px;padding:6px 9px;font-size:9px;font-weight:900;letter-spacing:.12em;color:#9baabd}.card h2{font-size:30px;margin:24px 0 9px;letter-spacing:-.03em}.card p{color:var(--muted);line-height:1.6;margin:0}.launch{display:flex;align-items:center;justify-content:space-between;margin-top:30px;font-size:12px;font-weight:800}.stream .launch{color:var(--gold)}.recovery .launch{color:var(--blue)}.footer{margin-top:34px;color:#657489;font-size:11px}@media(max-width:760px){main{padding-top:44px}.grid{grid-template-columns:1fr}.card{min-height:auto}}
  </style>
</head>
<body><main>
  <section class="hero">
    <div class="ey">BHARAD NANNURI · ENGINEERING PORTFOLIO</div>
    <h1>Engineering Labs</h1>
    <p>Interactive systems built to make reliability behavior visible — generate real traffic, inject failures, observe degradation, and validate recovery paths.</p>
  </section>
  <section class="grid">
    <a class="card stream" href="/streamforge/">
      <span class="tag">DISTRIBUTED SYSTEMS</span>
      <h2>StreamForge</h2>
      <p>Kafka resilience control plane with workload generation, dependency failures, retries, DLQ, replay, consumer lag, latency, and recovery telemetry.</p>
      <div class="launch"><span>Launch lab</span><span>→</span></div>
    </a>
    <a class="card recovery" href="/recoverymesh/">
      <span class="tag">HA / DISASTER RECOVERY</span>
      <h2>RecoveryMesh</h2>
      <p>Database DR control plane with primary/secondary writes, automatic failover, RTO/RPO visibility, reconciliation, and controlled failback.</p>
      <div class="launch"><span>Launch lab</span><span>→</span></div>
    </a>
  </section>
  <div class="footer">Portfolio labs · Java · Spring Boot · Kafka · PostgreSQL · Cloudflare Containers</div>
</main></body></html>`, {
  headers: {"content-type":"text/html; charset=UTF-8","cache-control":"public, max-age=300"}
});

const warmingPage = (name, accent, message) => new Response(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="5"><title>${name} is warming up</title>
<style>html,body{height:100%;margin:0;background:#07090d;color:#f4f7fb;font-family:Inter,system-ui,-apple-system,Segoe UI,sans-serif}body{display:grid;place-items:center}.box{width:min(560px,88vw);padding:34px;border:1px solid #222d3c;border-radius:20px;background:linear-gradient(180deg,#111824,#0e131b);box-shadow:0 20px 70px rgba(0,0,0,.35)}.ey{font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:${accent};font-weight:800}h1{font-size:36px;line-height:1.05;margin:12px 0 10px}p{color:#aeb9c8;line-height:1.6;margin:0 0 22px}.bar{height:8px;background:#171f2b;border-radius:99px;overflow:hidden}.bar:before{content:"";display:block;height:100%;width:35%;background:${accent};border-radius:99px;animation:move 1.2s ease-in-out infinite alternate}small{display:block;margin-top:14px;color:#6f7d90}@keyframes move{from{transform:translateX(-20%)}to{transform:translateX(210%)}}</style></head>
<body><div class="box"><div class="ey">${name}</div><h1>Warming up the lab.</h1><p>${message}</p><div class="bar"></div><small>This page refreshes automatically.</small></div></body></html>`,
{status:503,headers:{"content-type":"text/html; charset=UTF-8","cache-control":"no-store","retry-after":"5"}});

export class StreamForgeContainer extends Container {
  defaultPort = 8080;
  requiredPorts = [8080];
  sleepAfter = "20m";
  enableInternet = false;

  async fetch(request) {
    try {
      await this.startAndWaitForPorts({
        ports:[8080],
        cancellationOptions:{instanceGetTimeoutMS:8000,portReadyTimeoutMS:5000,waitInterval:400}
      });
      return this.containerFetch(request);
    } catch (_) {
      return warmingPage("StreamForge Control Plane", "#f2b84b", "Kafka, PostgreSQL, and the Spring Boot services are starting. This demo sleeps when idle to keep infrastructure cost low.");
    }
  }
}

function stripPrefix(request, prefix) {
  const url = new URL(request.url);
  let path = url.pathname.slice(prefix.length);
  if (!path) path = "/";
  url.pathname = path;
  return new Request(url.toString(), request);
}

async function rewriteForPrefix(response, prefix) {
  const type = response.headers.get("content-type") || "";
  if (!type.includes("text/html") && !type.includes("javascript")) return response;

  let body = await response.text();
  if (prefix === "/streamforge" && type.includes("text/html")) {
    body = body
      .replaceAll('"/api/', '"/streamforge/api/')
      .replaceAll("'/api/", "'/streamforge/api/");
  }

  if (prefix === "/recoverymesh") {
    if (type.includes("text/html")) {
      body = body
        .replaceAll('href="/styles.css"', 'href="/recoverymesh/styles.css"')
        .replaceAll('src="/app.js"', 'src="/recoverymesh/app.js"');
    } else if (type.includes("javascript")) {
      body = body
        .replaceAll("'/api/", "'/recoverymesh/api/")
        .replaceAll('"/api/', '"/recoverymesh/api/');
    }
  }

  const headers = new Headers(response.headers);
  headers.delete("content-length");
  headers.set("cache-control", "no-store");
  return new Response(body, {status:response.status,statusText:response.statusText,headers});
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/_edge/health") {
      return Response.json({
        edge:"healthy",
        runtime:"engineering-labs-router",
        routes:["/streamforge","/recoverymesh"]
      });
    }

    if (path === "/") return landingPage();

    if (path === "/streamforge") {
      return Response.redirect(new URL("/streamforge/", url), 308);
    }

    if (path === "/recoverymesh") {
      return Response.redirect(new URL("/recoverymesh/", url), 308);
    }

    if (path.startsWith("/streamforge/")) {
      const instance = getContainer(env.STREAMFORGE, "portfolio-demo");
      const response = await instance.fetch(stripPrefix(request, "/streamforge"));
      return rewriteForPrefix(response, "/streamforge");
    }

    if (path.startsWith("/recoverymesh/")) {
      const response = await env.RECOVERYMESH_SERVICE.fetch(stripPrefix(request, "/recoverymesh"));
      return rewriteForPrefix(response, "/recoverymesh");
    }

    // Backward compatibility for older StreamForge UI/API calls.
    if (path.startsWith("/api/")) {
      const instance = getContainer(env.STREAMFORGE, "portfolio-demo");
      return instance.fetch(request);
    }

    return new Response("Not Found", {status:404});
  }
};
