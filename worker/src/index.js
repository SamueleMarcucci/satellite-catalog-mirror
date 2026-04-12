function headersFor(fileName, object) {
  const headers = new Headers();
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("ETag", object.httpEtag);

  if (fileName === "manifest.json") {
    headers.set("Content-Type", "application/json; charset=utf-8");
    headers.set("Cache-Control", "public, max-age=300");
  } else if (fileName === "current.3le") {
    headers.set("Content-Type", "text/plain; charset=utf-8");
    headers.set("Content-Encoding", "gzip");
    headers.set("Cache-Control", "public, max-age=3600");
  } else if (fileName === "current.3le.gz") {
    headers.set("Content-Type", "application/gzip");
    headers.set("Cache-Control", "public, max-age=3600");
  }

  return headers;
}

function keyFor(fileName, env) {
  const prefix = (env.CATALOG_PREFIX || "catalog/").replace(/^\/+|\/+$/g, "");
  return `${prefix}/${fileName}`;
}

async function serveObject(fileName, env) {
  const storedFile = fileName === "current.3le" ? "current.3le.gz" : fileName;
  const object = await env.CATALOG_BUCKET.get(keyFor(storedFile, env));
  if (!object) {
    return new Response("Not found\n", { status: 404 });
  }
  return new Response(object.body, {
    headers: headersFor(fileName, object),
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, If-None-Match",
        },
      });
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("Method not allowed\n", { status: 405 });
    }

    if (url.pathname === "/" || url.pathname === "/catalog") {
      return new Response("Satellite TLE catalog mirror\n", {
        headers: { "Content-Type": "text/plain; charset=utf-8" },
      });
    }

    if (url.pathname === "/catalog/manifest.json") {
      return serveObject("manifest.json", env);
    }
    if (url.pathname === "/catalog/current.3le") {
      return serveObject("current.3le", env);
    }
    if (url.pathname === "/catalog/current.3le.gz") {
      return serveObject("current.3le.gz", env);
    }

    return new Response("Not found\n", { status: 404 });
  },
};
