function headersFor(kind, fileName, object) {
  const headers = new Headers();
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("ETag", object.httpEtag);

  if (fileName === "manifest.json") {
    headers.set("Content-Type", "application/json; charset=utf-8");
    headers.set("Cache-Control", kind === "snapshots" ? "public, max-age=30" : "public, max-age=300");
  } else if (fileName === "current.3le") {
    headers.set("Content-Type", "text/plain; charset=utf-8");
    headers.set("Content-Encoding", "gzip");
    headers.set("Cache-Control", "public, max-age=3600");
  } else if (fileName === "current.3le.gz") {
    headers.set("Content-Type", "application/gzip");
    headers.set("Cache-Control", "public, max-age=3600");
  } else if (fileName === "current.json") {
    headers.set("Content-Type", "application/json; charset=utf-8");
    headers.set("Cache-Control", "public, max-age=60");
  } else if (fileName === "current.json.gz") {
    headers.set("Content-Type", "application/gzip");
    headers.set("Cache-Control", "public, max-age=60");
  }

  return headers;
}

function prefixFor(kind, env) {
  if (kind === "snapshots") {
    return (env.SNAPSHOT_PREFIX || "snapshots/").replace(/^\/+|\/+$/g, "");
  }
  return (env.CATALOG_PREFIX || "catalog/").replace(/^\/+|\/+$/g, "");
}

function keyFor(kind, fileName, env) {
  const prefix = prefixFor(kind, env);
  return `${prefix}/${fileName}`;
}

async function serveObject(kind, fileName, env) {
  const storedFile = fileName === "current.3le" ? `${fileName}.gz` : fileName;
  const object = await env.CATALOG_BUCKET.get(keyFor(kind, storedFile, env));
  if (!object) {
    return new Response("Not found\n", { status: 404 });
  }
  return new Response(object.body, {
    headers: headersFor(kind, fileName, object),
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
      return serveObject("catalog", "manifest.json", env);
    }
    if (url.pathname === "/catalog/current.3le") {
      return serveObject("catalog", "current.3le", env);
    }
    if (url.pathname === "/catalog/current.3le.gz") {
      return serveObject("catalog", "current.3le.gz", env);
    }

    if (url.pathname === "/snapshots" || url.pathname === "/snapshots/") {
      return new Response("Satellite position snapshots\n", {
        headers: { "Content-Type": "text/plain; charset=utf-8" },
      });
    }
    if (url.pathname === "/snapshots/manifest.json") {
      return serveObject("snapshots", "manifest.json", env);
    }
    if (url.pathname === "/snapshots/current.json") {
      return serveObject("snapshots", "current.json", env);
    }
    if (url.pathname === "/snapshots/current.json.gz") {
      return serveObject("snapshots", "current.json.gz", env);
    }

    return new Response("Not found\n", { status: 404 });
  },
};
