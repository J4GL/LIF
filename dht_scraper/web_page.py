"""The single HTML page of the web interface. No external assets, no innerHTML."""

INDEX_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DHT scraper</title>
<style>
  :root { color-scheme: light dark; --line: #8884; --accent: #2a6fdb; }
  body { margin: 0; font: 14px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  header { padding: 12px 16px; border-bottom: 1px solid var(--line); }
  h1 { margin: 0 0 8px; font-size: 18px; }
  #stats { display: flex; flex-wrap: wrap; gap: 6px 18px; font-size: 13px; }
  #stats b { font-variant-numeric: tabular-nums; }
  main { display: grid; grid-template-columns: 1fr; gap: 16px; padding: 16px; }
  @media (min-width: 1000px) { main { grid-template-columns: 3fr 2fr; } }
  form { display: flex; gap: 8px; margin-bottom: 10px; }
  input[type=search] { flex: 1; padding: 6px 8px; font-size: 14px; }
  input[type=number] { width: 70px; padding: 6px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }
  thead th { position: sticky; top: 0; background: Canvas; }
  tbody tr { cursor: pointer; }
  tbody tr:hover { background: #8882; }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
  td.name { word-break: break-word; }
  a { color: var(--accent); }
  #detail { border: 1px solid var(--line); border-radius: 6px; padding: 12px; align-self: start; position: sticky; top: 12px; }
  #detail h2 { margin: 0 0 8px; font-size: 16px; word-break: break-word; }
  code { font-size: 12px; word-break: break-all; }
  dl { display: grid; grid-template-columns: max-content 1fr; gap: 4px 12px; margin: 8px 0; }
  dt { opacity: .7; }
  #files { max-height: 50vh; overflow: auto; }
  .muted { opacity: .7; }
  button { padding: 6px 10px; }
</style>
</head>
<body>
<header>
  <h1>DHT scraper</h1>
  <div id="stats">
    <span>uptime <b data-stat="uptime_seconds">0</b>s</span>
    <span>nodes <b data-stat="nodes">0</b></span>
    <span>hashes <b data-stat="hashes_seen">0</b></span>
    <span>with metadata <b data-stat="with_metadata">0</b></span>
    <span>fetch queue <b data-stat="fetch_queue_size">0</b></span>
    <span>in progress <b data-stat="fetch_in_progress">0</b></span>
    <span>failed <b data-stat="fetch_failed">0</b></span>
    <span>samples <b data-stat="samples_received">0</b></span>
    <span>sent <b data-stat="packets_sent">0</b></span>
    <span>received <b data-stat="packets_received">0</b></span>
  </div>
</header>
<main>
  <section id="search">
    <form id="search-form">
      <input id="query" type="search" placeholder="Search torrent names and file paths" autocomplete="off">
      <input id="limit" type="number" min="1" max="200" value="50" title="max results">
      <button type="submit">Search</button>
    </form>
    <table id="results">
      <thead><tr><th>Name</th><th class="num">Size</th><th class="num">Files</th><th class="num">Seen</th><th class="num">Announces</th><th class="num">Last seen</th><th>Magnet</th></tr></thead>
      <tbody></tbody>
    </table>
    <p id="empty" class="muted" hidden>No fetched torrents match yet. Metadata arrives as peers answer.</p>
  </section>
  <aside id="detail" hidden>
    <button id="close-detail" type="button">Close</button>
    <h2 id="detail-name"></h2>
    <p><code id="detail-hash"></code></p>
    <p><a id="detail-magnet">magnet link</a></p>
    <dl id="detail-fields"></dl>
    <h3>Files</h3>
    <div id="files"><table><thead><tr><th>Path</th><th class="num">Length</th></tr></thead><tbody id="detail-files"></tbody></table></div>
    <h3>Candidate peers</h3>
    <p id="detail-peers" class="muted"></p>
  </aside>
</main>
<script>
(function () {
  "use strict";
  var MAGNET_PREFIX = "magnet:?xt=urn:btih:";
  var searchTimer = null;

  function clearChildren(element) { while (element.firstChild) { element.removeChild(element.firstChild); } }
  function formatBytes(n) {
    var units = ["B", "KiB", "MiB", "GiB", "TiB"]; var i = 0; var v = Number(n) || 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
    return (i === 0 ? v : v.toFixed(1)) + " " + units[i];
  }
  function formatTime(unix) { return unix ? new Date(unix * 1000).toLocaleTimeString() : "-"; }
  function cell(row, text, cls) { var td = document.createElement("td"); td.textContent = text; if (cls) { td.className = cls; } row.appendChild(td); return td; }
  function fetchJson(url) { return fetch(url, { cache: "no-store" }).then(function (r) { return r.json(); }); }

  function refreshStats() {
    fetchJson("/api/stats").then(function (stats) {
      var spans = document.querySelectorAll("[data-stat]");
      for (var i = 0; i < spans.length; i += 1) {
        var key = spans[i].getAttribute("data-stat"); var value = stats[key];
        spans[i].textContent = key === "uptime_seconds" ? Math.round(value) : String(value);
      }
    }).catch(function () {});
  }

  function renderResults(results) {
    var body = document.querySelector("#results tbody"); clearChildren(body);
    document.getElementById("empty").hidden = results.length > 0;
    results.forEach(function (item) {
      var tr = document.createElement("tr"); tr.dataset.hash = item.info_hash;
      cell(tr, item.name, "name"); cell(tr, formatBytes(item.size), "num"); cell(tr, String(item.file_count), "num");
      cell(tr, String(item.seen_count), "num"); cell(tr, String(item.announce_count), "num"); cell(tr, formatTime(item.last_seen), "num");
      var td = document.createElement("td"); var a = document.createElement("a"); a.textContent = "magnet";
      if (typeof item.magnet === "string" && item.magnet.indexOf(MAGNET_PREFIX) === 0) { a.href = item.magnet; }
      a.addEventListener("click", function (event) { event.stopPropagation(); });
      td.appendChild(a); tr.appendChild(td);
      tr.addEventListener("click", function () { loadDetail(item.info_hash); });
      body.appendChild(tr);
    });
  }

  function runSearch() {
    var q = document.getElementById("query").value; var limit = document.getElementById("limit").value || "50";
    fetchJson("/api/search?q=" + encodeURIComponent(q) + "&limit=" + encodeURIComponent(limit)).then(function (payload) { renderResults(payload.results || []); }).catch(function () {});
  }

  function addField(dl, label, value) {
    var dt = document.createElement("dt"); dt.textContent = label; var dd = document.createElement("dd"); dd.textContent = value; dl.appendChild(dt); dl.appendChild(dd);
  }

  function renderDetail(detail) {
    var meta = detail.metadata; var panel = document.getElementById("detail");
    document.getElementById("detail-name").textContent = meta ? meta.name : "(metadata not fetched)";
    document.getElementById("detail-hash").textContent = detail.info_hash;
    var link = document.getElementById("detail-magnet"); link.removeAttribute("href");
    if (typeof detail.magnet === "string" && detail.magnet.indexOf(MAGNET_PREFIX) === 0) { link.href = detail.magnet; }
    var dl = document.getElementById("detail-fields"); clearChildren(dl);
    if (meta) {
      addField(dl, "size", formatBytes(meta.size)); addField(dl, "piece length", formatBytes(meta.piece_length));
      addField(dl, "files", String(meta.file_count)); addField(dl, "private", meta.is_private ? "yes" : "no");
      addField(dl, "fetched from", meta.source_peer ? meta.source_peer.ip + ":" + meta.source_peer.port : "-");
      addField(dl, "fetched at", formatTime(meta.fetched_at));
    }
    addField(dl, "fetch state", detail.fetch_state + " (attempts " + detail.fetch_attempts + (detail.last_error ? ", last error " + detail.last_error : "") + ")");
    addField(dl, "seen / announces", detail.seen_count + " / " + detail.announce_count);
    addField(dl, "first seen", formatTime(detail.first_seen)); addField(dl, "last seen", formatTime(detail.last_seen));
    var files = document.getElementById("detail-files"); clearChildren(files);
    (meta ? meta.files : []).forEach(function (f) { var tr = document.createElement("tr"); cell(tr, f.path); cell(tr, formatBytes(f.length), "num"); files.appendChild(tr); });
    document.getElementById("detail-peers").textContent = detail.peers.length ? detail.peers.map(function (p) { return p.ip + ":" + p.port; }).join(", ") : "none";
    panel.hidden = false;
  }

  function loadDetail(hash) { fetchJson("/api/torrent/" + encodeURIComponent(hash)).then(function (detail) { if (!detail.error) { renderDetail(detail); } }).catch(function () {}); }

  document.getElementById("search-form").addEventListener("submit", function (event) { event.preventDefault(); runSearch(); });
  document.getElementById("query").addEventListener("input", function () { clearTimeout(searchTimer); searchTimer = setTimeout(runSearch, 300); });
  document.getElementById("close-detail").addEventListener("click", function () { document.getElementById("detail").hidden = true; });
  refreshStats(); runSearch();
  setInterval(refreshStats, 2000);
  setInterval(function () { if (!document.getElementById("query").value) { runSearch(); } }, 5000);
}());
</script>
</body>
</html>
"""


# Parents: CatalogRequestHandler.dispatch
# Keywords: html, index page, render, bytes
def render_index_page() -> bytes:
    assert "innerHTML" not in INDEX_PAGE_HTML and "src=" not in INDEX_PAGE_HTML and "<link" not in INDEX_PAGE_HTML
    result = INDEX_PAGE_HTML.encode("utf-8")
    assert b"<html" in result and b"</html>" in result
    return result
