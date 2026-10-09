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
  #stats { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 18px; font-size: 13px; }
  #stats-summary, #stats-more { display: flex; flex-wrap: wrap; gap: 6px 18px; }
  #stats-more[hidden] { display: none; }
  #stats b { font-variant-numeric: tabular-nums; }
  #stats-toggle { padding: 1px 8px; font-size: 12px; }
  main { display: grid; grid-template-columns: 1fr; gap: 16px; padding: 16px; }
  @media (min-width: 1000px) { main { grid-template-columns: 3fr 2fr; } }
  form { display: flex; gap: 8px; margin-bottom: 10px; }
  input[type=search] { flex: 1; padding: 6px 8px; font-size: 14px; }
  input[type=number] { width: 70px; padding: 6px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }
  thead th { position: sticky; top: 0; background: Canvas; }
  thead th button { all: unset; cursor: pointer; font-weight: bold; }
  thead th button:focus-visible { outline: 2px solid var(--accent); }
  tbody tr { cursor: pointer; }
  tbody tr:hover { background: #8882; }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
  td.name { word-break: break-word; }
  a { color: var(--accent); }
  #tooltip { position: fixed; z-index: 10; pointer-events: none; transform: translate(-50%, -100%); padding: 3px 7px; border-radius: 4px; background: #222; color: #fff; font-size: 12px; white-space: nowrap; font-variant-numeric: tabular-nums; }
  #tooltip.below { transform: translate(-50%, 0); }
  #tooltip[hidden] { display: none; }
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
    <div id="stats-summary">
      <span>uptime <b data-stat="uptime_seconds">0</b></span>
      <span>torrents <b data-stat="torrents">-</b></span>
      <span>total size <b data-stat="torrents_size">-</b></span>
      <span>database size <b data-stat="database_size">-</b></span>
    </div>
    <button id="stats-toggle" type="button" aria-controls="stats-more" aria-expanded="false" title="all statistics">&gt;&gt;</button>
    <div id="stats-more" hidden>
    <span>nodes <b data-stat="nodes">0</b></span>
    <span>hashes <b data-stat="hashes_seen">0</b></span>
    <span>with metadata <b data-stat="with_metadata">0</b></span>
    <span>connections <b data-stat="fetch_connections">0</b></span>
    <span>fetch attempts <b data-stat="fetch_attempts">0</b></span>
    <span>in progress <b data-stat="fetch_in_progress">0</b></span>
    <span>failed <b data-stat="fetch_failed">0</b></span>
    <span>lookups <b data-stat="lookups_started">0</b></span>
    <span>samples <b data-stat="samples_received">0</b></span>
    <span>sent <b data-stat="packets_sent">0</b></span>
    <span>received <b data-stat="packets_received">0</b></span>
    <span>in database <b data-stat="index_documents">-</b></span>
    </div>
  </div>
</header>
<main>
  <section id="search">
    <form id="search-form">
      <input id="query" type="search" placeholder="Search torrent names and file paths" autocomplete="off">
      <input id="limit" type="number" min="1" max="200" value="20" title="max results">
      <button type="submit">Search</button>
    </form>
    <table id="results">
      <thead><tr><th data-sort="name"><button type="button">Name</button></th><th class="num" data-sort="size"><button type="button">Size</button></th><th class="num" data-sort="file_count"><button type="button">Files</button></th><th class="num" data-sort="seen_count"><button type="button">Seen</button></th><th class="num" data-sort="announce_count"><button type="button">Announces</button></th><th class="num" data-sort="last_seen"><button type="button">Last seen</button></th><th>Magnet</th></tr></thead>
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
  </aside>
</main>
<div id="tooltip" role="tooltip" hidden></div>
<script>
(function () {
  "use strict";
  var MAGNET_PREFIX = "magnet:?xt=urn:btih:";
  var EMPTY_MESSAGE = "No fetched torrents match yet. Metadata arrives as peers answer.";
  var UNAVAILABLE_MESSAGE = "Search database unavailable, retrying\u2026";
  var searchTimer = null;
  var sortState = { sort: null, order: null };

  function clearChildren(element) { while (element.firstChild) { element.removeChild(element.firstChild); } }
  function formatBytes(n) {
    var units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]; var i = 0; var v = Number(n) || 0;
    while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
    return (i === 0 ? v : v.toFixed(1)) + " " + units[i];
  }
  function formatCount(n) {
    var units = ["", "k", "M", "B", "T"]; var v = Math.round(Number(n) || 0);
    if (Math.abs(v) < 1000) { return String(v); }
    var scaled = v / 1000; var i = 1;
    while (Math.round(scaled * 10) / 10 >= 1000 && i < units.length - 1) { scaled /= 1000; i += 1; }
    return String(Math.round(scaled * 10) / 10) + units[i];
  }
  function formatFull(n) { return String(Math.round(Number(n) || 0)).replace(/\\B(?=(\\d{3})+(?!\\d))/g, " "); }
  function setCount(element, value) {
    var short = formatCount(value); var full = formatFull(value);
    element.textContent = short;
    if (short === full) { element.removeAttribute("data-full"); } else { element.setAttribute("data-full", full); }
  }
  function formatDuration(seconds) {
    var s = Math.max(0, Math.round(Number(seconds) || 0)); var d = Math.floor(s / 86400); var h = Math.floor(s % 86400 / 3600); var m = Math.floor(s % 3600 / 60);
    return (d ? d + "d " : "") + (d || h ? h + "h " : "") + (d || h || m ? m + "m " : "") + (s % 60) + "s";
  }
  function formatTime(unix) { return unix ? new Date(unix * 1000).toLocaleTimeString() : "-"; }
  function cell(row, text, cls) { var td = document.createElement("td"); td.textContent = text; if (cls) { td.className = cls; } row.appendChild(td); return td; }
  function countCell(row, value) { var td = document.createElement("td"); td.className = "num"; setCount(td, value); row.appendChild(td); return td; }
  function fetchJson(url) { return fetch(url, { cache: "no-store" }).then(function (r) { return r.json(); }); }

  function refreshStats() {
    fetchJson("/api/stats").then(function (stats) {
      var spans = document.querySelectorAll("[data-stat]");
      for (var i = 0; i < spans.length; i += 1) {
        var key = spans[i].getAttribute("data-stat"); var value = stats[key];
        if (value === undefined) { spans[i].textContent = "-"; spans[i].removeAttribute("data-full"); }
        else if (key === "uptime_seconds") { spans[i].textContent = formatDuration(value); }
        else if (key === "torrents_size" || key === "database_size") { spans[i].textContent = formatBytes(value); }
        else { setCount(spans[i], value); }
      }
    }).catch(function () {});
  }

  function renderResults(results, message) {
    var body = document.querySelector("#results tbody"); clearChildren(body);
    var empty = document.getElementById("empty");
    empty.textContent = message || EMPTY_MESSAGE;
    empty.hidden = results.length > 0;
    results.forEach(function (item) {
      var tr = document.createElement("tr"); tr.dataset.hash = item.info_hash;
      cell(tr, item.name, "name"); cell(tr, formatBytes(item.size), "num"); countCell(tr, item.file_count);
      countCell(tr, item.seen_count); countCell(tr, item.announce_count); cell(tr, formatTime(item.last_seen), "num");
      var td = document.createElement("td"); var a = document.createElement("a"); a.textContent = "magnet";
      if (typeof item.magnet === "string" && item.magnet.indexOf(MAGNET_PREFIX) === 0) { a.href = item.magnet; }
      a.addEventListener("click", function (event) { event.stopPropagation(); });
      td.appendChild(a); tr.appendChild(td);
      tr.addEventListener("click", function () { loadDetail(item.info_hash); });
      body.appendChild(tr);
    });
  }

  function runSearch() {
    var q = document.getElementById("query").value; var limit = document.getElementById("limit").value || "20";
    var sortPart = sortState.sort ? "&sort=" + encodeURIComponent(sortState.sort) + "&order=" + encodeURIComponent(sortState.order) : "";
    fetchJson("/api/search?q=" + encodeURIComponent(q) + "&limit=" + encodeURIComponent(limit) + sortPart).then(function (payload) {
      if (payload.error) { renderResults([], UNAVAILABLE_MESSAGE); } else { renderResults(payload.results || []); }
    }).catch(function () {});
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
      addField(dl, "fetched at", formatTime(meta.fetched_at));
    }
    addField(dl, "fetch state", detail.fetch_state + " (attempts " + detail.fetch_attempts + (detail.last_error ? ", last error " + detail.last_error : "") + ")");
    addField(dl, "seen / announces", detail.seen_count + " / " + detail.announce_count);
    addField(dl, "first seen", formatTime(detail.first_seen)); addField(dl, "last seen", formatTime(detail.last_seen));
    var files = document.getElementById("detail-files"); clearChildren(files);
    (meta ? meta.files : []).forEach(function (f) { var tr = document.createElement("tr"); cell(tr, f.path); cell(tr, formatBytes(f.length), "num"); files.appendChild(tr); });
    panel.hidden = false;
  }

  var tooltip = document.getElementById("tooltip");
  function showTooltip(target) {
    var r = target.getBoundingClientRect(); var below = r.top < 40;
    tooltip.textContent = target.getAttribute("data-full"); tooltip.className = below ? "below" : "";
    tooltip.style.left = (r.left + r.width / 2) + "px"; tooltip.style.top = (below ? r.bottom + 6 : r.top - 6) + "px";
    tooltip.hidden = false;
  }
  function fullTarget(event) { return event.target && event.target.closest ? event.target.closest("[data-full]") : null; }
  document.addEventListener("mouseover", function (event) { var target = fullTarget(event); if (target) { showTooltip(target); } });
  document.addEventListener("mouseout", function (event) { if (fullTarget(event)) { tooltip.hidden = true; } });

  function loadDetail(hash) { fetchJson("/api/torrent/" + encodeURIComponent(hash)).then(function (detail) { if (!detail.error) { renderDetail(detail); } }).catch(function () {}); }

  function showSort() {
    var headers = document.querySelectorAll("#results thead th[data-sort]");
    for (var i = 0; i < headers.length; i += 1) {
      var key = headers[i].getAttribute("data-sort"); var button = headers[i].querySelector("button");
      button.textContent = button.textContent.replace(/ [\u25b2\u25bc]$/, "");
      if (key === sortState.sort) {
        headers[i].setAttribute("aria-sort", sortState.order === "asc" ? "ascending" : "descending");
        button.textContent += sortState.order === "asc" ? " \u25b2" : " \u25bc";
      } else {
        headers[i].removeAttribute("aria-sort");
      }
    }
  }
  function readSortFromAddress() {
    var match = /^#sort=([a-z_]+)&order=(asc|desc)$/.exec(location.hash);
    if (match && document.querySelector('#results thead th[data-sort="' + match[1] + '"]')) { sortState = { sort: match[1], order: match[2] }; }
    showSort();
  }
  function sortBy(key) {
    var order = key === sortState.sort ? (sortState.order === "asc" ? "desc" : "asc") : (key === "name" ? "asc" : "desc");
    sortState = { sort: key, order: order };
    history.replaceState(null, "", "#sort=" + key + "&order=" + order);
    showSort(); runSearch();
  }
  Array.prototype.forEach.call(document.querySelectorAll("#results thead th[data-sort] button"), function (button) {
    button.addEventListener("click", function () { sortBy(button.parentNode.getAttribute("data-sort")); });
  });
  readSortFromAddress();
  document.getElementById("search-form").addEventListener("submit", function (event) { event.preventDefault(); runSearch(); });
  document.getElementById("query").addEventListener("input", function () { clearTimeout(searchTimer); searchTimer = setTimeout(runSearch, 300); });
  document.getElementById("stats-toggle").addEventListener("click", function () {
    var more = document.getElementById("stats-more"); more.hidden = !more.hidden;
    this.setAttribute("aria-expanded", String(!more.hidden)); this.textContent = more.hidden ? ">>" : "<<";
  });
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
