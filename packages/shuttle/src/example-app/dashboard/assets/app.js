const $ = (s, root = document) => root.querySelector(s);
const $$ = (s, root = document) => [...root.querySelectorAll(s)];
const esc = (v) =>
  String(v ?? "").replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
const paths = {
  grid: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
  refresh: '<path d="M20 7v5h-5M4 17v-5h5"/><path d="M6.1 7a7 7 0 0 1 11.6-1L20 9M4 15l2.3 3a7 7 0 0 0 11.6-1"/>',
  download: '<path d="M12 3v12m-4-4 4 4 4-4M4 16v4h16v-4"/>',
  search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 4 4"/>',
  database:
    '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  alert: '<path d="m12 3 10 18H2L12 3Z"/><path d="M12 9v5m0 3h.01"/>',
  fields: '<path d="M8 5h13M8 12h13M8 19h13M3 5h.01M3 12h.01M3 19h.01"/>',
  plug: '<path d="M9 3v5m6-5v5M6 8h12v3a6 6 0 0 1-12 0V8Zm6 9v5"/>',
  right: '<path d="m9 5 7 7-7 7"/>',
  left: '<path d="m15 5-7 7 7 7"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  check: '<path d="m5 12 4 4L19 6"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  menu: '<path d="M4 6h16M4 12h16M4 18h16"/>',
  play: '<path d="M7 5v14l11-7L7 5Z"/>',
  box: '<path d="m12 3 9 5-9 5-9-5 9-5ZM3 8v9l9 5 9-5V8M12 13v9"/>',
};
function icon(name) {
  return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${
    paths[name] || paths.box
  }</svg>`;
}
function icons(root = document) {
  for (const e of $$("[data-icon]", root)) e.innerHTML = icon(e.dataset.icon);
}

const projectionTitles = { blocks: "屏蔽关系" };
const statLabels = { total: "总行数", active: "有效", blockers: "发起屏蔽的用户" };
const messageTypes = {
  1: "CAST_ADD",
  2: "CAST_REMOVE",
  3: "REACTION_ADD",
  4: "REACTION_REMOVE",
  5: "LINK_ADD",
  6: "LINK_REMOVE",
  7: "VERIFICATION_ADD",
  8: "VERIFICATION_REMOVE",
  11: "USER_DATA_ADD",
  12: "USERNAME_PROOF",
  14: "FRAME_ACTION",
  15: "LINK_COMPACT_STATE",
};
const reasonLabels = {
  "Invalid mainnet block message": "不是有效的 mainnet block 消息（非 mainnet、缺字段或 hash 异常）",
};
const reasonShort = { "Invalid mainnet block message": "无效的 mainnet 消息" };
const kindLabels = { rebuild: "从数据库重建", backfill: "backfill" };
const triggerLabels = { cli: "命令行", dashboard: "本页面" };
const state = {
  status: null,
  view: "overview",
  projection: null,
  reason: "",
  fid: "",
  offset: 0,
  limit: 20,
  issues: null,
  issueSeq: 0,
  timer: null,
  lastOk: null,
};
let toastTimer;
let previousFocus;
let fidTimer;

function num(n) {
  return n === null || n === undefined ? "—" : Number(n).toLocaleString("en-US");
}
function formatTime(value, full = false) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "时间未知";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "America/Edmonton",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    ...(full ? { year: "numeric", second: "2-digit" } : {}),
    hour12: false,
  }).format(date);
}
function duration(a, b) {
  const ms = (b ? new Date(b) : new Date()) - new Date(a);
  if (!(ms >= 0)) return "";
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s} 秒`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m} 分钟`;
  return `${(m / 60).toFixed(1)} 小时`;
}
function short(v, n = 8) {
  const s = String(v || "");
  return s.length > n * 2 + 3 ? `${s.slice(0, n)}…${s.slice(-n)}` : s;
}
function badge(label, tone = "") {
  return `<span class="badge ${tone}"><i class="badge-dot"></i>${esc(label)}</span>`;
}
function metric(label, value, symbol, note = "", tone = "") {
  return `<article class="metric ${tone}"><span class="metric-icon">${icon(
    symbol,
  )}</span><div class="metric-label">${esc(label)}</div><div class="metric-value">${value}</div>${
    note ? `<div class="metric-note">${esc(note)}</div>` : ""
  }</article>`;
}
function reason(text) {
  return reasonLabels[text] || text || "未记录原因";
}
function title(name) {
  return projectionTitles[name] ? `${projectionTitles[name]}` : name;
}
async function api(url, options = {}) {
  const r = await fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...options.headers,
    },
  });
  const data = await r.json().catch(() => ({ error: `HTTP ${r.status}` }));
  if (!r.ok) throw new Error(data.error || "读取失败，请重试");
  return data;
}
function showError(message) {
  const e = $("#global-error");
  e.textContent = message;
  e.classList.remove("hidden");
}
function clearError() {
  $("#global-error").classList.add("hidden");
}
function toast(message) {
  clearTimeout(toastTimer);
  $("#toast").textContent = message;
  $("#toast").classList.remove("hidden");
  toastTimer = setTimeout(() => $("#toast").classList.add("hidden"), 3200);
}
function sectionError(s) {
  return s && !s.ok ? s.error : "";
}

function runStatus(run) {
  if (!run) return badge("未运行");
  if (run.status === "running") return run.stale ? badge("可能已中断", "amber") : badge("运行中", "purple");
  if (run.status === "succeeded") return badge("成功", "green");
  return badge("失败", "red");
}
function runProgress(run) {
  if (!run) return "";
  const failed = run.failedCount ? `，失败 ${num(run.failedCount)}` : "";
  if (run.total) {
    const pct = Math.min(100, ((run.done + run.failedCount) / run.total) * 100);
    return `<div class="mini-track ${run.failedCount ? "red" : ""}"><div style="width:${pct.toFixed(
      1,
    )}%"></div></div><span class="muted">${num(run.done)} / ${num(run.total)} 批${failed}</span>`;
  }
  const unit = run.kind === "rebuild" ? "个 FID" : "";
  return `<span class="muted">已处理 ${num(run.done)} ${unit}${failed}${
    run.lastFid ? ` · 到 FID ${num(run.lastFid)}` : ""
  }</span>`;
}
function runCell(run) {
  if (!run) return `<div class="run-cell">${badge("从未运行")}</div>`;
  return `<div class="run-cell">${runStatus(run)}${runProgress(run)}<span class="muted">${formatTime(
    run.startedAt,
  )} · ${esc(triggerLabels[run.trigger] || run.trigger)}</span></div>`;
}
function scopeText(run) {
  const fids = run.fids?.length
    ? `FID ${run.fids.length > 6 ? run.fids.slice(0, 6).join(", ") + ` 等 ${run.fids.length} 个` : run.fids.join(", ")}`
    : "全部 FID";
  const proj = run.projections.length ? run.projections.join(", ") : "全量原始（所有消息）";
  return `${proj} · ${fids}`;
}

// ---------- data ----------
async function loadStatus(manual = false) {
  const button = $("#refresh");
  if (manual) {
    button.disabled = true;
    button.classList.add("is-loading");
  }
  try {
    const status = await api("/api/status");
    state.status = status;
    state.lastOk = new Date();
    clearError();
    render();
  } catch (e) {
    // Keep the last good data on screen; never replace it with zeros.
    showError(`刷新失败：${e.message}${state.lastOk ? `（显示的是 ${formatTime(state.lastOk, true)} 的数据）` : ""}`);
    if (!state.status)
      $("#view").innerHTML = `<section class="panel"><div class="panel-empty"><div class="empty-icon">${icon(
        "alert",
      )}</div><strong>无法读取状态</strong>${esc(e.message)}</div></section>`;
  } finally {
    if (manual) {
      button.disabled = false;
      button.classList.remove("is-loading");
    }
    schedule();
  }
}
function anyRunning() {
  const s = state.status;
  return !!(s?.runs?.ok && s.runs.data.some((r) => r.status === "running" && !r.stale));
}
function schedule() {
  clearTimeout(state.timer);
  state.timer = setTimeout(() => loadStatus(), anyRunning() ? 3000 : 30000);
}

// ---------- chrome ----------
function renderNav() {
  const s = state.status;
  $("#all-count").textContent = s.projections.length;
  $("#projection-nav").innerHTML = s.projections
    .map((p) => {
      const issues = p.issues.ok ? p.issues.data.total : 0;
      const running = [p.lastRebuild, p.lastBackfill].some((r) => r?.status === "running" && !r.stale);
      const dot = running ? "purple" : issues ? "amber" : "green";
      return `<button class="nav-table ${
        state.view === "projection" && state.projection === p.name ? "active" : ""
      }" data-projection="${esc(
        p.name,
      )}"><span class="nav-dot ${dot}"></span><span class="nav-table-name">${esc(p.name)}</span>${issues ? `<span class="nav-badge">${num(issues)}</span>` : ""}</button>`;
    })
    .join("");
  $("#overview-nav").classList.toggle("active", state.view === "overview");
  $("#runs-nav").classList.toggle("active", state.view === "runs");
  const running = s.runs.ok ? s.runs.data.filter((r) => r.status === "running" && !r.stale).length : 0;
  $("#running-count").textContent = running;
  $("#running-count").classList.toggle("hidden", !running);
  $("#env-label").textContent = `本机 · schema ${s.schema}`;
  $("#update-time").textContent = `更新于 ${formatTime(state.lastOk, true)}`;
}
function heading(eyebrow, h1, description, action = "") {
  $("#page-eyebrow").textContent = eyebrow;
  $("#page-title").textContent = h1;
  $("#page-description").textContent = description;
  $("#heading-action").innerHTML = action;
  $("#crumb").textContent = h1;
}
function render() {
  if (!state.status) return;
  renderNav();
  if (state.view === "projection" && state.status.projections.some((p) => p.name === state.projection))
    renderProjection();
  else if (state.view === "runs") renderRuns();
  else {
    state.view = "overview";
    renderOverview();
  }
  icons();
}

// ---------- overview ----------
function migrationNotice(s) {
  if (!s.migrations.ok) return `<div class="notice error">读取迁移状态失败：${esc(s.migrations.error)}</div>`;
  if (s.migrations.data.pending.length)
    return `<div class="notice error">数据库有未执行的迁移：<code>${esc(
      s.migrations.data.pending.join(", "),
    )}</code>。先部署 shard（<code>scripts/deploy.sh deploy</code>），之后才有运行记录，页面上的重建 / backfill 也会被拒绝。</div>`;
  return "";
}
function streamTotals(s) {
  if (!s.streams.ok) return null;
  let backlog = 0;
  let dlq = 0;
  for (const st of s.streams.data) {
    dlq += st.dlq;
    for (const g of st.groups) backlog += (g.pending || 0) + (g.lag || 0);
  }
  return { backlog, dlq };
}
function renderOverview() {
  const s = state.status;
  heading(`schema ${s.schema}`, "投影状态", "实时投影、数据缺口与 backfill / 重建记录");
  const issueTotal =
    s.projections.reduce((n, p) => n + (p.issues.ok ? p.issues.data.total : 0), 0) +
    s.orphanIssues.reduce((n, i) => n + i.count, 0);
  const issue24 = s.projections.reduce((n, p) => n + (p.issues.ok ? p.issues.data.last24h : 0), 0);
  const issuesFailed = s.projections.some((p) => !p.issues.ok);
  const totals = streamTotals(s);
  const running = s.runs.ok ? s.runs.data.filter((r) => r.status === "running").length : null;
  $("#metrics").innerHTML = [
    metric("已注册投影", num(s.projections.length), "database", s.projections.map((p) => p.name).join("、")),
    metric(
      "数据缺口记录",
      issuesFailed ? "读取失败" : num(issueTotal),
      "alert",
      issuesFailed ? "" : `近 24 小时新增 ${num(issue24)}`,
      issueTotal ? "amber" : "",
    ),
    metric(
      "实时积压",
      totals ? num(totals.backlog) : "未连接",
      "clock",
      totals ? `pending + lag，DLQ ${num(totals.dlq)}` : sectionError(s.streams),
      totals?.dlq ? "amber" : "",
    ),
    metric(
      "运行中任务",
      running === null ? "—" : num(running),
      "play",
      s.queue.ok
        ? `backfill worker ${s.queue.data.workers ? `在线 ${s.queue.data.workers}` : "不在线"}`
        : "worker 状态未知",
    ),
  ].join("");
  const rows = s.projections
    .map((p) => {
      const stats = p.stats
        ? p.stats.ok
          ? `<div class="stat-list">${Object.entries(p.stats.data)
              .map(([k, v]) => `<span>${esc(statLabels[k] || k)} <b>${num(v)}</b></span>`)
              .join("")}</div>`
          : `<span class="inline-error">${esc(p.stats.error)}</span>`
        : '<span class="muted">未提供统计</span>';
      const issues = p.issues.ok
        ? `<span class="${p.issues.data.total ? "count-alert" : ""}">${num(p.issues.data.total)}</span>${
            p.issues.data.last24h ? `<div class="muted">24h +${num(p.issues.data.last24h)}</div>` : ""
          }`
        : '<span class="inline-error">读取失败</span>';
      return `<tr data-projection="${esc(
        p.name,
      )}"><td><div class="table-name"><span class="table-title">${esc(
        title(p.name),
      )}</span><code>${esc(
        p.name,
      )}${
        p.table && p.table !== p.name ? ` → ${esc(p.table)}` : ""
      }</code></div></td><td>${stats}</td><td class="number">${issues}</td><td>${runCell(p.lastRebuild)}</td><td>${runCell(p.lastBackfill)}</td><td><span class="row-arrow">${icon("right")}</span></td></tr>`;
    })
    .join("");
  const orphan = s.orphanIssues.length
    ? `<div class="table-note"><h3>未注册投影的缺口</h3><p>${s.orphanIssues
        .map((i) => `<code>${esc(i.projection)}</code> ${esc(reason(i.reason))}：${num(i.count)}`)
        .join("<br>")}</p></div>`
    : "";
  $("#view").innerHTML = `${migrationNotice(s)}
  <section class="panel"><div class="panel-heading"><h2>投影</h2><span class="muted">点开查看缺口明细与运行记录</span></div><div class="table-wrap"><table class="overview-table"><thead><tr><th>投影</th><th>业务表</th><th class="number">缺口记录</th><th>最近重建</th><th>最近 backfill</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>${
    orphan ? `<div class="panel-pad">${orphan}</div>` : ""
  }</section>
  <div class="overview-stack">${streamsPanel(s)}${workerPanel(s)}${runsPanel(s.runs, "最近运行", 8)}</div>`;
}
function streamsPanel(s) {
  if (!s.streams.ok)
    return `<section class="panel"><div class="panel-heading"><h2>实时同步（Redis Stream）</h2></div><div class="panel-empty"><div class="empty-icon">${icon(
      "plug",
    )}</div><strong>未连接 Redis</strong>${esc(s.streams.error)}</div></section>`;
  if (!s.streams.data.length)
    return `<section class="panel"><div class="panel-heading"><h2>实时同步（Redis Stream）</h2></div><div class="panel-empty"><strong>没有找到 hub 事件流</strong>Redis 里没有 <code>hub:*:evt:msg:*</code></div></section>`;
  const rows = s.streams.data
    .map((st) => {
      const g = st.groups.find((x) => x.name === "hub_events") || st.groups[0];
      return `<tr><td class="mono">${esc(
        st.shard,
      )}</td><td class="mono">${esc(
        st.host,
      )}</td><td class="number">${num(
        st.length,
      )}</td><td class="number">${
        g ? num(g.pending) : "—"
      }</td><td class="number">${
        g ? num(g.lag) : "—"
      }</td><td class="number">${num(
        st.lastEventId,
      )}</td><td class="number ${st.dlq ? "count-alert" : ""}">${num(st.dlq)}</td></tr>`;
    })
    .join("");
  return `<section class="panel"><div class="panel-heading"><h2>实时同步（Redis Stream）</h2><span class="muted">consumer group hub_events；DLQ 不自动裁剪，需要人工查看</span></div><div class="table-wrap"><table><thead><tr><th>shard</th><th>hub</th><th class="number">stream 长度</th><th class="number">待确认</th><th class="number">未读取</th><th class="number">最后 event id</th><th class="number">DLQ</th></tr></thead><tbody>${rows}</tbody></table></div></section>`;
}
function workerPanel(s) {
  const m = s.migrations.ok ? s.migrations.data : null;
  const q = s.queue.ok ? s.queue.data : null;
  const counts = q
    ? Object.entries(q.counts)
        .map(
          ([k, v]) =>
            `<span>${esc(
              {
                waiting: "等待",
                active: "执行中",
                delayed: "延迟",
                failed: "失败",
                completed: "已完成(保留)",
                paused: "暂停",
              }[k] || k,
            )} <b>${num(v)}</b></span>`,
        )
        .join("")
    : "";
  return `<section class="panel"><div class="panel-heading"><h2>数据库与 backfill 队列</h2></div><div class="panel-pad"><dl class="kv">
  <dt>迁移</dt><dd>${
    m
      ? `${num(m.executed.length)} 个已执行${
          m.pending.length ? `，<span class="count-alert">待执行 ${esc(m.pending.join(", "))}</span>` : "，没有待执行"
        }${
          m.executed.length
            ? `<div class="muted">最新 <code>${esc(m.executed[m.executed.length - 1].name)}</code></div>`
            : ""
        }`
      : `<span class="inline-error">${esc(sectionError(s.migrations))}</span>`
  }</dd>
  <dt>backfill worker</dt><dd>${
    q
      ? q.workers
        ? badge(`在线 ${q.workers}`, "green")
        : `${badge(
            "不在线",
            "amber",
          )}<div class="muted">在服务器执行 <code>sudo docker compose --profile worker up -d</code> 后才能从本页发起 backfill</div>`
      : `<span class="inline-error">${esc(sectionError(s.queue))}</span>`
  }</dd>
  ${q ? `<dt>队列</dt><dd><div class="stat-list">${counts}</div></dd>` : ""}
  <dt>FID 上限参考</dt><dd>${s.maxFid.ok ? num(s.maxFid.data) : "—"} <span class="muted">（onchain_events 里最大的 fid）</span></dd>
 </dl></div></section>`;
}
function runsPanel(runs, heading, limit) {
  if (!runs.ok)
    return `<section class="panel"><div class="panel-heading"><h2>${esc(
      heading,
    )}</h2></div><div class="panel-empty"><strong>无法读取运行记录</strong>${esc(runs.error)}</div></section>`;
  const list = runs.data.slice(0, limit);
  if (!list.length)
    return `<section class="panel"><div class="panel-heading"><h2>${esc(
      heading,
    )}</h2></div><div class="panel-empty"><div class="empty-icon">${icon(
      "clock",
    )}</div><strong>还没有运行记录</strong>重建和 backfill 从迁移 007 起开始记录，之前的只在日志里。</div></section>`;
  const rows = list
    .map(
      (r) =>
        `<tr data-run="${esc(r.id)}"><td>${esc(kindLabels[r.kind] || r.kind)}</td><td>${runStatus(
          r,
        )}</td><td><div class="run-cell">${runProgress(r)}</div></td><td class="reason-cell">${esc(
          scopeText(r),
        )}</td><td>${formatTime(r.startedAt)}<div class="muted">${esc(
          duration(r.startedAt, r.finishedAt || (r.status === "running" ? null : r.updatedAt)),
        )}</div></td><td>${esc(
          triggerLabels[r.trigger] || r.trigger,
        )}</td><td><button class="link-button" data-run="${esc(r.id)}">详情${icon("right")}</button></td></tr>`,
    )
    .join("");
  return `<section class="panel"><div class="panel-heading"><h2>${esc(heading)}</h2><span class="muted">${num(
    runs.data.length,
  )} 条</span></div><div class="table-wrap"><table class="records-table"><thead><tr><th>类型</th><th>状态</th><th>进度</th><th>范围</th><th>开始</th><th>发起</th><th></th></tr></thead><tbody>${rows}</tbody></table></div></section>`;
}

// ---------- projection ----------
function projectionData() {
  return state.status.projections.find((p) => p.name === state.projection);
}
function renderProjection() {
  const p = projectionData();
  const s = state.status;
  const canAct = s.migrations.ok && !s.migrations.data.pending.length;
  heading(
    "投影",
    p.name,
    `${title(p.name)} · 业务表 ${p.table || "—"} · 回补消息族 ${p.reconcileTypes
      .map((t) => messageTypes[t] || t)
      .join(", ")}`,
    `<button class="button small" data-action="rebuild" ${canAct ? "" : "disabled"}>${icon(
      "database",
    )}<span>从数据库重建</span></button><button class="button small primary" data-action="backfill" ${
      canAct ? "" : "disabled"
    }>${icon("download")}<span>只补投影 backfill</span></button>`,
  );
  const stats = p.stats?.ok
    ? Object.entries(p.stats.data)
        .slice(0, 2)
        .map(([k, v]) => metric(statLabels[k] || k, num(v), "database", p.table ? `业务表 ${p.table}` : ""))
    : [
        metric(
          "业务表统计",
          p.stats ? "读取失败" : "—",
          "database",
          p.stats ? sectionError(p.stats) : "投影没有提供 stats",
        ),
      ];
  const issues = p.issues.ok ? p.issues.data : null;
  $("#metrics").innerHTML = [
    ...stats,
    metric(
      "数据缺口记录",
      issues ? num(issues.total) : "读取失败",
      "alert",
      issues ? `近 24 小时新增 ${num(issues.last24h)}，${issues.byReason.length} 种原因` : sectionError(p.issues),
      issues?.total ? "amber" : "",
    ),
    metric(
      "最近重建",
      p.lastRebuild ? { running: "运行中", succeeded: "成功", failed: "失败" }[p.lastRebuild.status] : "从未",
      "clock",
      p.lastRebuild ? formatTime(p.lastRebuild.startedAt) : "用库里已有原始数据重算",
    ),
  ].join("");
  const max = issues ? Math.max(1, ...issues.byReason.map((r) => r.count)) : 1;
  const reasons = issues
    ? issues.byReason.length
      ? `<div class="field-bars">${issues.byReason
          .map(
            (r) =>
              `<div class="bar-row clickable ${state.reason === r.reason ? "selected" : ""}" data-reason="${esc(
                r.reason,
              )}" title="${esc(r.reason)}"><div class="bar-label"><code>${esc(
                reasonShort[r.reason] || short(r.reason, 22),
              )}</code></div><div class="bar-track"><div class="bar-fill" style="width:${
                (r.count / max) * 100
              }%"></div></div><span class="bar-count">${num(r.count)}</span></div>`,
          )
          .join("")}</div><div class="status-explanation">点原因筛选下方明细。最后一条记录：${formatTime(
          issues.byReason.reduce((a, r) => (a && a > r.lastAt ? a : r.lastAt), null),
          true,
        )}</div>`
      : `<div class="panel-empty"><div class="empty-icon">${icon(
          "check",
        )}</div><strong>没有数据缺口</strong>校验失败的消息会记在这里并跳过，不阻塞事件流。</div>`
    : `<div class="panel-empty"><strong>读取失败</strong>${esc(sectionError(p.issues))}</div>`;
  const runs = s.runs.ok
    ? { ok: true, data: s.runs.data.filter((r) => !r.projections.length || r.projections.includes(p.name)) }
    : s.runs;
  $("#view").innerHTML = `${migrationNotice(s)}
  <div class="compact-columns"><section class="panel"><div class="panel-heading"><h2>缺口原因分布</h2><span class="muted">shuttle_projection_issues</span></div>${reasons}</section>
  <section class="panel"><div class="panel-heading"><h2>处理方式</h2></div><div class="panel-pad"><dl class="kv"><dt>实时</dt><dd>shard 消费 hub 事件，在同一事务里写原始消息和业务表。</dd><dt>从数据库重建</dt><dd>用库里已有的原始消息重算业务表；只写业务表，可重复执行。投影上线前已入库的数据只能靠它补上。</dd><dt>只补投影 backfill</dt><dd>向节点拉取该投影需要的消息族，补齐库里缺的部分；需要服务器上的 worker 在线。</dd><dt>最近 backfill</dt><dd>${runCell(
    p.lastBackfill,
  )}</dd></dl></div></section></div>
  <section class="panel" id="records-panel" style="margin-top:18px"><div class="panel-heading record-top"><div class="record-heading"><h2>缺口明细</h2><span class="record-count" id="record-count">读取中…</span></div></div>
   <div class="record-tools"><label class="search">${icon(
     "search",
   )}<input id="fid-filter" inputmode="numeric" aria-label="按 FID 筛选" placeholder="FID" value="${esc(
    state.fid,
  )}"></label><select id="reason-filter" aria-label="按原因筛选"><option value="">全部原因</option>${(
    issues?.byReason || []
  )
    .map(
      (r) =>
        `<option value="${esc(r.reason)}" ${state.reason === r.reason ? "selected" : ""}>${esc(
          short(r.reason, 30),
        )}（${num(r.count)}）</option>`,
    )
    .join("")}</select><button class="button export-records" id="export-records">${icon("download")}导出 CSV</button></div>
   <div id="record-body"><div class="loading-line"><span class="spinner"></span>读取缺口明细…</div></div></section>
  <div class="overview-stack">${runsPanel(runs, "运行记录", 20)}</div>`;
  loadIssues();
}
function issueQuery(extra = {}) {
  const q = new URLSearchParams({ projection: state.projection, offset: state.offset, limit: state.limit, ...extra });
  if (state.reason) q.set("reason", state.reason);
  if (state.fid) q.set("fid", state.fid);
  return q;
}
async function loadIssues() {
  const seq = ++state.issueSeq;
  try {
    const page = await api(`/api/issues?${issueQuery()}`);
    if (seq !== state.issueSeq || !$("#record-body")) return;
    state.issues = page;
    renderIssues();
  } catch (e) {
    if (seq !== state.issueSeq || !$("#record-body")) return;
    $("#record-count").textContent = "读取失败";
    $("#record-body").innerHTML = `<div class="panel-empty"><strong>${esc(
      e.message,
    )}</strong><button class="link-button" id="retry-records">重新读取</button></div>`;
  }
}
function renderIssues() {
  const p = state.issues;
  $("#record-count").textContent = `${num(p.total)} 条`;
  if (!p.items.length) {
    $("#record-body").innerHTML = `<div class="panel-empty"><div class="empty-icon">${icon("search")}</div><strong>${
      state.reason || state.fid ? "没有匹配的记录" : "没有缺口记录"
    }</strong>${state.reason || state.fid ? "调整 FID 或原因筛选后再试。" : ""}</div>`;
    return;
  }
  $(
    "#record-body",
  ).innerHTML = `<div class="table-wrap"><table class="records-table"><thead><tr><th>FID</th><th>消息 hash</th><th>原因</th><th>类型 / 操作</th><th>记录时间</th><th></th></tr></thead><tbody>${p.items
    .map(
      (r, i) =>
        `<tr><td class="mono fid-cell">${esc(r.fid ?? "—")}</td><td class="mono key-cell" title="${esc(r.hash)}">${esc(
          short(r.hash, 6),
        )}</td><td class="reason-cell">${esc(reason(r.reason))}</td><td class="mono">${esc(
          messageTypes[r.payload?.type] || r.payload?.type || "—",
        )}${r.payload?.operation ? ` · ${esc(r.payload.operation)}` : ""}</td><td>${formatTime(
          r.createdAt,
          true,
        )}</td><td><button class="link-button" data-issue="${i}">详情${icon("right")}</button></td></tr>`,
    )
    .join("")}</tbody></table></div>
  <div class="pagination"><span>显示 ${num(p.offset + 1)}–${num(
    Math.min(p.offset + p.items.length, p.total),
  )} 条，共 ${num(p.total)} 条</span><div class="pager-controls"><select id="page-size" aria-label="每页数量">${[
    10, 20, 50, 100,
  ]
    .map((n) => `<option value="${n}" ${n === state.limit ? "selected" : ""}>${n} 条 / 页</option>`)
    .join("")}</select><span>第 ${Math.floor(p.offset / state.limit) + 1} / ${Math.max(
    1,
    Math.ceil(p.total / state.limit),
  )} 页</span><button class="icon-button" id="page-prev" aria-label="上一页" ${p.offset === 0 ? "disabled" : ""}>${icon(
    "left",
  )}</button><button class="icon-button" id="page-next" aria-label="下一页" ${
    p.offset + p.limit >= p.total ? "disabled" : ""
  }>${icon("right")}</button></div></div>`;
}

// ---------- runs ----------
function renderRuns() {
  const s = state.status;
  heading("操作", "运行记录", "从数据库重建与 backfill 的历史、进度和结果（迁移 007 起记录）");
  const runs = s.runs.ok ? s.runs.data : [];
  const count = (k) => runs.filter((r) => r.kind === k).length;
  $("#metrics").innerHTML = [
    metric("运行中", num(runs.filter((r) => r.status === "running" && !r.stale).length), "play"),
    metric(
      "可能已中断",
      num(runs.filter((r) => r.stale).length),
      "alert",
      "超过 5 分钟没有心跳",
      runs.some((r) => r.stale) ? "amber" : "",
    ),
    metric("重建", num(count("rebuild")), "database", "最近 20 条内"),
    metric("backfill", num(count("backfill")), "download", "最近 20 条内"),
  ].join("");
  $("#view").innerHTML = `${migrationNotice(
    s,
  )}<div id="all-runs"><div class="loading-line"><span class="spinner"></span>读取运行记录…</div></div>`;
  api("/api/runs?limit=200")
    .then((data) => {
      if ($("#all-runs")) {
        $("#all-runs").innerHTML = runsPanel({ ok: true, data }, "全部运行记录", 200);
        icons();
        state.allRuns = data;
      }
    })
    .catch((e) => {
      if ($("#all-runs")) $("#all-runs").innerHTML = runsPanel({ ok: false, error: e.message }, "全部运行记录", 0);
    });
}

// ---------- drawer ----------
function openDrawer(html) {
  previousFocus = document.activeElement;
  $("#drawer").innerHTML = html;
  icons($("#drawer"));
  $("#drawer-overlay").classList.remove("hidden");
  $("#drawer").classList.remove("hidden");
  document.body.style.overflow = "hidden";
  $("#drawer").focus();
}
function closeDrawer() {
  $("#drawer-overlay").classList.add("hidden");
  $("#drawer").classList.add("hidden");
  document.body.style.overflow = "";
  if (previousFocus?.isConnected) previousFocus.focus();
  previousFocus = null;
}
function openIssue(index) {
  const r = state.issues?.items[index];
  if (!r) return;
  openDrawer(`<div class="drawer-header"><h2 id="drawer-title">缺口详情</h2><button class="icon-button" id="close-drawer" aria-label="关闭">${icon(
    "close",
  )}</button></div><div class="drawer-body"><div class="drawer-identity">${esc(
    r.projection,
  )} · FID</div><div class="drawer-fid">${esc(r.fid ?? "未知")}</div>
  <section class="detail-section"><h3>原因</h3><p>${esc(reason(r.reason))}</p>${
    reasonLabels[r.reason] ? `<p class="muted"><code>${esc(r.reason)}</code></p>` : ""
  }</section>
  <section class="detail-section"><h3>消息</h3><dl class="kv"><dt>hash</dt><dd class="mono">${esc(
    r.hash,
  )}</dd><dt>类型</dt><dd>${esc(messageTypes[r.payload?.type] || r.payload?.type || "—")}</dd><dt>操作</dt><dd>${esc(
    r.payload?.operation || "—",
  )}</dd><dt>记录时间</dt><dd>${formatTime(r.createdAt, true)}</dd></dl></section>
  <section class="detail-section"><h3>payload</h3><pre class="payload">${esc(JSON.stringify(r.payload, null, 2))}</pre></section>
  <section class="detail-section"><h3>说明</h3><p class="muted">校验失败的消息不写入 messages（避免污染该 FID 之后的重算），事务照常提交。数据修正后，它会在下次实时事件或只补投影 backfill 时重新进入。</p></section></div>`);
}
function findRun(id) {
  const lists = [state.status?.runs?.ok ? state.status.runs.data : [], state.allRuns || []];
  for (const l of lists) {
    const r = l.find((x) => x.id === id);
    if (r) return r;
  }
  return null;
}
function openRun(id) {
  const r = findRun(id);
  if (!r) return;
  const details =
    Array.isArray(r.details) && r.details.length
      ? `<section class="detail-section"><h3>失败的 FID（最多 50 条）</h3>${r.details
          .map(
            (d) =>
              `<div class="repair-error-row"><span class="mono">${esc(d.fid)}</span><div><code>${esc(
                d.projection,
              )}</code> ${esc(d.error)}</div></div>`,
          )
          .join("")}</section>`
      : "";
  openDrawer(`<div class="drawer-header"><h2 id="drawer-title">运行详情</h2><button class="icon-button" id="close-drawer" aria-label="关闭">${icon(
    "close",
  )}</button></div><div class="drawer-body"><div class="drawer-identity">${esc(
    kindLabels[r.kind] || r.kind,
  )}</div><div class="drawer-fid">${runStatus(r)}</div>
  <section class="detail-section"><dl class="kv"><dt>run id</dt><dd class="mono">${esc(
    r.id,
  )}</dd><dt>范围</dt><dd>${esc(scopeText(r))}</dd><dt>发起</dt><dd>${esc(
    triggerLabels[r.trigger] || r.trigger,
  )}</dd><dt>进度</dt><dd><div class="run-cell">${runProgress(r)}</div></dd><dt>开始</dt><dd>${formatTime(
    r.startedAt,
    true,
  )}</dd><dt>最后心跳</dt><dd>${formatTime(r.updatedAt, true)}</dd><dt>结束</dt><dd>${
    r.finishedAt ? `${formatTime(r.finishedAt, true)}（用时 ${esc(duration(r.startedAt, r.finishedAt))}）` : "—"
  }</dd>${r.error ? `<dt>错误</dt><dd class="count-alert">${esc(r.error)}</dd>` : ""}</dl></section>
  ${
    r.stale
      ? `<section class="detail-section"><p class="form-hint warn">超过 5 分钟没有更新心跳：发起它的进程可能已经退出。重建可以直接重新发起，结果不变。</p></section>`
      : ""
  }
  ${
    r.kind === "backfill" && r.status === "running" && !r.stale
      ? `<section class="detail-section"><p class="muted">进度由服务器上的 worker 每完成一批（10 个 FID）更新一次。</p></section>`
      : ""
  }
  ${details}</div>`);
}

// ---------- actions ----------
function openAction(kind) {
  const s = state.status;
  const p = projectionData();
  const names = s.projections.map((x) => x.name);
  const worker = s.queue.ok ? s.queue.data.workers : 0;
  const maxFid = s.maxFid.ok ? s.maxFid.data : "";
  const isRebuild = kind === "rebuild";
  $("#action-title").textContent = isRebuild ? "从数据库重建" : "只补投影 backfill";
  $("#action-content").innerHTML = `<form id="action-form" class="form-grid">
  ${
    isRebuild
      ? `<div class="field"><div class="form-hint">投影</div><div class="checks">${names
          .map(
            (n) =>
              `<label><input type="checkbox" name="projection" value="${esc(n)}" ${
                n === p?.name ? "checked" : ""
              }>${esc(n)}</label>`,
          )
          .join("")}</div></div>`
      : `<div class="form-hint">会补齐所有已注册投影（${esc(
          names.join("、"),
        )}）需要的消息族；不需要的消息不入库。不提供全量原始 backfill。</div>`
  }
  <label class="field">FID（逗号或空格分隔，留空表示全部）<textarea name="fids" placeholder="例如 3, 2, 1"></textarea></label>
  ${
    isRebuild
      ? ""
      : `<label class="field">不填 FID 时：补到 FID 上限<input type="number" name="maxFid" min="1" step="1" value="${esc(
          maxFid,
        )}"></label>`
  }
  <div class="form-hint">${
    isRebuild
      ? "在本机进程里执行，经网络写服务器数据库；每个 FID 一个事务，持有与实时处理相同的锁，可在 shard 运行时执行，可重复执行。全量重建较慢，建议在服务器用 <code>docker compose run … yarn start rebuild</code>（同样会出现在运行记录里）。关闭本页面服务会中断重建。"
      : `任务排进 Redis 队列，由服务器上的 worker 执行（当前 ${
          worker ? `在线 ${worker} 个` : "<b>不在线</b>"
        }）。每批 10 个 FID，中断后可重跑，已存在的消息会跳过。`
  }</div>
  ${
    !isRebuild && !worker
      ? '<div class="form-hint warn">worker 不在线，提交会被拒绝。先在服务器执行 <code>sudo docker compose --profile worker up -d</code>。</div>'
      : ""
  }
  <div class="repair-footer"><span id="action-error" class="inline-error"></span><button class="button primary" type="submit" id="action-submit">${icon(
    isRebuild ? "database" : "download",
  )}<span>开始</span></button></div>
 </form>`;
  $("#action-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = new FormData(e.target);
    const fids = String(form.get("fids") || "")
      .split(/[\s,，]+/)
      .filter(Boolean);
    const body = { fids };
    if (isRebuild) {
      body.projections = form.getAll("projection");
      if (!body.projections.length) {
        $("#action-error").textContent = "至少选一个投影";
        return;
      }
    } else if (!fids.length) body.maxFid = Number(form.get("maxFid"));
    const scope = fids.length ? `${fids.length} 个 FID` : isRebuild ? "全部 FID" : `FID 1 到 ${num(body.maxFid)}`;
    if (
      !confirm(
        `${isRebuild ? `从数据库重建 ${body.projections.join("、")}` : "只补投影 backfill"}：${scope}。确定开始？`,
      )
    )
      return;
    const button = $("#action-submit");
    button.disabled = true;
    $("#action-error").textContent = "";
    try {
      const result = await api(`/api/${kind}`, { method: "POST", body: JSON.stringify(body) });
      $("#action-dialog").close();
      toast(`已开始 ${isRebuild ? "重建" : "backfill"}${result.jobs ? `，排队 ${num(result.jobs)} 批` : ""}`);
      await loadStatus();
    } catch (err) {
      $("#action-error").textContent = err.message;
      button.disabled = false;
    }
  });
  icons($("#action-content"));
  $("#action-dialog").showModal();
}

// ---------- navigation & events ----------
function go(view, projection = null) {
  state.view = view;
  state.projection = projection;
  if (view === "projection") {
    state.reason = "";
    state.fid = "";
    state.offset = 0;
  }
  const q =
    view === "projection" ? `?projection=${encodeURIComponent(projection)}` : view === "runs" ? "?view=runs" : "/";
  history.pushState(null, "", q === "/" ? "/" : q);
  closeMenu();
  render();
  $("#main").focus({ preventScroll: true });
  window.scrollTo(0, 0);
}
function fromUrl() {
  const q = new URLSearchParams(location.search);
  if (q.get("projection")) {
    state.view = "projection";
    state.projection = q.get("projection");
  } else if (q.get("view") === "runs") state.view = "runs";
  else state.view = "overview";
}
function closeMenu() {
  $("#sidebar").classList.remove("open");
  $(".menu-scrim")?.remove();
  $("#menu-toggle").setAttribute("aria-expanded", "false");
}
document.addEventListener("click", (e) => {
  const t = e.target.closest("button, tr[data-projection], tr[data-run], .bar-row[data-reason], .menu-scrim");
  if (!t) return;
  if (t.classList.contains("menu-scrim")) return closeMenu();
  if (t.id === "refresh") return loadStatus(true);
  if (t.id === "overview-nav") return go("overview");
  if (t.id === "runs-nav") return go("runs");
  if (t.id === "close-drawer") return closeDrawer();
  if (t.id === "close-action") return $("#action-dialog").close();
  if (t.id === "retry-records") return loadIssues();
  if (t.id === "page-prev") {
    state.offset = Math.max(0, state.offset - state.limit);
    return loadIssues();
  }
  if (t.id === "page-next") {
    state.offset += state.limit;
    return loadIssues();
  }
  if (t.id === "export-records") {
    const q = issueQuery();
    q.delete("offset");
    q.delete("limit");
    location.href = `/api/issues/export?${q}`;
    return;
  }
  if (t.id === "menu-toggle") {
    const open = $("#sidebar").classList.toggle("open");
    t.setAttribute("aria-expanded", String(open));
    if (open) {
      const el = document.createElement("div");
      el.className = "menu-scrim visible";
      document.body.append(el);
    } else closeMenu();
    return;
  }
  if (t.dataset.action) return openAction(t.dataset.action);
  if (t.dataset.issue !== undefined) return openIssue(Number(t.dataset.issue));
  if (t.dataset.run) return openRun(t.dataset.run);
  if (t.dataset.reason !== undefined) {
    state.reason = state.reason === t.dataset.reason ? "" : t.dataset.reason;
    state.offset = 0;
    return renderProjectionKeepScroll();
  }
  if (t.dataset.projection) return go("projection", t.dataset.projection);
});
function renderProjectionKeepScroll() {
  const y = window.scrollY;
  renderProjection();
  icons();
  window.scrollTo(0, y);
}
document.addEventListener("change", (e) => {
  if (e.target.id === "reason-filter") {
    state.reason = e.target.value;
    state.offset = 0;
    renderProjectionKeepScroll();
  }
  if (e.target.id === "page-size") {
    state.limit = Number(e.target.value);
    state.offset = 0;
    loadIssues();
  }
});
document.addEventListener("input", (e) => {
  if (e.target.id !== "fid-filter") return;
  clearTimeout(fidTimer);
  fidTimer = setTimeout(() => {
    const v = e.target.value.trim();
    if (v && !/^\d+$/.test(v)) return;
    state.fid = v;
    state.offset = 0;
    loadIssues();
  }, 300);
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && !$("#drawer").classList.contains("hidden")) closeDrawer();
});
$("#drawer-overlay").addEventListener("click", closeDrawer);
window.addEventListener("popstate", () => {
  fromUrl();
  render();
});
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) loadStatus();
});
icons();
fromUrl();
loadStatus();
