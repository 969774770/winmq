/* winMQ 管理页逻辑（原生 JS，无外部依赖） */
"use strict";

var token = sessionStorage.getItem("winmq_token") || "";
var cfgInfo = null;
var currentQueue = "";

// ---------- 基础 ----------

function $(id) { return document.getElementById(id); }

function showToast(msg) {
  var t = $("toast");
  t.textContent = msg;
  t.classList.remove("hidden");
  clearTimeout(t._timer);
  t._timer = setTimeout(function () { t.classList.add("hidden"); }, 2500);
}

function adminFetch(method, path, body) {
  return fetch(path, {
    method: method,
    headers: { "Content-Type": "application/json", "X-Admin-Token": token },
    body: body ? JSON.stringify(body) : undefined
  }).then(function (r) {
    if (r.status === 401) { showLogin(); throw new Error("请先登录"); }
    return r.json();
  });
}

function fmt(n) {
  if (n === null || n === undefined) return "-";
  return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function fmtBytes(n) {
  if (n === null || n === undefined) return "-";
  if (n >= 1073741824) return (n / 1073741824).toFixed(2) + " GB";
  if (n >= 1048576) return (n / 1048576).toFixed(2) + " MB";
  if (n >= 1024) return (n / 1024).toFixed(2) + " KB";
  return n + " B";
}

// ---------- 登录 ----------

function showLogin() {
  $("loginLayer").classList.remove("hidden");
  $("mainLayer").classList.add("hidden");
}

function showMain() {
  $("loginLayer").classList.add("hidden");
  $("mainLayer").classList.remove("hidden");
  loadConfig();
  refreshOverview();
  refreshQueues();
  refreshDaily();
  refreshKeys();
  setInterval(refreshOverview, 2000);
  setInterval(refreshQueues, 3000);
  setInterval(refreshDaily, 5000);
}

$("loginBtn").onclick = function () {
  var secret = $("loginSecret").value;
  if (!secret) return;
  fetch("/admin/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ secret: secret })
  }).then(function (r) { return r.json(); }).then(function (d) {
    if (d.token) {
      token = d.token;
      sessionStorage.setItem("winmq_token", token);
      $("loginErr").textContent = "";
      showMain();
    } else {
      $("loginErr").textContent = d.error || "登录失败";
    }
  });
};
$("loginSecret").addEventListener("keydown", function (e) {
  if (e.key === "Enter") $("loginBtn").click();
});

// ---------- 页签 ----------

var tabs = document.querySelectorAll(".tab");
for (var i = 0; i < tabs.length; i++) {
  tabs[i].onclick = function () {
    document.querySelectorAll(".tab").forEach(function (t) { t.classList.remove("active"); });
    this.classList.add("active");
    ["console", "api"].forEach(function (name) {
      $("tab-" + name).classList.toggle("hidden", name !== this);
    }.bind(this.dataset.tab));
  };
}

// ---------- 总览 ----------

function loadConfig() {
  adminFetch("GET", "/admin/api/config").then(function (d) {
    cfgInfo = d;
    $("connInfo").textContent = d.httpAddr + " · AK: " + d.accessKeyId;
    genApiExample(d);
  });
}

function refreshOverview() {
  adminFetch("GET", "/admin/api/overview").then(function (d) {
    $("ovQueues").textContent = fmt(d.queueCount);
    $("ovActive").textContent = fmt(d.activeMessages);
    $("ovEnqRate").textContent = fmt(d.enqRate);
    $("ovDeqRate").textContent = fmt(d.deqRate);
    $("ovEnqTotal").textContent = fmt(d.enqTotal);
    $("ovDeqTotal").textContent = fmt(d.deqTotal);
    $("ovDisk").textContent = fmtBytes(d.diskUsage || 0);
    $("ovReq").textContent = fmt(d.reqTotal);
    $("ovTraffic").textContent = fmtBytes(d.bytesIn || 0) + " / " + fmtBytes(d.bytesOut || 0);
    $("pausedBadge").classList.toggle("hidden", !d.paused);
  }).catch(function () {});
}

// ---------- 今日统计 / 每日统计 ----------

function refreshDaily() {
  adminFetch("GET", "/admin/api/daily?days=7").then(function (d) {
    var t = d.today || {};
    $("tdReq").textContent = fmt(t.req);
    $("tdEnq").textContent = fmt(t.enq);
    $("tdDeq").textContent = fmt(t.deq);
    $("tdTraffic").textContent = fmtBytes((t.bytesIn || 0) + (t.bytesOut || 0));

    var tb = $("dailyTable").querySelector("tbody");
    tb.innerHTML = "";
    (d.days || []).forEach(function (ds) {
      var tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + ds.date + "</td>" +
        "<td>" + fmt(ds.req) + "</td>" +
        "<td>" + fmt(ds.enq) + "</td>" +
        "<td>" + fmt(ds.deq) + "</td>" +
        "<td>" + fmtBytes(ds.bytesIn) + "</td>" +
        "<td>" + fmtBytes(ds.bytesOut) + "</td>";
      tb.appendChild(tr);
    });
    if (!d.days || !d.days.length) {
      tb.innerHTML = '<tr><td colspan="6" style="color:#6b7280;text-align:center;padding:20px">暂无历史数据</td></tr>';
    }
  }).catch(function () {});
}

// ---------- API Key 管理 ----------

function refreshKeys() {
  adminFetch("GET", "/admin/api/keys").then(function (d) {
    var tb = $("keyTable").querySelector("tbody");
    tb.innerHTML = "";
    var now = Date.now();
    (d.keys || []).forEach(function (k) {
      var status = k.status || "有效";
      var expText = k.expiresAt ? new Date(k.expiresAt).toLocaleString() : "永不过期";
      var tr = document.createElement("tr");
      tr.innerHTML =
        '<td class="key-val"></td>' +
        "<td>" + (k.name || "-") + "</td>" +
        "<td>" + new Date(k.createdAt).toLocaleString() + "</td>" +
        "<td>" + expText + "</td>" +
        '<td class="' + (status === "有效" && !(k.expiresAt && now >= k.expiresAt) ? "k-ok" : "k-bad") + '">' + status + "</td>" +
        '<td><button class="btn sm danger" data-act="del">删除</button></td>';
      tr.querySelector(".key-val").textContent = k.key;
      tr.querySelector('[data-act="del"]').onclick = function () {
        if (!confirm("确定删除该 Key？使用它的客户端将立即失效。")) return;
        adminFetch("DELETE", "/admin/api/keys/" + encodeURIComponent(k.key)).then(function () {
          showToast("已删除");
          refreshKeys();
        });
      };
      tb.appendChild(tr);
    });
    if (!d.keys || !d.keys.length) {
      tb.innerHTML = '<tr><td colspan="6" style="color:#6b7280;text-align:center;padding:20px">暂无 Key，点击"创建 Key"</td></tr>';
    }
  }).catch(function () {});
}

$("btnCreateKey").onclick = function () {
  adminFetch("POST", "/admin/api/keys", {
    name: $("keyName").value.trim(),
    expiresInDays: +$("keyExp").value
  }).then(function () {
    showToast("Key 已创建");
    $("keyName").value = "";
    refreshKeys();
  });
};

// ---------- 队列列表 ----------

function refreshQueues() {
  adminFetch("GET", "/admin/api/overview").then(function (d) {
    var names = d.queues || [];
    Promise.all(names.map(function (n) {
      return fetch("/admin/api/queues/" + encodeURIComponent(n) + "/messages?limit=1", {
        headers: { "X-Admin-Token": token }
      }).then(function (r) { return r.json(); }).catch(function () { return null; });
    })).then(renderQueueTable);
  });
}

function renderQueueTable(items) {
  var tb = $("queueTable").querySelector("tbody");
  tb.innerHTML = "";
  (items || []).forEach(function (item) {
    if (!item || !item.queue) return;
    var q = item.queue;
    var tr = document.createElement("tr");
    tr.innerHTML =
      '<td><a class="q-link"><b>' + q.queueName + "</b></a></td>" +
      '<td class="num">' + fmt(q.activeMessages) + "</td>" +
      '<td class="num">' + fmt(q.enqTotal) + "</td>" +
      '<td class="num">' + fmt(q.deqTotal) + "</td>" +
      "<td>" +
        '<button class="btn sm" data-act="view">消息</button> ' +
        '<button class="btn sm" data-act="compact">回收</button> ' +
        '<button class="btn sm danger" data-act="del">删除</button>' +
      "</td>";
    tr.querySelector(".q-link").onclick = function () { openMsgs(q.queueName); };
    tr.querySelector('[data-act="view"]').onclick = function () { openMsgs(q.queueName); };
    tr.querySelector('[data-act="compact"]').onclick = function () {
      if (!confirm("回收队列 " + q.queueName + " 的磁盘空间？\n\n"
        + "删除消息后磁盘不会立即释放（存储引擎用墓碑标记），此操作会压实数据文件、\n"
        + "物理回收已删除消息占用的空间。数据量大时需数分钟，在后台执行，期间请避免大量读写。")) return;
      adminFetch("POST", "/admin/api/queues/" + encodeURIComponent(q.queueName) + "/compact")
        .then(function (d) {
          showToast(d.message || "已开始回收");
          refreshOverview();
        }).catch(function (e) { showToast(e.message); });
    };
    tr.querySelector('[data-act="del"]').onclick = function () {
      if (!confirm("确定删除队列 " + q.queueName + "？其所有消息将被清除。")) return;
      adminFetch("DELETE", "/admin/api/queues/" + encodeURIComponent(q.queueName)).then(function () {
        showToast("已删除");
        refreshQueues();
      });
    };
    tb.appendChild(tr);
  });
  if (!items || !items.length) {
    tb.innerHTML = '<tr><td colspan="5" style="color:#6b7280;text-align:center;padding:30px">暂无队列，点击"创建队列"开始</td></tr>';
  }
}

$("btnCreateQueue").onclick = function () {
  $("dlgQueueTitle").textContent = "创建队列";
  $("qName").value = "";
  $("qSave").onclick = saveCreateQueue;
  $("dlgQueue").classList.remove("hidden");
};

function saveCreateQueue() {
  var name = $("qName").value.trim();
  if (!name) { showToast("请输入队列名"); return; }
  adminFetch("POST", "/admin/api/queues", { queueName: name }).then(function () {
    showToast("队列已创建");
    $("dlgQueue").classList.add("hidden");
    refreshQueues();
  }).catch(function (e) { showToast(e.message); });
}

// ---------- 消息弹窗（分页） ----------

var pageOffset = 0; // 当前页起始偏移
var pageTotal = 0;  // 现存消息总数

function openMsgs(name) {
  currentQueue = name;
  pageOffset = 0;
  $("dlgMsgsTitle").textContent = "队列消息 - " + name;
  $("dlgMsgs").classList.remove("hidden");
  loadMsgs();
}

function curPageSize() {
  return +$("pageSize").value || 20;
}

function loadMsgs() {
  var size = curPageSize();
  var list = $("msgList");
  // 深页需从队列头部扫描，可能耗时较久，先给出提示
  if (pageOffset >= 5000) {
    list.innerHTML = '<div style="color:#6b7280;text-align:center;padding:30px">正在扫描第 ' +
      (Math.floor(pageOffset / size) + 1) + ' 页…（数据量大时较慢，请稍候）</div>';
  }
  fetch("/admin/api/queues/" + encodeURIComponent(currentQueue) +
        "/messages?offset=" + pageOffset + "&limit=" + size, {
    headers: { "X-Admin-Token": token }
  }).then(function (r) { return r.json(); }).then(function (d) {
    list.innerHTML = "";
    var msgs = d.messages || [];
    pageTotal = d.total || 0;

    if (!msgs.length) {
      if (pageOffset > 0) {
        // 当前页已空（消息被消费掉），自动回退一页
        pageOffset = Math.max(0, pageOffset - size);
        updatePager();
        return loadMsgs();
      }
      list.innerHTML = '<div style="color:#6b7280;text-align:center;padding:30px">队列为空</div>';
      updatePager();
      return;
    }

    var base = pageOffset;
    msgs.forEach(function (m, i) {
      var div = document.createElement("div");
      div.className = "msg-item";
      div.innerHTML =
        '<div class="mid">#' + (base + i + 1) + " · " + m.messageId + "</div>" +
        '<div class="mbody"></div>';
      div.querySelector(".mbody").textContent = m.body;
      list.appendChild(div);
    });
    updatePager();
  }).catch(function () {});
}

// updatePager 刷新分页控件状态
function updatePager() {
  var size = curPageSize();
  var page = size > 0 ? Math.floor(pageOffset / size) + 1 : 1;
  var pages = size > 0 ? Math.max(1, Math.ceil(pageTotal / size)) : 1;
  $("pageInfo").textContent = "第 " + page + " / " + pages + " 页";
  $("msgTotal").textContent = "共 " + fmt(pageTotal) + " 条（深度分页需扫描，越往后越慢）";
  $("btnFirstPage").disabled = pageOffset <= 0;
  $("btnPrevPage").disabled = pageOffset <= 0;
  $("btnNextPage").disabled = pageOffset + size >= pageTotal;
}

$("btnFirstPage").onclick = function () { pageOffset = 0; loadMsgs(); };
$("btnPrevPage").onclick = function () {
  pageOffset = Math.max(0, pageOffset - curPageSize());
  loadMsgs();
};
$("btnNextPage").onclick = function () {
  if (pageOffset + curPageSize() < pageTotal) {
    pageOffset += curPageSize();
    loadMsgs();
  }
};
$("pageSize").onchange = function () { pageOffset = 0; loadMsgs(); };

$("btnRefreshMsgs").onclick = loadMsgs;
$("btnSendTest").onclick = function () {
  adminFetch("POST", "/admin/api/queues/" + encodeURIComponent(currentQueue) + "/test-messages",
    { count: 1, body: "测试消息 " + new Date().toLocaleTimeString() })
    .then(function () { showToast("已发送"); loadMsgs(); });
};
$("btnBenchThis").onclick = function () {
  $("dlgMsgs").classList.add("hidden");
  $("bQueue").value = currentQueue;
  $("dlgBench").classList.remove("hidden");
};

// ---------- 压测 ----------

$("btnBenchTop").onclick = function () { $("dlgBench").classList.remove("hidden"); };
$("btnBenchRun").onclick = function () {
  $("benchRunning").classList.remove("hidden");
  $("benchOut").classList.add("hidden");
  adminFetch("POST", "/admin/api/bench", {
    queue: $("bQueue").value.trim() || "bench",
    count: +$("bCount").value || 10000,
    concurrency: +$("bConc").value || 16
  }).then(function (d) {
    $("benchRunning").classList.add("hidden");
    var out = $("benchOut");
    var pass = d.sendRate >= 10000 && d.recvRate >= 10000;
    var cls = pass ? "bench-ok" : "bench-bad";
    out.innerHTML =
      "发送: " + fmt(d.sendCount) + " 条 / " + d.sendMs + " ms  =  <b>" + d.sendRate.toFixed(0) + "</b> 条/秒（失败 " + d.sendFailed + "）\n" +
      "接收(即删除): " + fmt(d.recvCount) + " 条 / " + d.recvMs + " ms  =  <b>" + d.recvRate.toFixed(0) + "</b> 条/秒（失败 " + d.recvFailed + "）\n\n" +
      '及格线: 10000 条/秒  →  <span class="' + cls + '">' + (pass ? "PASS 达标" : "未达标，可增大并发数后重试") + "</span>";
    out.classList.remove("hidden");
    refreshQueues();
  }).catch(function (e) {
    $("benchRunning").classList.add("hidden");
    showToast(e.message);
  });
};

// ---------- API 文档示例 ----------

function genApiExample(d) {
  var host = "http://127.0.0.1" + (d.httpAddr && d.httpAddr[0] === ":" ? d.httpAddr : ":" + d.httpAddr);
  $("apiExample").textContent =
    "# 创建队列\n" +
    "curl -X POST " + host + "/api/queues \\\n" +
    '  -H "X-Api-Key: <你的key>" \\\n' +
    '  -d \'{"queueName":"order"}\'\n\n' +
    "# 发送消息\n" +
    "curl -X POST " + host + "/api/queues/order/messages \\\n" +
    '  -H "X-Api-Key: <你的key>" \\\n' +
    '  -d \'{"body":"hello"}\'\n\n' +
    "# 接收消息（接收即删除；wait=5 为长轮询秒数）\n" +
    "curl \"" + host + "/api/queues/order/messages?num=1&wait=5\" \\\n" +
    "  -H \"X-Api-Key: <你的key>\"";
}

// ---------- 弹窗通用关闭 ----------

document.querySelectorAll(".modal").forEach(function (m) {
  m.addEventListener("click", function (e) {
    if (e.target === m || e.target.hasAttribute("data-close")) m.classList.add("hidden");
  });
});

// 启动
if (token) {
  showMain();
} else {
  showLogin();
}
