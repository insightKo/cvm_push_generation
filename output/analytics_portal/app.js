// --- Auth check (portal CVM) ---
// Дашборд отдаётся порталом по /analytics/ за его авторизацией. Пользователь и роли
// приходят из GET /api/v1/auth/me (cookie-сессия портала); локальной авторизации нет.
let PORTAL_USER = null;

// roles включает 'admin' → admin; 'analyst' → analyst; иначе viewer (экспорт скрыт классом role-viewer)
function portalRole(user) {
    const raw = Array.isArray(user?.roles) ? user.roles : (user?.roles || user?.role ? [user.roles || user.role] : []);
    const roles = raw.map(r => String(r && typeof r === 'object' ? (r.name || r.code || '') : r).toLowerCase());
    if (roles.includes('admin')) return 'admin';
    if (roles.includes('analyst')) return 'analyst';
    return 'viewer';
}

function showAuthOverlay() {
    const el = document.getElementById('auth-overlay');
    if (el) el.classList.add('show');
}

// «Выйти»: в embedded-режиме кнопка скрыта, иначе уводим родительское окно на /login портала
function logout() {
    if (document.body.classList.contains('embedded')) return;
    try { window.top.location = '/login'; } catch (e) { window.location.href = '/login'; }
}

(function() {
    // Embedded-режим (iframe на странице «Дашборды» портала): ?embedded=1
    if (/[?&]embedded=1(?:&|$)/.test(window.location.search)) {
        document.body.classList.add('embedded');
    }
    // Пока портал не ответил — права viewer
    document.body.classList.add('role-viewer');

    fetch('/api/v1/auth/me', { credentials: 'include', headers: { 'Accept': 'application/json' } })
        .then(r => {
            if (!r.ok) throw new Error('auth/me HTTP ' + r.status);
            return r.json();
        })
        .then(user => {
            const role = portalRole(user);
            PORTAL_USER = Object.assign({}, user, { role });
            // Populate user bar
            const nameEl = document.getElementById('user-bar-name');
            if (nameEl) nameEl.textContent = user.name || user.username || user.email || '';
            // Export buttons: admin/analyst only
            if (role !== 'viewer') document.body.classList.remove('role-viewer');
        })
        .catch(() => showAuthOverlay());
})();

// --- Utilities ---
const fmt = {
    int: v => v == null ? '\u2014' : Math.round(v).toLocaleString('ru-RU'),
    rub: v => v == null ? '\u2014' : Math.round(v).toLocaleString('ru-RU') + ' \u20BD',
    pct: v => v == null ? '\u2014' : (v * 100).toFixed(1) + '%',
    dec: v => v == null ? '\u2014' : v.toFixed(2),
    dec3: v => v == null ? '\u2014' : v.toFixed(3),
};

function fmtVal(v, type) { return (fmt[type] || fmt.dec)(v); }

function yoyChange(seg, metric) {
    // Compare selected month vs same month last year using SNAPSHOTS
    const selYm = getSnapYm(getSelectedYm());
    const prevYm = getPrevYearYm(selYm);
    const cur = SNAPSHOTS[selYm]?.[seg]?.[metric];
    const prev = SNAPSHOTS[prevYm]?.[seg]?.[metric];
    if (cur == null || prev == null || prev === 0) return null;
    return (cur - prev) / Math.abs(prev);
}

// Get last 12 months ending at selectedYm (inclusive)
function getLast12Months(selectedYm) {
    const idx = YM_LIST.indexOf(selectedYm);
    if (idx < 0) return YM_LIST.slice(-12);
    const start = Math.max(0, idx - 11);
    return YM_LIST.slice(start, idx + 1);
}

// === UNIFIED SEGMENT COLORS (used everywhere) ===
const UNIFIED_SEG_COLORS = {
    // Russian names (База клиентов, Динамика базы, CVM, ГКГ)
    'Активные LFL': '#003A70',
    'Активные':     '#E87722',
    'Новые':        '#2E8B57',
    'Отток':        '#C41E3A',
    'Случайные':    '#9B59B6',
    'Спящие':       '#7B61FF',
    'Фрод':         '#D4A76A',
    // English keys (Executive Summary, Segment pages)
    'ACTIVE_LFL':   '#003A70',
    'ACTIVE':       '#E87722',
    'NEW':          '#2E8B57',
    'CHURN':        '#C41E3A',
    'RANDOM':       '#9B59B6',
};

// Get array of metric values for a segment over given months
// === GLOBAL CHANNEL TOGGLE (Offline / E-commerce) ===
// Affects: Executive Summary, Segment pages, Price/Check segments
window._activeChannel = window._activeChannel || 'offline'; // 'offline' | 'ecom'

function getActiveSnapshots() {
    if (typeof SNAPSHOTS_ECOM === 'undefined') return SNAPSHOTS;
    return window._activeChannel === 'ecom' ? SNAPSHOTS_ECOM : SNAPSHOTS_OFFLINE;
}

function getActiveSnap(ym) {
    const all = getActiveSnapshots();
    return all[ym] || {};
}

// i_report (Общий отчёт) обновляется раньше сегментных данных — для вкладок
// на SNAPSHOTS берём последний месяц, за который сегментные данные есть
function getSnapYm(ym) {
    const snaps = getActiveSnapshots();
    let idx = YM_LIST.indexOf(ym);
    if (idx < 0) idx = YM_LIST.length - 1;
    for (let i = idx; i >= 0; i--) {
        const m = YM_LIST[i];
        if (snaps[m] && Object.keys(snaps[m]).length) return m;
    }
    return ym;
}

function setActiveChannel(ch) {
    if (window._activeChannel === ch) return;
    window._activeChannel = ch;
    // Re-render whichever tab is currently active
    const activeTab = document.querySelector('.nav-item.active')?.dataset?.tab;
    if (!activeTab) return;
    if (activeTab === 'summary') {
        const sel = document.getElementById('month-selector');
        buildSummary(sel ? sel.value : null);
    } else if (['active_lfl','active','churn','random','new'].includes(activeTab)) {
        const container = document.getElementById('tab-' + activeTab);
        if (container) delete container.dataset.built;
        buildSegmentPage(activeTab.toUpperCase());
    } else if (activeTab === 'price_seg' || activeTab === 'check_seg') {
        if (_segGroupState[activeTab]) _segGroupState[activeTab].channel = ch;
        buildSegmentGroup(activeTab);
    } else if (activeTab === 'trends') {
        buildTrends();
    }
    // Update all toggle UIs
    document.querySelectorAll('.global-channel-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.ch === ch);
    });
}

function renderGlobalChannelToggle() {
    return `
        <div class="seg-channel-toggle global-channel-toggle">
            <button class="seg-channel-btn global-channel-btn ${window._activeChannel === 'offline' ? 'active' : ''}" data-ch="offline" onclick="setActiveChannel('offline')">🏪 Оффлайн</button>
            <button class="seg-channel-btn global-channel-btn ${window._activeChannel === 'ecom' ? 'active' : ''}" data-ch="ecom" onclick="setActiveChannel('ecom')">💻 E-commerce</button>
        </div>
    `;
}

function getMetricSeries(seg, metric, months) {
    const snap = getActiveSnapshots();
    return months.map(ym => snap[ym]?.[seg]?.[metric] ?? null);
}

// Get short month labels like "Фев 25"
function getMonthLabels(months) {
    return months.map(ym => YM_LABELS[ym] || ym);
}

// Currently selected month
function getSelectedYm() {
    const sel = document.getElementById('month-selector');
    return sel ? sel.value : YM_LIST[YM_LIST.length - 1];
}

function yoyPct(seg, metric) {
    const c = yoyChange(seg, metric);
    if (c == null) return '—';
    return (c >= 0 ? '+' : '') + (c * 100).toFixed(1) + '%';
}

const chartInstances = {};
function destroyChart(id) {
    if (chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; }
}

// DIXY brand colors for charts
const CHART_ORANGE = '#FF7900';
const CHART_GREEN = '#00897B';
const CHART_PURPLE = '#6366F1';
const CHART_GRAY = '#94A3B8';

Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 9;
Chart.defaults.scales.linear = Chart.defaults.scales.linear || {};
Chart.defaults.scales.category = Chart.defaults.scales.category || {};
// Grid lines off globally
if (!Chart.defaults.scale.grid) Chart.defaults.scale.grid = {};
Chart.defaults.scale.grid.display = false;
if (!Chart.defaults.scale.border) Chart.defaults.scale.border = {};
Chart.defaults.scale.border.display = false;
Chart.defaults.color = '#6b7b8d';
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyleWidth = 10;
Chart.defaults.plugins.legend.labels.padding = 12;
Chart.defaults.plugins.legend.labels.boxHeight = 8;
Chart.defaults.plugins.legend.labels.font = { size: 10 };
// Add spacing below legend via global plugin
Chart.register({
    id: 'legendSpacing',
    beforeInit(chart) {
        const origFit = chart.legend.fit;
        chart.legend.fit = function() {
            origFit.call(this);
            this.height += 14;
        };
    }
});
Chart.defaults.elements.line.tension = 0.3;
Chart.defaults.elements.point.radius = 3;
Chart.defaults.elements.point.hoverRadius = 5;
Chart.defaults.plugins.datalabels = { display: false };

// --- Navigation ---
const DEFAULT_TAB = 'summary';
const TAB_HASH_RE = /^#tab=([A-Za-z0-9_-]+)$/;

// Активация вкладки — единый путь для клика по nav, хеша URL (#tab=<key>) и внутренних переходов
function activateTab(tab) {
    const item = document.querySelector(`.nav-item[data-tab="${tab}"]`);
    const panel = document.getElementById('tab-' + tab);
    if (!item || !panel) return false;
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    item.classList.add('active');
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    panel.classList.add('active');
    window.scrollTo(0, 0);

    if (['active_lfl','active','churn','random','new'].includes(tab)) {
        buildSegmentPage(tab.toUpperCase());
    }
    if (tab === 'report') {
        buildReport();
    }
    if (tab === 'clientbase') {
        buildClientBase();
    }
    if (tab === 'cvm') {
        buildCVM();
    }
    if (tab === 'dynamics') {
        buildDynamics();
    }
    if (tab === 'gcg') {
        buildGCG();
    }
    if (tab === 'trends') {
        buildTrends();
    }
    if (tab === 'price_seg') {
        buildSegmentGroup('price_seg');
    }
    if (tab === 'check_seg') {
        buildSegmentGroup('check_seg');
    }
    if (tab === 'coffee') {
        buildCoffee();
    }

    syncTabHash(tab);
    notifyParentTab(tab);
    return true;
}

// Хеш в URL: без прокрутки и без события hashchange
function syncTabHash(tab) {
    if (window.location.hash === '#tab=' + tab) return;
    try { history.replaceState(null, '', '#tab=' + tab); } catch (e) {}
}

// Сообщаем родительской странице портала активную вкладку
function notifyParentTab(tab) {
    try {
        if (window.parent && window.parent !== window) {
            window.parent.postMessage({ type: 'cvm-analytics-tab', tab: tab }, '*');
        }
    } catch (e) {}
}

function tabFromHash() {
    const m = TAB_HASH_RE.exec(window.location.hash || '');
    return m ? m[1] : '';
}

// Вкладка из URL при загрузке и по hashchange; неизвестный/пустой key → вкладка по умолчанию.
// Идём через click() по пункту nav — тот же путь, что и у пользователя (включая слушатели
// на document, которые инициализируют «Миссии» по клику).
function applyTabFromHash() {
    const key = tabFromHash();
    const item = document.querySelector(`.nav-item[data-tab="${key}"]`)
        || document.querySelector(`.nav-item[data-tab="${DEFAULT_TAB}"]`);
    if (!item) return;
    if (item.classList.contains('active')) { notifyParentTab(item.dataset.tab); return; }
    item.click();
}

document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', e => {
        e.preventDefault();
        activateTab(item.dataset.tab);
    });
});
window.addEventListener('hashchange', applyTabFromHash);

// --- Cross-links to definitions ---
document.addEventListener('click', e => {
    const link = e.target.closest('.seg-def-link');
    if (!link) return;
    e.preventDefault();
    const seg = link.dataset.def;
    // Switch to definitions tab (общий путь: active, hash, postMessage)
    activateTab('definitions');
    // Scroll to specific definition
    setTimeout(() => {
        const el = document.getElementById('def-' + seg);
        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 100);
});

// --- Month Selector ---
function initMonthSelector() {
    const sel = document.getElementById('month-selector');
    YM_LIST.forEach(ym => {
        const opt = document.createElement('option');
        opt.value = ym;
        opt.textContent = YM_LABELS[ym];
        sel.appendChild(opt);
    });
    sel.value = YM_LIST[YM_LIST.length - 1];
    sel.addEventListener('change', () => {
        buildSummary(sel.value);
        // Reset segment page caches so they rebuild with context
        document.querySelectorAll('.tab-content[data-built]').forEach(el => delete el.dataset.built);
    });
}

function getSnap(ym) {
    // Use active channel snapshots if available, fallback to legacy SNAPSHOTS
    if (typeof SNAPSHOTS_OFFLINE !== 'undefined') {
        return getActiveSnap(ym);
    }
    return SNAPSHOTS[ym] || {};
}

function getPrevYearYm(ym) {
    const y = parseInt(ym.slice(0, 4)) - 1;
    const m = ym.slice(4);
    return '' + y + m;
}

function snapChange(snap, prevSnap, seg, metric) {
    const cur = snap[seg]?.[metric];
    const prev = prevSnap[seg]?.[metric];
    if (cur == null || prev == null || prev === 0) return null;
    return (cur - prev) / prev;
}

// --- Executive Summary ---
function buildSummary(ym) {
    if (!ym) ym = YM_LIST[YM_LIST.length - 1];
    const requestedYm = ym;
    ym = getSnapYm(ym);

    // Пометка, если выбранный месяц новее сегментных данных
    let snapNote = document.getElementById('summary-snap-note');
    if (!snapNote) {
        snapNote = document.createElement('div');
        snapNote.id = 'summary-snap-note';
        snapNote.className = 'snap-fallback-note';
        document.querySelector('#tab-summary .page-header')?.insertAdjacentElement('afterend', snapNote);
    }
    if (requestedYm !== ym) {
        snapNote.style.display = '';
        snapNote.textContent = `Данные по сегментам доступны по ${YM_LABELS[ym]} — показатели ниже за этот месяц. Данные за ${YM_LABELS[requestedYm]} доступны на вкладке «Общий отчёт».`;
    } else {
        snapNote.style.display = 'none';
    }

    const snap = getSnap(ym);
    const prevYm = getPrevYearYm(ym);
    const prevSnap = getSnap(prevYm);
    const label = YM_LABELS[ym];
    const channelLbl = window._activeChannel === 'ecom' ? 'E-commerce' : 'Offline';

    // Channel toggle
    const ct = document.getElementById('summary-channel-toggle');
    if (ct) ct.innerHTML = renderGlobalChannelToggle();

    // Update table title
    document.getElementById('summary-table-title').textContent = `Сводная таблица по сегментам (${label} · ${channelLbl})`;

    const totalClients = ['NEW','ACTIVE','ACTIVE_LFL','RANDOM'].reduce((s, seg) => s + (snap[seg]?.CLIENTS || 0), 0);
    const prevTotal = ['NEW','ACTIVE','ACTIVE_LFL','RANDOM'].reduce((s, seg) => s + (prevSnap[seg]?.CLIENTS || 0), 0);
    const totalChange = prevTotal ? (totalClients - prevTotal) / prevTotal : null;

    // --- Total KPI Card ---
    const totalKpi = document.getElementById('total-kpi');
    totalKpi.innerHTML = `
        <div class="kpi-label">Всего клиентов (без оттока)</div>
        <div class="kpi-value">${fmt.int(totalClients)}</div>
        ${totalChange != null ? `<div class="kpi-change ${totalChange >= 0 ? 'up' : 'down'}">${totalChange >= 0 ? '+' : ''}${(totalChange*100).toFixed(1)}% YoY</div>` : ''}
        <div class="kpi-sub">${label}</div>
    `;

    // --- Segment Pie Chart ---
    destroyChart('chart-segment-pie');
    const pieSegs = ['NEW','ACTIVE','ACTIVE_LFL','RANDOM','CHURN'];
    const pieData = pieSegs.map(s => snap[s]?.CLIENTS || 0);
    const pieColors = pieSegs.map(s => SEG_COLORS[s]);
    const pieLabels = pieSegs.map(s => SEG_RU[s]);
    const pieTotal = pieData.reduce((a, b) => a + b, 0);
    const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text').trim() || '#e0e0e0';
    const cardBg = getComputedStyle(document.documentElement).getPropertyValue('--card-bg').trim() || '#1e1e2e';

    // Custom plugin: outlabels with connector lines + anti-collision
    const outlabelPlugin = {
        id: 'pieOutlabels',
        afterDraw(chart) {
            const { ctx } = chart;
            const meta = chart.getDatasetMeta(0);
            const ds = chart.data.datasets[0];
            const total = ds.data.reduce((a, b) => a + b, 0);
            if (!total) return;

            const labelHeight = 28;
            // Collect label positions, split into left/right
            const leftLabels = [], rightLabels = [];

            meta.data.forEach((arc, i) => {
                const val = ds.data[i];
                if (!val) return;
                const pct = ((val / total) * 100).toFixed(1);
                const lbl = chart.data.labels[i];
                const midAngle = (arc.startAngle + arc.endAngle) / 2;
                const outerR = arc.outerRadius;
                const cx = arc.x;
                const cy = arc.y;
                const edgeX = cx + Math.cos(midAngle) * outerR;
                const edgeY = cy + Math.sin(midAngle) * outerR;
                const isRight = Math.cos(midAngle) >= 0;
                const elbowLen = 16;
                const elbowX = cx + Math.cos(midAngle) * (outerR + elbowLen);
                const elbowY = cy + Math.sin(midAngle) * (outerR + elbowLen);

                const item = { i, lbl, pct, edgeX, edgeY, elbowX, elbowY, isRight, color: ds.backgroundColor[i], y: elbowY };
                (isRight ? rightLabels : leftLabels).push(item);
            });

            // Sort by Y and push apart overlapping labels
            function resolveCollisions(labels) {
                labels.sort((a, b) => a.y - b.y);
                for (let pass = 0; pass < 5; pass++) {
                    for (let j = 1; j < labels.length; j++) {
                        const gap = labels[j].y - labels[j-1].y;
                        if (gap < labelHeight) {
                            const shift = (labelHeight - gap) / 2;
                            labels[j-1].y -= shift;
                            labels[j].y += shift;
                        }
                    }
                }
            }

            resolveCollisions(leftLabels);
            resolveCollisions(rightLabels);

            // Draw all labels
            [...leftLabels, ...rightLabels].forEach(item => {
                const lineLen = 22;
                const endX = item.elbowX + (item.isRight ? lineLen : -lineLen);
                const endY = item.y;

                ctx.save();
                ctx.strokeStyle = item.color;
                ctx.lineWidth = 1.5;
                ctx.beginPath();
                ctx.moveTo(item.edgeX, item.edgeY);
                ctx.lineTo(item.elbowX, item.elbowY);
                ctx.lineTo(endX, endY);
                ctx.stroke();

                ctx.fillStyle = item.color;
                ctx.beginPath();
                ctx.arc(item.edgeX, item.edgeY, 2.5, 0, Math.PI * 2);
                ctx.fill();

                const textX = endX + (item.isRight ? 4 : -4);
                ctx.textAlign = item.isRight ? 'left' : 'right';
                ctx.textBaseline = 'middle';
                ctx.font = '600 11px Inter, sans-serif';
                ctx.fillStyle = textColor;
                ctx.fillText(item.lbl, textX, endY - 6);
                ctx.font = '700 11px Inter, sans-serif';
                ctx.fillStyle = item.color;
                ctx.fillText(item.pct + '%', textX, endY + 7);
                ctx.restore();
            });
        }
    };

    chartInstances['chart-segment-pie'] = new Chart(document.getElementById('chart-segment-pie'), {
        type: 'doughnut',
        data: {
            labels: pieLabels,
            datasets: [{
                data: pieData,
                backgroundColor: pieColors,
                borderWidth: 2,
                borderColor: cardBg
            }]
        },
        plugins: [outlabelPlugin],
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '45%',
            layout: { padding: { top: 50, bottom: 40, left: 100, right: 100 } },
            plugins: {
                legend: { display: false },
                datalabels: { display: false },
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            const v = ctx.parsed;
                            const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = total ? ((v / total) * 100).toFixed(1) : 0;
                            return ` ${ctx.label}: ${fmt.int(v)} (${pct}%)`;
                        }
                    }
                },
                title: { display: false }
            }
        }
    });

    // --- Weighted average ARPU (all segments excl. churn) ---
    const activeSegs = ['NEW','ACTIVE','ACTIVE_LFL','RANDOM'];
    function weightedArpu(s) {
        let sumCB = 0, sumC = 0;
        activeSegs.forEach(seg => {
            const c = s[seg]?.CLIENTS || 0;
            const b = s[seg]?.BUDGET || 0;
            sumCB += c * b;
            sumC += c;
        });
        return sumC ? sumCB / sumC : null;
    }
    const curArpu = weightedArpu(snap);
    const prevArpu = weightedArpu(prevSnap);
    const arpuChange = (curArpu != null && prevArpu != null && prevArpu !== 0) ? (curArpu - prevArpu) / prevArpu : null;

    // --- Other KPI Cards ---
    const kpiGrid = document.getElementById('kpi-cards');
    const kpis = [
        { label: 'Новые клиенты', value: fmt.int(snap.NEW?.CLIENTS), color: CHART_GREEN,
          change: snapChange(snap, prevSnap, 'NEW', 'CLIENTS') },
        { label: 'Ядро (LFL)', value: fmt.pct(snap.ACTIVE_LFL?.SHARE_CLIENTS), color: CHART_ORANGE,
          change: snapChange(snap, prevSnap, 'ACTIVE_LFL', 'SHARE_CLIENTS') },
        { label: 'Отток', value: fmt.pct(snap.CHURN?.SHARE_CLIENTS), color: '#C6364A',
          change: snapChange(snap, prevSnap, 'CHURN', 'SHARE_CLIENTS') },
        { label: 'ARPU (ср. взв.)', value: fmt.rub(curArpu), color: CHART_ORANGE,
          change: arpuChange },
    ];

    kpiGrid.innerHTML = kpis.map(k => `
        <div class="kpi-card" style="border-top-color:${k.color}">
            <div class="kpi-label">${k.label}</div>
            <div class="kpi-value">${k.value}</div>
            ${k.change != null ? `<div class="kpi-change ${k.change >= 0 ? 'up' : 'down'}">${k.change >= 0 ? '+' : ''}${(k.change*100).toFixed(1)}% YoY</div>` : ''}
            <div class="kpi-sub">${label}</div>
        </div>
    `).join('');

    // Summary Table
    const table = document.getElementById('summary-table');

    // Compute share of turnover per segment
    const allSegs = SEGMENTS;
    const totalTO = allSegs.reduce((s, seg) => s + (snap[seg]?.CLIENTS || 0) * (snap[seg]?.BUDGET || 0), 0);
    const prevTotalTO = allSegs.reduce((s, seg) => s + (prevSnap[seg]?.CLIENTS || 0) * (prevSnap[seg]?.BUDGET || 0), 0);
    function segShareTO(s, seg) {
        const tot = allSegs.reduce((sum, sg) => sum + (s[sg]?.CLIENTS || 0) * (s[sg]?.BUDGET || 0), 0);
        const segTO = (s[seg]?.CLIENTS || 0) * (s[seg]?.BUDGET || 0);
        return tot ? segTO / tot : null;
    }

    const cols = [
        ['SHARE_CLIENTS', 'Доля базы', 'pct', null],
        ['SHARE_TO', 'Доля в ТО', 'pct', 'computed'],
        ['CLIENTS', 'Клиентов', 'int', null],
        ['BUDGET', 'ARPU', 'rub', null],
        ['COUNT_CHECK', 'Чеков/клиента', 'dec', 'arpu'],
        ['AVG_CHECK', 'Ср. чек', 'rub', 'arpu'],
        ['AVG_SKU', 'SKU/чек', 'dec', 'check'],
        ['AVG_COST_SKU', 'Ср. цена SKU', 'rub', 'check'],
        ['PRICE_INDEX', 'Цен. индекс', 'dec3', null],
        ['SALE', 'Скидка карта', 'pct', null],
        ['REAL_SALE', 'Реал. скидка', 'pct', null],
    ];

    // Computed columns
    const computedCols = [
        {
            label: 'Баллов/кл.',
            getValue: (seg) => {
                const bp = snap[seg]?.BONUS_PAY;
                const cl = snap[seg]?.CLIENTS;
                return (bp != null && cl) ? Math.abs(bp) / cl : null;
            },
            getPrev: (seg) => {
                const bp = prevSnap[seg]?.BONUS_PAY;
                const cl = prevSnap[seg]?.CLIENTS;
                return (bp != null && cl) ? Math.abs(bp) / cl : null;
            },
            fmt: v => v == null ? '—' : Math.round(v).toLocaleString('ru-RU')
        },
        {
            label: 'Redemption',
            getValue: (seg) => snap[seg]?.Redemption ?? null,
            getPrev: (seg) => prevSnap[seg]?.Redemption ?? null,
            fmt: v => v == null ? '—' : (v * 100).toFixed(1) + '%'
        }
    ];

    function yoyBadge(seg, col) {
        const cur = snap[seg]?.[col];
        const prev = prevSnap[seg]?.[col];
        if (cur == null || prev == null || prev === 0) return '';
        const ch = (cur - prev) / prev;
        const cls = ch >= 0 ? 'up' : 'down';
        const sign = ch >= 0 ? '+' : '';
        return `<span class="tbl-yoy ${cls}">${sign}${(ch*100).toFixed(1)}%</span>`;
    }

    function computedYoyBadge(cur, prev) {
        if (cur == null || prev == null || prev === 0) return '';
        const ch = (cur - prev) / prev;
        const cls = ch >= 0 ? 'up' : 'down';
        const sign = ch >= 0 ? '+' : '';
        return `<span class="tbl-yoy ${cls}">${sign}${(ch*100).toFixed(1)}%</span>`;
    }

    // Build two-row header: group row + column names row
    const groupRow = [];
    const colRow = [];
    let i = 0;
    groupRow.push('<th rowspan="2" class="th-seg">Сегмент</th>');
    while (i < cols.length) {
        const [col, lbl, , grp] = cols[i];
        if (grp === 'arpu') {
            // ARPU group: spans 2 child cols (Чеков/клиента + Ср. чек)
            groupRow.push('<th colspan="2" class="th-group th-group-arpu">ARPU = Чеки × Ср. чек</th>');
            colRow.push(`<th class="th-child th-child-arpu">${cols[i][1]}</th>`);
            colRow.push(`<th class="th-child th-child-arpu">${cols[i+1][1]}</th>`);
            i += 2;
        } else if (grp === 'check') {
            // Check group: spans 2 child cols (SKU/чек + Ср. цена SKU)
            groupRow.push('<th colspan="2" class="th-group th-group-check">Ср. чек = SKU × Цена</th>');
            colRow.push(`<th class="th-child th-child-check">${cols[i][1]}</th>`);
            colRow.push(`<th class="th-child th-child-check">${cols[i+1][1]}</th>`);
            i += 2;
        } else {
            groupRow.push(`<th rowspan="2">${lbl}</th>`);
            i++;
        }
    }
    computedCols.forEach(c => groupRow.push(`<th rowspan="2">${c.label}</th>`));

    function cellClass(grp) {
        if (grp === 'arpu') return ' class="td-arpu"';
        if (grp === 'check') return ' class="td-check"';
        return '';
    }

    table.innerHTML = `
        <thead>
            <tr>${groupRow.join('')}</tr>
            <tr>${colRow.join('')}</tr>
        </thead>
        <tbody>
            ${SEGMENTS.map(seg => `<tr>
                <td><a href="#" class="seg-badge seg-def-link" data-def="${seg}" title="Определение сегмента"><span class="dot" style="background:${SEG_COLORS[seg]}"></span>${SEG_RU[seg]}</a></td>
                ${cols.map(([col, , type, grp]) => {
                    if (col === 'SHARE_TO') {
                        const cur = segShareTO(snap, seg);
                        const prev = segShareTO(prevSnap, seg);
                        return `<td><div class="tbl-cell">${fmtVal(cur, type)}${computedYoyBadge(cur, prev)}</div></td>`;
                    }
                    return `<td${cellClass(grp)}><div class="tbl-cell">${fmtVal(snap[seg]?.[col], type)}${yoyBadge(seg, col)}</div></td>`;
                }).join('')}
                ${computedCols.map(cc => {
                    const cur = cc.getValue(seg);
                    const prev = cc.getPrev(seg);
                    return `<td><div class="tbl-cell">${cc.fmt(cur)}${computedYoyBadge(cur, prev)}</div></td>`;
                }).join('')}
            </tr>`).join('')}
        </tbody>
    `;

    // --- Dynamic 12-month charts ---
    const last12 = getLast12Months(ym);
    const xLabels = getMonthLabels(last12);

    // Update chart titles
    const firstLabel = YM_LABELS[last12[0]] || '';
    const lastLabel = YM_LABELS[last12[last12.length - 1]] || '';
    const periodStr = firstLabel + ' — ' + lastLabel;

    document.getElementById('chart-structure')?.closest('.card')?.querySelector('h3')
        && (document.getElementById('chart-structure').closest('.card').querySelector('h3').textContent = 'Структура клиентской базы (' + periodStr + ')');
    document.getElementById('chart-arpu-all')?.closest('.card')?.querySelector('h3')
        && (document.getElementById('chart-arpu-all').closest('.card').querySelector('h3').textContent = 'ARPU по сегментам (' + periodStr + '), руб.');
    document.getElementById('chart-avgcheck-all')?.closest('.card')?.querySelector('h3')
        && (document.getElementById('chart-avgcheck-all').closest('.card').querySelector('h3').textContent = 'Средний чек по сегментам (' + periodStr + '), руб.');
    document.getElementById('chart-freq-all')?.closest('.card')?.querySelector('h3')
        && (document.getElementById('chart-freq-all').closest('.card').querySelector('h3').textContent = 'Чеков/клиента по сегментам (' + periodStr + ')');

    // Structure chart — CHURN shown as negative below zero
    destroyChart('chart-structure');
    const positiveSegs = SEGMENTS.filter(s => s !== 'CHURN');
    const structDatasets = positiveSegs.map(seg => ({
        label: SEG_RU[seg],
        data: getMetricSeries(seg, 'SHARE_CLIENTS', last12),
        backgroundColor: SEG_COLORS[seg],
        borderRadius: 2,
        stack: 'positive'
    }));
    structDatasets.push({
        label: SEG_RU['CHURN'],
        data: getMetricSeries('CHURN', 'SHARE_CLIENTS', last12).map(v => v != null ? -v : null),
        backgroundColor: SEG_COLORS['CHURN'],
        borderRadius: 2,
        stack: 'negative'
    });
    chartInstances['chart-structure'] = new Chart(document.getElementById('chart-structure'), {
        type: 'bar',
        data: { labels: xLabels, datasets: structDatasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: {
                x: { stacked: true, grid: { display: false } },
                y: {
                    stacked: true,
                    ticks: { callback: v => (Math.abs(v)*100).toFixed(0) + '%' },
                    grid: { color: 'rgba(255,255,255,0.07)' }
                }
            },
            plugins: {
                legend: { position: 'top', align: 'start' },
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            const v = Math.abs(ctx.parsed.y);
                            return ` ${ctx.dataset.label}: ${(v*100).toFixed(1)}%`;
                        }
                    }
                }
            }
        }
    });

    // ARPU line chart
    destroyChart('chart-arpu-all');
    chartInstances['chart-arpu-all'] = new Chart(document.getElementById('chart-arpu-all'), {
        type: 'line',
        data: {
            labels: xLabels,
            datasets: SEGMENTS.map(seg => ({
                label: SEG_RU[seg],
                data: getMetricSeries(seg, 'BUDGET', last12),
                borderColor: SEG_COLORS[seg],
                borderWidth: 2.5, fill: false,
            }))
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: { ticks: { callback: v => v.toLocaleString('ru-RU') }, grid: { color: '#f0f2f5' } }
            },
            plugins: { legend: { position: 'top', align: 'start' } }
        }
    });

    // Avg check chart
    destroyChart('chart-avgcheck-all');
    chartInstances['chart-avgcheck-all'] = new Chart(document.getElementById('chart-avgcheck-all'), {
        type: 'line',
        data: {
            labels: xLabels,
            datasets: SEGMENTS.map(seg => ({
                label: SEG_RU[seg],
                data: getMetricSeries(seg, 'AVG_CHECK', last12),
                borderColor: SEG_COLORS[seg],
                borderWidth: 2.5, fill: false,
            }))
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: { ticks: { callback: v => v.toLocaleString('ru-RU') }, grid: { color: '#f0f2f5' } }
            },
            plugins: { legend: { position: 'top', align: 'start' } }
        }
    });

    // Frequency chart
    destroyChart('chart-freq-all');
    chartInstances['chart-freq-all'] = new Chart(document.getElementById('chart-freq-all'), {
        type: 'line',
        data: {
            labels: xLabels,
            datasets: SEGMENTS.map(seg => ({
                label: SEG_RU[seg],
                data: getMetricSeries(seg, 'COUNT_CHECK', last12),
                borderColor: SEG_COLORS[seg],
                borderWidth: 2.5, fill: false,
            }))
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: { grid: { color: '#f0f2f5' } }
            },
            plugins: { legend: { position: 'top', align: 'start' } }
        }
    });

    // Previous year 12-month period
    const prevLast12 = last12.map(m => {
        const y = parseInt(m.slice(0, 4)) - 1;
        return '' + y + m.slice(4);
    });
    const prevFirstLabel = YM_LABELS[prevLast12[0]] || '';
    const prevLastLabel = YM_LABELS[prevLast12[prevLast12.length - 1]] || '';
    const prevPeriodStr = prevFirstLabel + ' — ' + prevLastLabel;

    // Retention chart (1 - CHURN share = retained %)
    destroyChart('chart-retention');
    document.getElementById('chart-retention')?.closest('.card')?.querySelector('h3')
        && (document.getElementById('chart-retention').closest('.card').querySelector('h3').textContent = 'Удержание клиентов (' + periodStr + ')');
    const retentionData = last12.map(m => {
        const churnShare = SNAPSHOTS[m]?.['CHURN']?.SHARE_CLIENTS;
        return churnShare != null ? 1 - churnShare : null;
    });
    const prevRetentionData = prevLast12.map(m => {
        const churnShare = SNAPSHOTS[m]?.['CHURN']?.SHARE_CLIENTS;
        return churnShare != null ? 1 - churnShare : null;
    });

    chartInstances['chart-retention'] = new Chart(document.getElementById('chart-retention'), {
        type: 'line',
        data: {
            labels: xLabels,
            datasets: [
                { label: prevPeriodStr, data: prevRetentionData, borderColor: CHART_GRAY, borderDash: [6,3], borderWidth: 2, fill: false, pointRadius: 2 },
                { label: periodStr, data: retentionData, borderColor: CHART_ORANGE, borderWidth: 2.5,
                  fill: true, backgroundColor: 'rgba(255,121,0,0.08)' },
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: { ticks: { callback: v => (v*100).toFixed(0)+'%' }, grid: { color: '#f0f2f5' },
                     suggestedMin: 0.7, suggestedMax: 0.9 }
            },
            plugins: { legend: { position: 'top', align: 'start' } }
        }
    });

    // Loyalty Index chart
    destroyChart('chart-loyalty');
    document.getElementById('chart-loyalty')?.closest('.card')?.querySelector('h3')
        && (document.getElementById('chart-loyalty').closest('.card').querySelector('h3').textContent = 'Индекс лояльности по сегментам (' + periodStr + ')');
    chartInstances['chart-loyalty'] = new Chart(document.getElementById('chart-loyalty'), {
        type: 'line',
        data: {
            labels: xLabels,
            datasets: SEGMENTS.filter(s => s !== 'CHURN').map(seg => ({
                label: SEG_RU[seg],
                data: getMetricSeries(seg, 'LOYALTY_INDEX', last12),
                borderColor: SEG_COLORS[seg],
                borderWidth: 2.5, fill: false,
            }))
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: {
                x: { grid: { display: false } },
                y: { ticks: { callback: v => v.toFixed(3) }, grid: { color: '#f0f2f5' } }
            },
            plugins: { legend: { position: 'top', align: 'start' } }
        }
    });

    // McKinsey addendum block
    buildMckinseyAddendum(ym);
}

// --- McKinsey-style Executive Addendum ---
function buildMckinseyAddendum(ym) {
    const container = document.getElementById('mck-addendum');
    if (!container) return;

    const snap = SNAPSHOTS_OFFLINE[ym] || {};
    const snapEcom = SNAPSHOTS_ECOM[ym] || {};
    const prevYm = getPrevYearYm(ym);
    const prevSnap = SNAPSHOTS_OFFLINE[prevYm] || {};
    const ymLabel = YM_LABELS[ym] || ym;
    const lifecycleSegs = ['ACTIVE_LFL','ACTIVE','RANDOM','NEW','CHURN'];

    // 1. Pareto: revenue contribution by segment
    const totalRevenue = lifecycleSegs.reduce((s, seg) => s + ((snap[seg]?.CLIENTS || 0) * (snap[seg]?.BUDGET || 0)), 0);
    const segContrib = lifecycleSegs
        .map(seg => ({
            seg,
            label: SEG_RU[seg] || seg,
            color: SEG_COLORS[seg],
            clients: snap[seg]?.CLIENTS || 0,
            arpu: snap[seg]?.BUDGET || 0,
            revenue: (snap[seg]?.CLIENTS || 0) * (snap[seg]?.BUDGET || 0)
        }))
        .sort((a, b) => b.revenue - a.revenue);

    let cumPct = 0;
    segContrib.forEach(s => { s.pctRevenue = totalRevenue ? s.revenue / totalRevenue : 0; cumPct += s.pctRevenue; s.cumPct = cumPct; });

    // 2. Health indicators (traffic-light) for key KPIs
    const healthChecks = [
        { label: 'База клиентов', cur: lifecycleSegs.filter(s=>s!=='CHURN').reduce((a,s)=>a+(snap[s]?.CLIENTS||0),0),
          prev: lifecycleSegs.filter(s=>s!=='CHURN').reduce((a,s)=>a+(prevSnap[s]?.CLIENTS||0),0), unit: 'M', divisor: 1e6, format: v => (v/1e6).toFixed(2)+' млн' },
        { label: 'ARPU (LFL)', cur: snap.ACTIVE_LFL?.BUDGET || 0, prev: prevSnap.ACTIVE_LFL?.BUDGET || 0, format: v => Math.round(v).toLocaleString('ru-RU')+' ₽' },
        { label: 'Доля оттока', cur: snap.CHURN?.SHARE_CLIENTS || 0, prev: prevSnap.CHURN?.SHARE_CLIENTS || 0, format: v => (v*100).toFixed(1)+'%', invert: true },
        { label: 'Чеков/клиента (LFL)', cur: snap.ACTIVE_LFL?.COUNT_CHECK || 0, prev: prevSnap.ACTIVE_LFL?.COUNT_CHECK || 0, format: v => v.toFixed(2) },
        { label: 'Средний чек (LFL)', cur: snap.ACTIVE_LFL?.AVG_CHECK || 0, prev: prevSnap.ACTIVE_LFL?.AVG_CHECK || 0, format: v => Math.round(v)+' ₽' },
        { label: 'Реал. скидка (LFL)', cur: snap.ACTIVE_LFL?.REAL_SALE || 0, prev: prevSnap.ACTIVE_LFL?.REAL_SALE || 0, format: v => (v*100).toFixed(1)+'%', invert: true },
    ];
    healthChecks.forEach(h => {
        h.delta = h.prev ? (h.cur - h.prev) / h.prev : 0;
        const dPct = h.delta * 100;
        const positive = h.invert ? dPct < 0 : dPct > 0;
        const big = Math.abs(dPct) > 5;
        h.status = !big ? 'yellow' : (positive ? 'green' : 'red');
    });

    // 3. What-if: revenue impact of +5% increase in each segment
    const whatIfImpact = lifecycleSegs.filter(s => s !== 'CHURN').map(seg => {
        const cli = snap[seg]?.CLIENTS || 0;
        const arpu = snap[seg]?.BUDGET || 0;
        const baseRev = cli * arpu;
        // Scenarios:
        const ifClients5pct = cli * 1.05 * arpu - baseRev;
        const ifArpu5pct = cli * arpu * 1.05 - baseRev;
        return { seg, label: SEG_RU[seg], color: SEG_COLORS[seg], baseRev, deltaClients: ifClients5pct, deltaArpu: ifArpu5pct };
    }).sort((a,b) => b.baseRev - a.baseRev);

    // 4. Offline vs Ecom comparison (latest)
    const offTotal = lifecycleSegs.filter(s=>s!=='CHURN').reduce((a,s)=>a+(snap[s]?.CLIENTS||0),0);
    const ecomTotal = lifecycleSegs.filter(s=>s!=='CHURN').reduce((a,s)=>a+(snapEcom[s]?.CLIENTS||0),0);
    const offRev = lifecycleSegs.filter(s=>s!=='CHURN').reduce((a,s)=>a+((snap[s]?.CLIENTS||0)*(snap[s]?.BUDGET||0)),0);
    const ecomRev = lifecycleSegs.filter(s=>s!=='CHURN').reduce((a,s)=>a+((snapEcom[s]?.CLIENTS||0)*(snapEcom[s]?.BUDGET||0)),0);

    container.innerHTML = `
        <!-- McKinsey divider header -->
        <div class="mck-divider">
            <div class="mck-divider-line"></div>
            <div class="mck-divider-label">📊 АНАЛИТИЧЕСКИЙ БЛОК · ${ymLabel}</div>
            <div class="mck-divider-line"></div>
        </div>

        <!-- 1. Health Dashboard -->
        <div class="card mck-card">
            <div class="card-header"><h3>🚦 Health Check · KPI vs ${YM_LABELS[prevYm] || 'прошлый год'}</h3></div>
            <div class="mck-health-grid">
                ${healthChecks.map(h => `
                    <div class="mck-health-cell mck-health-${h.status}">
                        <div class="mck-health-label">${h.label}</div>
                        <div class="mck-health-value">${h.format(h.cur)}</div>
                        <div class="mck-health-delta ${h.delta >= 0 ? 'up' : 'down'}">${h.delta >= 0 ? '+' : ''}${(h.delta*100).toFixed(1)}% YoY</div>
                    </div>
                `).join('')}
            </div>
            <div class="mck-legend">
                <span class="mck-dot mck-dot-green"></span>&nbsp;Сильный рост (>5%)
                <span class="mck-dot mck-dot-yellow" style="margin-left:16px"></span>&nbsp;Стабильно (±5%)
                <span class="mck-dot mck-dot-red" style="margin-left:16px"></span>&nbsp;Снижение / риск
            </div>
        </div>

        <!-- 2. Pareto: Revenue Concentration -->
        <div class="card mck-card">
            <div class="card-header">
                <h3>💰 Pareto-анализ: вклад сегментов в выручку</h3>
                <span class="mck-subtitle">Какие сегменты делают 80% оборота</span>
            </div>
            <div class="mck-pareto">
                ${segContrib.map((s, i) => `
                    <div class="mck-pareto-row">
                        <div class="mck-pareto-label">
                            <span class="dot" style="background:${s.color}"></span> ${s.label}
                        </div>
                        <div class="mck-pareto-bar-wrap">
                            <div class="mck-pareto-bar" style="width:${s.pctRevenue*100}%;background:${s.color}"></div>
                            <span class="mck-pareto-bar-label">${(s.pctRevenue*100).toFixed(1)}%</span>
                        </div>
                        <div class="mck-pareto-cum">∑ ${(s.cumPct*100).toFixed(0)}%</div>
                        <div class="mck-pareto-revenue">${(s.revenue/1e9).toFixed(2)} млрд ₽</div>
                    </div>
                `).join('')}
            </div>
            <div class="mck-conclusion">
                <strong>Вывод:</strong> Топ-2 сегмента (${segContrib[0].label} + ${segContrib[1].label})
                делают <strong>${((segContrib[0].pctRevenue + segContrib[1].pctRevenue)*100).toFixed(0)}%</strong> выручки.
                Любая просадка retention в этих сегментах = риск для P&L.
            </div>
        </div>

        <!-- 3. What-if Scenarios -->
        <div class="card mck-card">
            <div class="card-header">
                <h3>🎯 What-if: эффект роста +5% по сегменту</h3>
                <span class="mck-subtitle">Прирост выручки/мес при увеличении базы или ARPU на 5%</span>
            </div>
            <table class="data-table mck-whatif">
                <thead>
                    <tr>
                        <th>Сегмент</th>
                        <th>Текущая выручка</th>
                        <th>+5% к базе клиентов</th>
                        <th>+5% к ARPU</th>
                        <th>Совокупно (×1.1025)</th>
                    </tr>
                </thead>
                <tbody>
                    ${whatIfImpact.map(w => `
                        <tr>
                            <td><span class="dot" style="background:${w.color}"></span> ${w.label}</td>
                            <td>${(w.baseRev/1e9).toFixed(2)} млрд</td>
                            <td class="mck-positive">+${(w.deltaClients/1e6).toFixed(1)} М ₽</td>
                            <td class="mck-positive">+${(w.deltaArpu/1e6).toFixed(1)} М ₽</td>
                            <td class="mck-positive">+${((w.baseRev*0.1025)/1e6).toFixed(1)} М ₽</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>

        <!-- 4. Offline vs E-commerce -->
        <div class="card mck-card">
            <div class="card-header">
                <h3>🌐 Offline vs E-commerce · ${ymLabel}</h3>
                <span class="mck-subtitle">Базовый дисбаланс каналов и потенциал омни-роста</span>
            </div>
            <div class="mck-channel-grid">
                <div class="mck-channel-cell">
                    <div class="mck-channel-title" style="color:#003A70">🏪 ОФФЛАЙН</div>
                    <div class="mck-channel-row"><span>Клиентов:</span><strong>${(offTotal/1e6).toFixed(2)} млн</strong></div>
                    <div class="mck-channel-row"><span>ARPU (LFL):</span><strong>${Math.round(snap.ACTIVE_LFL?.BUDGET || 0).toLocaleString('ru-RU')} ₽</strong></div>
                    <div class="mck-channel-row"><span>Ср. чек (LFL):</span><strong>${Math.round(snap.ACTIVE_LFL?.AVG_CHECK || 0)} ₽</strong></div>
                    <div class="mck-channel-row"><span>Чеков/кл (LFL):</span><strong>${(snap.ACTIVE_LFL?.COUNT_CHECK || 0).toFixed(2)}</strong></div>
                    <div class="mck-channel-row"><span>Выручка/мес:</span><strong>${(offRev/1e9).toFixed(2)} млрд ₽</strong></div>
                </div>
                <div class="mck-channel-cell">
                    <div class="mck-channel-title" style="color:#2E8B57">💻 E-COMMERCE</div>
                    <div class="mck-channel-row"><span>Клиентов:</span><strong>${(ecomTotal/1e3).toFixed(0)} тыс</strong></div>
                    <div class="mck-channel-row"><span>ARPU (LFL):</span><strong>${Math.round(snapEcom.ACTIVE_LFL?.BUDGET || 0).toLocaleString('ru-RU')} ₽</strong></div>
                    <div class="mck-channel-row"><span>Ср. чек (LFL):</span><strong>${Math.round(snapEcom.ACTIVE_LFL?.AVG_CHECK || 0)} ₽</strong></div>
                    <div class="mck-channel-row"><span>Чеков/кл (LFL):</span><strong>${(snapEcom.ACTIVE_LFL?.COUNT_CHECK || 0).toFixed(2)}</strong></div>
                    <div class="mck-channel-row"><span>Выручка/мес:</span><strong>${(ecomRev/1e9).toFixed(2)} млрд ₽</strong></div>
                </div>
                <div class="mck-channel-insight">
                    <div class="mck-channel-insight-title">📈 Инсайт McKinsey</div>
                    <p>E-com клиенты — <strong>${((snapEcom.ACTIVE_LFL?.AVG_CHECK || 1) / (snap.ACTIVE_LFL?.AVG_CHECK || 1)).toFixed(1)}x</strong> премиальный чек, но в <strong>${(offTotal / Math.max(ecomTotal,1)).toFixed(0)}x</strong> меньшая база.</p>
                    <p style="margin-top:8px">Конверсия 5% оффлайн-базы в e-com принесёт <strong style="color:#2E8B57">+${((offTotal*0.05 * (snapEcom.ACTIVE_LFL?.BUDGET || 0))/1e9).toFixed(2)} млрд ₽/мес</strong> при сохранении уровня ARPU.</p>
                </div>
            </div>
        </div>

        <!-- 5. Strategic Priorities -->
        <div class="card mck-card mck-priorities">
            <div class="card-header"><h3>🎯 ТОП-3 СТРАТЕГИЧЕСКИХ ПРИОРИТЕТА (McKinsey-вью)</h3></div>
            <div class="mck-priority-grid">
                <div class="mck-priority">
                    <div class="mck-priority-num">1</div>
                    <div class="mck-priority-title">Защита ядра LFL</div>
                    <div class="mck-priority-desc">
                        ${(segContrib[0].pctRevenue*100).toFixed(0)}% выручки от 1 сегмента — единая точка отказа.
                        Внедрить early-warning систему: triggers на изменение поведения за 30 дней до оттока.
                    </div>
                    <div class="mck-priority-impact">Impact: -1pp churn = +${((snap.ACTIVE_LFL?.CLIENTS||0)*0.01 * (snap.ACTIVE_LFL?.BUDGET||0)/1e6).toFixed(0)} М ₽/мес</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num">2</div>
                    <div class="mck-priority-title">Конверсия в Омни</div>
                    <div class="mck-priority-desc">
                        E-com база ${((ecomTotal/offTotal)*100).toFixed(1)}% от offline, но ARPU выше в ${((snapEcom.ACTIVE_LFL?.BUDGET||1) / (snap.ACTIVE_LFL?.BUDGET||1)).toFixed(1)}x.
                        Кампания "первый онлайн-заказ" с бесплатной доставкой ≥1500 ₽.
                    </div>
                    <div class="mck-priority-impact">Impact: 5% конверсии = +${(offTotal*0.05 * (snapEcom.ACTIVE_LFL?.BUDGET||0)/1e9).toFixed(2)} млрд ₽/мес</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num">3</div>
                    <div class="mck-priority-title">Миграция RANDOM → ACTIVE</div>
                    <div class="mck-priority-desc">
                        ${((snap.RANDOM?.CLIENTS||0)/1e3).toFixed(0)} тыс случайных клиентов — потенциал ${((snap.ACTIVE?.BUDGET||0) - (snap.RANDOM?.BUDGET||0)).toFixed(0)} ₽ ARPU-uplift.
                        Welcome-серия push'ей на 2-й месяц + бонус на любую категорию.
                    </div>
                    <div class="mck-priority-impact">Impact: 10% конверсии = +${((snap.RANDOM?.CLIENTS||0)*0.1 * ((snap.ACTIVE?.BUDGET||0)-(snap.RANDOM?.BUDGET||0))/1e6).toFixed(0)} М ₽/мес</div>
                </div>
            </div>
        </div>

        <!-- 6. Research recommendations -->
        <div class="card mck-card mck-research">
            <div class="card-header"><h3>🔬 Рекомендуемые дополнительные исследования</h3></div>
            <div class="mck-research-grid">
                <div class="mck-research-item">
                    <div class="mck-research-icon">🛒</div>
                    <div class="mck-research-title">Категорийная корзина по сегментам</div>
                    <div class="mck-research-desc">Анализ Market Basket: какие категории чаще всего покупают вместе. Кросс-сел рекомендации для каждого сегмента.</div>
                </div>
                <div class="mck-research-item">
                    <div class="mck-research-icon">📍</div>
                    <div class="mck-research-title">Геоаналитика магазинов</div>
                    <div class="mck-research-desc">Heat-map LFL по городам: где растёт чек, где падает частота. Локальные конкурентные угрозы.</div>
                </div>
                <div class="mck-research-item">
                    <div class="mck-research-icon">⏰</div>
                    <div class="mck-research-title">Cohort retention curves</div>
                    <div class="mck-research-desc">Когортный анализ удержания: % NEW → ACTIVE через 30/60/90 дней. Поиск точки отвала.</div>
                </div>
                <div class="mck-research-item">
                    <div class="mck-research-icon">💳</div>
                    <div class="mck-research-title">Чувствительность к скидке</div>
                    <div class="mck-research-desc">Эластичность: насколько меняется частота при -5% / -10% / -15% скидки. Optimal discount level per segment.</div>
                </div>
                <div class="mck-research-item">
                    <div class="mck-research-icon">🎯</div>
                    <div class="mck-research-title">Driver tree выручки</div>
                    <div class="mck-research-desc">Декомпозиция изменений ТО: рост базы vs ARPU vs чек vs частота. Какой драйвер критичен.</div>
                </div>
                <div class="mck-research-item">
                    <div class="mck-research-icon">🔄</div>
                    <div class="mck-research-title">Customer Lifetime Value</div>
                    <div class="mck-research-desc">CLV модель: ARPU × retention rate × срок жизни. Окупаемость CVM-инвестиций по сегментам.</div>
                </div>
                <div class="mck-research-item">
                    <div class="mck-research-icon">📊</div>
                    <div class="mck-research-title">Migration matrix между сегментами</div>
                    <div class="mck-research-desc">Транзитивная матрица: % клиентов перешедших NEW→ACTIVE→LFL→CHURN. Узкие места жизненного цикла.</div>
                </div>
                <div class="mck-research-item">
                    <div class="mck-research-icon">🏷️</div>
                    <div class="mck-research-title">Привлекательность СТМ</div>
                    <div class="mck-research-desc">Доля СТМ в чеке по сегментам. СТМ = маркер лояльности и драйвер маржи.</div>
                </div>
            </div>
        </div>
    `;
}

// --- Segment insights data ---
const SEG_INSIGHTS = {
    ACTIVE_LFL: {
        headline: 'Количество очень лояльных клиентов в базе растет. Частота визитов растет. Но они выбирают при покупке всё более дешёвые товары и снижают количество товаров в корзине',
        blocks: [
            {
                title: 'Клиентская база',
                tag: 'growth', tagText: 'РОСТ',
                points: [
                    () => `Стабильная доля ~56% базы — зрелый, предсказуемый сегмент, ядро сети`,
                    () => `Прирост клиентской базы <span class="metric-highlight green">${yoyPct('ACTIVE_LFL','CLIENTS')} YoY</span>`,
                    () => `Частота покупок растёт — клиенты становятся лояльнее`,
                ]
            },
            {
                title: 'Корзина и ценовое поведение',
                tag: 'warning', tagText: 'ВНИМАНИЕ',
                points: [
                    () => `SKU/чек снижается <span class="metric-highlight red">${yoyPct('ACTIVE_LFL','AVG_SKU')} YoY</span> — корзина беднеет`,
                    () => `ЦИ клиентов падает во 2 половине года, что вероятно связано с общим трендом на экономию в РФ`,
                    () => `Рост средней цены SKU <span class="metric-highlight orange">+12% YoY</span> обусловлен инфляцией`,
                ]
            },
            {
                title: 'ARPU и выручка',
                tag: 'warning', tagText: 'РИСК',
                points: [
                    () => `ARPU снижается <span class="metric-highlight red">${yoyPct('ACTIVE_LFL','BUDGET')} YoY</span> — эрозия ценности лояльных клиентов`,
                    () => `Средний чек растёт за счёт инфляции, но реальное потребление сокращается`,
                ]
            },
            {
                title: 'Промо-эффективность',
                tag: 'risk', tagText: 'КРИТИЧНО',
                points: [
                    () => `Скидка по карте выросла <span class="metric-highlight red">в 2 раза</span> — существенное увеличение промо-давления`,
                    () => `Реальная скидка растёт (с ~21% до ~24%) при снижающейся корзине — промо неэффективно`,
                ]
            },
        ],
        recs: [
            { title: 'Cross-sell', text: 'Увеличение SKU/чек через персонализированные рекомендации и комплементарные предложения' },
            { title: 'Промо-оптимизация', text: 'Переход от массовых скидок к таргетированным механикам. Анализ промо-эластичности' },
            { title: 'Удержание ядра', text: 'Программа retention через персонализированные предложения, не через увеличение скидки' },
        ]
    },
    ACTIVE: {
        headline: 'Клиентская база прирастала за счёт менее лояльных клиентов, с меньшим количеством позиций в чеке за счёт существенного увеличения скидки по карте',
        blocks: [
            {
                title: 'Рост базы',
                tag: 'growth', tagText: 'РОСТ',
                points: [
                    () => `Сегмент активно растёт: количество клиентов <span class="metric-highlight green">${yoyPct('ACTIVE','CLIENTS')} YoY</span>`,
                    () => `Доля от базы стабильна ~32% — второй по размеру сегмент`,
                ]
            },
            {
                title: 'Качество клиентов',
                tag: 'warning', tagText: 'ВНИМАНИЕ',
                points: [
                    () => `База прирастает менее лояльными клиентами — снижение кол-ва позиций в чеке <span class="metric-highlight red">${yoyPct('ACTIVE','AVG_SKU')} YoY</span>`,
                    () => `Средний чек растёт, но SKU/чек падает — покупают меньше товаров по более высоким ценам`,
                    () => `Увеличение частоты клиентов более высокого ценового сегмента`,
                ]
            },
            {
                title: 'Стоимость реактивации',
                tag: 'risk', tagText: 'КРИТИЧНО',
                points: [
                    () => `Скидка по карте выросла <span class="metric-highlight red">в 2 раза</span> (с ~2% до ~4%) — высокие затраты на реактивацию`,
                    () => `Необходимо выделить natural returnees — клиентов, которые вернулись бы без стимулов`,
                ]
            },
            {
                title: 'ARPU',
                tag: 'info', tagText: 'СТАБИЛЬНО',
                points: [
                    () => `ARPU остаётся стабильным <span class="metric-highlight orange">${yoyPct('ACTIVE','BUDGET')} YoY</span> — ценность клиента сохраняется`,
                ]
            },
        ],
        recs: [
            { title: 'ROI реактивации', text: 'Оптимизировать cost-to-reactivate. Выделить подсегменты по ROI и сократить промо для natural returnees' },
            { title: 'Конверсия в LFL', text: 'Программа конверсии активных в LFL через регулярные механики вовлечения' },
            { title: 'Корзина', text: 'Увеличение позиций в чеке через комплементарные предложения и бандлы' },
        ]
    },
    CHURN: {
        headline: 'Доля оттока незначительно увеличивалась в летние месяцы из-за увеличения случайных клиентов с более высоким ценовым сегментом. Динамика удержания позитивная.',
        blocks: [
            {
                title: 'Динамика оттока',
                tag: 'warning', tagText: 'ВНИМАНИЕ',
                points: [
                    () => `Доля оттока стабильна ~18%, но абсолютное число растёт <span class="metric-highlight red">${yoyPct('CHURN','CLIENTS')} YoY</span>`,
                    () => `Пиковый отток в июле-сентябре (19-20%) — сезонный паттерн, связанный с отпускным периодом`,
                    () => `Удержание клиентов незначительно снижается до 1%, связано с разбавлением базы менее лояльными клиентами`,
                ]
            },
            {
                title: 'Профиль оттока',
                tag: 'info', tagText: 'АНАЛИТИКА',
                points: [
                    () => `Ценовой индекс оттока выше всех сегментов (~0.31-0.32) — уходят самые "дорогие" покупатели`,
                    () => `ARPU оттока снижается <span class="metric-highlight red">${yoyPct('CHURN','BUDGET')} YoY</span> — уходят всё менее ценные клиенты`,
                ]
            },
            {
                title: 'Промо и удержание',
                tag: 'risk', tagText: 'НЕЭФФЕКТИВНО',
                points: [
                    () => `Скидка по карте для оттока выросла с ~2% до ~4-6% — промо не удерживает`,
                    () => `Рост промо-давления не конвертируется в снижение оттока`,
                ]
            },
            {
                title: 'Сезонность',
                tag: 'info', tagText: 'ПАТТЕРН',
                points: [
                    () => `Летний всплеск оттока (+2-3 п.п.) связан с отпускным периодом — ожидаемый и управляемый`,
                    () => `Зимнее снижение оттока — активизация промо перед Новым годом`,
                ]
            },
        ],
        recs: [
            { title: 'Early warning', text: 'Предиктивная модель оттока. Превентивные действия за 2-3 недели до ожидаемого ухода' },
            { title: 'Сегментация', text: 'Дифференцированные trigger-механики по ценности клиента. Не удерживать всех одинаково' },
            { title: 'Летний retention', text: 'Специальная программа на лето: накопительные бонусы, отложенные вознаграждения к сентябрю' },
        ]
    },
    RANDOM: {
        headline: 'Доля случайных клиентов и частота покупки увеличивалась за счёт увеличения товаров ДЦО. В апреле был всплеск привлечения клиентов высокого ценового сегмента.',
        blocks: [
            {
                title: 'Динамика сегмента',
                tag: 'growth', tagText: 'РОСТ',
                points: [
                    () => `Доля выросла с 7% до 8% <span class="metric-highlight orange">${yoyPct('RANDOM','SHARE_CLIENTS')} YoY</span> — больше нерегулярных визитов`,
                    () => `Рост частоты покупок связан с увеличением товаров ДЦО в ассортименте`,
                ]
            },
            {
                title: 'Профиль клиента',
                tag: 'info', tagText: 'АНАЛИТИКА',
                points: [
                    () => `Самый низкий ARPU (~1,300 руб.) и частота (~1.6 чека/мес) — минимальная вовлечённость`,
                    () => `Ценовой индекс самый высокий (~0.33) — покупают самые дорогие товары (импульсные покупки)`,
                    () => `В апреле всплеск привлечения клиентов высокого ценового сегмента`,
                ]
            },
            {
                title: 'Реакция на промо',
                tag: 'warning', tagText: 'НИЗКАЯ',
                points: [
                    () => `Реальная скидка ниже других сегментов (~19-20%) — слабо реагируют на промо`,
                    () => `Redemption минимальный — программа лояльности не вовлекает`,
                ]
            },
            {
                title: 'Ценовой сегмент',
                tag: 'info', tagText: 'НАБЛЮДЕНИЕ',
                points: [
                    () => `Самые лояльные клиенты (core сети) — это клиенты более низкого ценового сегмента`,
                    () => `Случайно в сеть заходят клиенты более высокого ценового сегмента`,
                ]
            },
        ],
        recs: [
            { title: 'Конверсия', text: 'Фокус на конверсию в "активных" через welcome-цепочки после 2-й покупки' },
            { title: 'Не удерживать', text: 'Не инвестировать в удержание случайных. ROI промо для этого сегмента отрицательный' },
            { title: 'Proximity', text: 'Триггерные push-уведомления при proximity к магазину для увеличения частоты визитов' },
        ]
    },
    NEW: {
        headline: 'Увеличивается приток новых клиентов в базу. Основные миссии первой закупки: сезонные/ситуативные товары, алкоголь, детские товары, напитки и готовые блюда.',
        blocks: [
            {
                title: 'Приток новых',
                tag: 'warning', tagText: 'ЗАМЕДЛЕНИЕ',
                points: [
                    () => `Доля новых клиентов снижается <span class="metric-highlight red">${yoyPct('NEW','SHARE_CLIENTS')} YoY</span> — воронка сужается`,
                    () => `Пики в ноябре-декабре (сезонность) — новогодние покупки привлекают новичков`,
                ]
            },
            {
                title: 'Ценность новичка',
                tag: 'growth', tagText: 'ПОТЕНЦИАЛ',
                points: [
                    () => `ARPU новых (~2,400 руб.) — второй после LFL, хорошее первое впечатление`,
                    () => `Средний чек сопоставим с другими сегментами — потенциал для развития`,
                ]
            },
            {
                title: 'Вовлечение',
                tag: 'risk', tagText: 'КРИТИЧНО',
                points: [
                    () => `Redemption крайне низкий (~5-6%) vs LFL (~60%) — программа лояльности не вовлекает новичков`,
                    () => `SKU/чек снижается <span class="metric-highlight red">${yoyPct('NEW','AVG_SKU')} YoY</span> — формируют узкие корзины`,
                ]
            },
            {
                title: 'Миссии первой покупки',
                tag: 'info', tagText: 'АНАЛИТИКА',
                points: [
                    () => `Алкоголь: праздники и подарки (23 февраля, 8 марта, Новый год) — экспресс-покупка`,
                    () => `Детские товары: life event — появление ребёнка привлекает в ближайший магазин`,
                    () => `Перекус и готовая еда — молодая аудитория, кофе на ходу`,
                ]
            },
        ],
        recs: [
            { title: 'Onboarding', text: 'Приветственные бонусы с дедлайном. Персонализация на основе первой покупки. Цель: NEW → ACTIVE за 30 дней' },
            { title: 'Redemption', text: 'Увеличить Redemption с 5% до 20%+ через упрощение механики и instant-награды' },
            { title: 'Life events', text: 'Партнёрства с роддомами/ЗАГСами — сертификаты при регистрации ребёнка' },
        ]
    },
};

// --- Segment Page Builder ---
function buildSegmentPage(seg) {
    const container = document.getElementById('tab-' + seg.toLowerCase());
    if (container.dataset.built) return;
    container.dataset.built = '1';

    const requestedYm = getSelectedYm();
    const ym = getSnapYm(requestedYm);
    const last12 = getLast12Months(ym);
    const xLabels = getMonthLabels(last12);
    const snap = getSnap(ym);
    const prevYm = getPrevYearYm(ym);
    const prevSnap = getSnap(prevYm);
    const curLabel = YM_LABELS[ym];
    const prevLast12 = last12.map(m => {
        const y = parseInt(m.slice(0, 4)) - 1;
        return '' + y + m.slice(4);
    });
    const prevXLabels = getMonthLabels(prevLast12);

    const color = SEG_COLORS[seg];
    const name = SEG_RU[seg];
    const descs = {
        CHURN: 'Клиенты без покупок в отчетный период, но есть хотя бы 1 покупка в прошлом месяце',
        RANDOM: 'Клиенты с перерывами между покупками более 5 недель за последний год',
        NEW: 'Первая покупка в отчетный период',
        ACTIVE_LFL: 'Есть покупки в отчетный период и в аналогичный период прошлого года',
        ACTIVE: 'Есть покупки в отчетный период, нет покупок в аналогичный период прошлого года',
    };

    const kpiMetrics = [
        ['SHARE_CLIENTS', 'Доля от базы', 'pct'],
        ['CLIENTS', 'Клиентов', 'int'],
        ['BUDGET', 'ARPU', 'rub'],
        ['AVG_CHECK', 'Ср. чек', 'rub'],
        ['COUNT_CHECK', 'Чеков/клиента', 'dec'],
        ['AVG_SKU', 'SKU/чек', 'dec'],
        ['SALE', 'Скидка по карте', 'pct'],
        ['REAL_SALE', 'Реал. скидка', 'pct'],
    ];

    const chartConfigs = [
        { id: `seg-${seg}-share`, title: 'Доля от базы', metric: 'SHARE_CLIENTS', yFmt: v => (v*100).toFixed(1)+'%', type: 'line' },
        { id: `seg-${seg}-clients`, title: 'Количество клиентов', metric: 'CLIENTS', yFmt: v => Math.round(v).toLocaleString('ru-RU'), type: 'bar' },
        { id: `seg-${seg}-arpu`, title: 'ARPU, руб.', metric: 'BUDGET', yFmt: v => Math.round(v).toLocaleString('ru-RU'), type: 'line' },
        { id: `seg-${seg}-check`, title: 'Средний чек, руб.', metric: 'AVG_CHECK', yFmt: v => Math.round(v).toLocaleString('ru-RU'), type: 'line' },
        { id: `seg-${seg}-freq`, title: 'Чеков/клиента', metric: 'COUNT_CHECK', yFmt: v => v.toFixed(2), type: 'line' },
        { id: `seg-${seg}-sku`, title: 'SKU / чек', metric: 'AVG_SKU', yFmt: v => v.toFixed(2), type: 'line' },
        { id: `seg-${seg}-discount`, title: 'Скидки', metric: null, type: 'multi-discount' },
        { id: `seg-${seg}-price`, title: 'Ценовой индекс', metric: 'PRICE_INDEX', yFmt: v => v.toFixed(3), type: 'line' },
    ];

    // Build insight HTML
    const ins = SEG_INSIGHTS[seg];
    const insightHTML = ins ? `
        <div class="segment-insight" style="border-left-color:${color}">
            <div class="segment-insight-header">
                <div class="icon" style="background:${color};color:#fff">!</div>
                <h3>Выводы и рекомендации</h3>
            </div>
            <div style="padding:14px 20px;background:#fafafa;border-bottom:1px solid var(--border);font-size:14px;font-weight:600;color:var(--text);line-height:1.5">
                ${ins.headline}
            </div>
            <div class="insight-grid">
                ${ins.blocks.map(b => `
                    <div class="insight-block">
                        <div class="insight-block-title">
                            <span class="tag tag-${b.tag}">${b.tagText}</span>
                            ${b.title}
                        </div>
                        <ul>${b.points.map(p => `<li>${p()}</li>`).join('')}</ul>
                    </div>
                `).join('')}
            </div>
            <div class="insight-rec-block">
                <div class="rec-title">Рекомендации</div>
                <div class="rec-items">
                    ${ins.recs.map(r => `
                        <div class="rec-item">
                            <strong>${r.title}</strong>
                            ${r.text}
                        </div>
                    `).join('')}
                </div>
            </div>
        </div>
    ` : '';

    container.innerHTML = `
        <div class="page-header">
            <div>
                <div class="segment-header">
                    <div class="segment-dot" style="background:${color}"></div>
                    <h1>${name}</h1>
                </div>
                <p class="segment-desc">${descs[seg]}</p>
            </div>
            <div class="header-meta">
                ${renderGlobalChannelToggle()}
                <span class="meta-badge" style="background:${color}">${curLabel}</span>
            </div>
        </div>

        ${requestedYm !== ym ? `<div class="snap-fallback-note">Данные по сегментам доступны по ${YM_LABELS[ym]} — показатели ниже за этот месяц.</div>` : ''}

        <div class="segment-kpi-grid">
            ${kpiMetrics.map(([col, label, type]) => {
                const val = snap[seg]?.[col];
                const change = snapChange(snap, prevSnap, seg, col);
                return `<div class="segment-kpi" style="border-top: 3px solid ${color}">
                    <div class="kpi-label">${label}</div>
                    <div class="kpi-value">${fmtVal(val, type)}</div>
                    ${change != null ? `<div class="kpi-change ${change >= 0 ? 'up' : 'down'}">${change >= 0 ? '+' : ''}${(change*100).toFixed(1)}% YoY</div>` : ''}
                    <div class="kpi-sub">${curLabel}</div>
                </div>`;
            }).join('')}
        </div>

        ${[0,2,4,6].map(i => `
            <div class="charts-row">
                ${chartConfigs.slice(i, i+2).map(c => `
                    <div class="card">
                        <div class="card-header"><h3>${c.title}</h3></div>
                        <div class="chart-container"><canvas id="${c.id}"></canvas></div>
                    </div>
                `).join('')}
            </div>
        `).join('')}

        ${insightHTML}
    `;

    // Render charts with last 12 months
    setTimeout(() => {
        const curData = metric => getMetricSeries(seg, metric, last12);
        const prevData = metric => getMetricSeries(seg, metric, prevLast12);
        const firstLbl = YM_LABELS[last12[0]] || '';
        const lastLbl = YM_LABELS[last12[last12.length - 1]] || '';
        const prevFirstLbl = YM_LABELS[prevLast12[0]] || '';
        const prevLastLbl = YM_LABELS[prevLast12[prevLast12.length - 1]] || '';
        const curPeriod = firstLbl + '–' + lastLbl;
        const prevPeriod = prevFirstLbl + '–' + prevLastLbl;

        chartConfigs.forEach(cfg => {
            const canvas = document.getElementById(cfg.id);
            if (!canvas) return;

            if (cfg.type === 'multi-discount') {
                chartInstances[cfg.id] = new Chart(canvas, {
                    type: 'line',
                    data: {
                        labels: xLabels,
                        datasets: [
                            { label: 'Карта (пред.)', data: prevData('SALE'), borderColor: CHART_GRAY, borderDash: [6,3], borderWidth: 2, fill: false, pointRadius: 2 },
                            { label: 'Карта (тек.)', data: curData('SALE'), borderColor: CHART_ORANGE, borderWidth: 2.5, fill: false },
                            { label: 'Реал. (пред.)', data: prevData('REAL_SALE'), borderColor: CHART_PURPLE, borderDash: [6,3], borderWidth: 2, fill: false, pointRadius: 2 },
                            { label: 'Реал. (тек.)', data: curData('REAL_SALE'), borderColor: '#C41E3A', borderWidth: 2.5, fill: false },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: v => (v*100).toFixed(1)+'%' }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: { legend: { position: 'top', align: 'start' } }
                    }
                });
                return;
            }

            const dCur = curData(cfg.metric);
            const dPrev = prevData(cfg.metric);

            if (cfg.type === 'bar') {
                chartInstances[cfg.id] = new Chart(canvas, {
                    type: 'bar',
                    data: {
                        labels: xLabels,
                        datasets: [
                            { label: prevPeriod, data: dPrev, backgroundColor: '#e0e0e0', borderRadius: 4 },
                            { label: curPeriod, data: dCur, backgroundColor: color, borderRadius: 4 },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: cfg.yFmt }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: { legend: { position: 'top', align: 'start' } }
                    }
                });
            } else {
                chartInstances[cfg.id] = new Chart(canvas, {
                    type: 'line',
                    data: {
                        labels: xLabels,
                        datasets: [
                            { label: prevPeriod, data: dPrev, borderColor: CHART_GRAY, borderDash: [6,3], borderWidth: 2, fill: false, pointRadius: 2 },
                            { label: curPeriod, data: dCur, borderColor: color, borderWidth: 2.5, fill: false },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: cfg.yFmt }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: { legend: { position: 'top', align: 'start' } }
                    }
                });
            }
        });
    }, 50);
}

// --- GCG (Global Control Group) Page ---
function buildGCG(selectedYm) {
    if (typeof GCG_DATA === 'undefined' || !GCG_DATA.length) return;

    const container = document.getElementById('tab-gcg');
    const months = [...new Set(GCG_DATA.map(d => d.ym))].sort((a,b) => b-a);
    const latestYm = selectedYm || months[0];
    const latestLabel = GCG_DATA.find(d => d.ym === latestYm)?.ym_label || '';

    // Month selector options
    const monthOpts = months.map(ym => {
        const lbl = GCG_DATA.find(d => d.ym === ym)?.ym_label || ym;
        return `<option value="${ym}"${ym === latestYm ? ' selected' : ''}>${lbl}</option>`;
    }).join('');

    // Get data for selected month
    const active = GCG_DATA.find(d => d.ym === latestYm && d.segment === 'Активные') || {};
    const churn = GCG_DATA.find(d => d.ym === latestYm && d.segment === 'Отток') || {};

    const totalAddTO = (active.add_to || 0) + (churn.add_to || 0);
    const arpuLiftActive = active.arpu_gcg ? ((active.arpu_cg - active.arpu_gcg) / active.arpu_gcg * 100) : 0;
    const arpuLiftChurn = churn.arpu_gcg ? ((churn.arpu_cg - churn.arpu_gcg) / churn.arpu_gcg * 100) : 0;
    const checkLiftActive = active.avgcheck_gcg ? ((active.avgcheck_cg - active.avgcheck_gcg) / active.avgcheck_gcg * 100) : 0;

    function fmtM(v) { return (v / 1e6).toFixed(1) + 'M ₽'; }
    function fmtR(v) { return Math.round(v).toLocaleString('ru-RU') + ' ₽'; }
    function fmtPct(v) { return (v >= 0 ? '+' : '') + v.toFixed(1) + '%'; }

    container.innerHTML = `
        <div class="page-header">
            <div>
                <h1>Глобальная контрольная группа</h1>
                <p class="subtitle">Оценка эффекта CVM-коммуникаций vs контрольная группа</p>
            </div>
            <div class="header-meta"><select id="gcg-month-sel" class="month-selector">${monthOpts}</select></div>
        </div>

        <!-- KPI Cards -->
        <div class="kpi-grid" style="grid-template-columns: repeat(4, 1fr);">
            <div class="kpi-card" style="border-top-color: var(--dixy-orange)">
                <div class="kpi-label">ДОП. ТО (ОБЩИЙ)</div>
                <div class="kpi-value">${fmtM(totalAddTO)}</div>
                <div class="kpi-sub">${latestLabel}</div>
            </div>
            <div class="kpi-card" style="border-top-color: #4A90D9">
                <div class="kpi-label">ДОП. ТО АКТИВНЫЕ</div>
                <div class="kpi-value">${fmtM(active.add_to || 0)}</div>
                <div class="kpi-sub">${latestLabel}</div>
            </div>
            <div class="kpi-card" style="border-top-color: #C41E3A">
                <div class="kpi-label">ДОП. ТО ОТТОК</div>
                <div class="kpi-value">${fmtM(churn.add_to || 0)}</div>
                <div class="kpi-sub">${latestLabel}</div>
            </div>
            <div class="kpi-card" style="border-top-color: #2E8B57">
                <div class="kpi-label">LIFT ARPU АКТИВНЫЕ</div>
                <div class="kpi-value">${fmtPct(arpuLiftActive)}</div>
                <div class="kpi-sub">ЦГ vs ГКГ</div>
            </div>
        </div>

        <!-- Charts row 1: Add TO by month + ARPU comparison -->
        <div class="grid-2" style="margin-top:16px">
            <div class="card">
                <div class="card-header"><h3>Доп. ТО по месяцам</h3></div>
                <div style="height:300px"><canvas id="gcg-addto"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>ARPU: ЦГ vs ГКГ</h3></div>
                <div style="height:300px"><canvas id="gcg-arpu"></canvas></div>
            </div>
        </div>

        <!-- Charts row 2: Avg check comparison + Lift dynamics -->
        <div class="grid-2" style="margin-top:16px">
            <div class="card">
                <div class="card-header"><h3>Ср. чек: ЦГ vs ГКГ</h3></div>
                <div style="height:300px"><canvas id="gcg-avgcheck"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>Lift ARPU по месяцам, %</h3></div>
                <div style="height:300px"><canvas id="gcg-lift"></canvas></div>
            </div>
        </div>

        <!-- Summary table -->
        <div class="card" style="margin-top:16px">
            <div class="card-header"><h3>Сводная таблица ГКГ</h3></div>
            <div class="table-wrap">
                <table class="data-table" id="gcg-table"></table>
            </div>
        </div>

        <!-- Insights -->
        <div class="card" style="margin-top:16px" id="gcg-insights"></div>
    `;

    // Build summary table
    const table = document.getElementById('gcg-table');
    table.innerHTML = `
        <thead><tr>
            <th rowspan="2">Месяц</th>
            <th rowspan="2">Сегмент</th>
            <th colspan="2">Клиенты</th>
            <th colspan="3">ARPU</th>
            <th colspan="3">Ср. чек</th>
            <th rowspan="2">Доп. ТО</th>
        </tr><tr>
            <th>ЦГ</th><th>ГКГ</th>
            <th>ЦГ</th><th>ГКГ</th><th>Lift</th>
            <th>ЦГ</th><th>ГКГ</th><th>Lift</th>
        </tr></thead>
        <tbody>
            ${months.map(ym => {
                return ['Активные','Отток'].map(seg => {
                    const d = GCG_DATA.find(r => r.ym === ym && r.segment === seg) || {};
                    const arpuLift = d.arpu_gcg ? ((d.arpu_cg - d.arpu_gcg) / d.arpu_gcg * 100) : 0;
                    const checkLift = d.avgcheck_gcg ? ((d.avgcheck_cg - d.avgcheck_gcg) / d.avgcheck_gcg * 100) : 0;
                    return '<tr>' +
                        '<td>' + (d.ym_label || '') + '</td>' +
                        '<td>' + seg + '</td>' +
                        '<td>' + (d.clients_cg || 0).toLocaleString('ru-RU') + '</td>' +
                        '<td>' + (d.clients_gcg || 0).toLocaleString('ru-RU') + '</td>' +
                        '<td>' + fmtR(d.arpu_cg || 0) + '</td>' +
                        '<td>' + fmtR(d.arpu_gcg || 0) + '</td>' +
                        '<td class="' + (arpuLift >= 0 ? 'up' : 'down') + '">' + fmtPct(arpuLift) + '</td>' +
                        '<td>' + fmtR(d.avgcheck_cg || 0) + '</td>' +
                        '<td>' + fmtR(d.avgcheck_gcg || 0) + '</td>' +
                        '<td class="' + (checkLift >= 0 ? 'up' : 'down') + '">' + fmtPct(checkLift) + '</td>' +
                        '<td>' + fmtM(d.add_to || 0) + '</td>' +
                    '</tr>';
                }).join('');
            }).join('')}
        </tbody>
    `;

    // Charts
    const sortedMonths = [...months].sort();
    const mLabels = sortedMonths.map(ym => GCG_DATA.find(d => d.ym === ym)?.ym_label || '');

    // Add TO bar chart (stacked: Active + Churn)
    destroyChart('gcg-addto');
    chartInstances['gcg-addto'] = new Chart(document.getElementById('gcg-addto'), {
        type: 'bar',
        data: {
            labels: mLabels,
            datasets: [
                {
                    label: 'Активные',
                    data: sortedMonths.map(ym => { const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Активные'); return d ? d.add_to / 1e6 : 0; }),
                    backgroundColor: UNIFIED_SEG_COLORS['Активные'], borderRadius: 4
                },
                {
                    label: 'Отток',
                    data: sortedMonths.map(ym => { const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Отток'); return d ? d.add_to / 1e6 : 0; }),
                    backgroundColor: UNIFIED_SEG_COLORS['Отток'], borderRadius: 4
                }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            layout: { padding: { top: 20 } },
            scales: { x: { stacked: true, ticks: { font: { size: 11 } } }, y: { stacked: true, ticks: { callback: v => v + 'M' } } },
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, font: { size: 10 }, padding: 12 } },
                datalabels: {
                    display: ctx => ctx.datasetIndex === 1,
                    anchor: 'end', align: 'end', offset: 2,
                    font: { size: 11, weight: '700' }, color: '#333',
                    formatter: (v, ctx) => {
                        const total = ctx.chart.data.datasets.reduce((s, ds) => s + (ds.data[ctx.dataIndex] || 0), 0);
                        return total.toFixed(1) + 'M';
                    }
                }
            }
        },
        plugins: [ChartDataLabels]
    });

    // ARPU grouped bar: ЦГ vs ГКГ for each month (Active segment only)
    destroyChart('gcg-arpu');
    chartInstances['gcg-arpu'] = new Chart(document.getElementById('gcg-arpu'), {
        type: 'bar',
        data: {
            labels: mLabels,
            datasets: [
                {
                    label: 'ЦГ (с CVM)',
                    data: sortedMonths.map(ym => { const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Активные'); return d?.arpu_cg || 0; }),
                    backgroundColor: '#C67A2E', borderRadius: 4
                },
                {
                    label: 'ГКГ (без CVM)',
                    data: sortedMonths.map(ym => { const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Активные'); return d?.arpu_gcg || 0; }),
                    backgroundColor: '#D4C4A0', borderRadius: 4
                }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            layout: { padding: { top: 20 } },
            scales: { y: { ticks: { callback: v => v.toLocaleString('ru-RU') + ' ₽' } }, x: { ticks: { font: { size: 11 } } } },
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, font: { size: 10 }, padding: 12 } },
                datalabels: {
                    anchor: 'end', align: 'end', offset: 2,
                    font: { size: 10, weight: '600' }, color: '#333',
                    formatter: v => v.toLocaleString('ru-RU')
                }
            }
        },
        plugins: [ChartDataLabels]
    });

    // Avg check grouped bar
    destroyChart('gcg-avgcheck');
    chartInstances['gcg-avgcheck'] = new Chart(document.getElementById('gcg-avgcheck'), {
        type: 'bar',
        data: {
            labels: mLabels,
            datasets: [
                {
                    label: 'ЦГ (с CVM)',
                    data: sortedMonths.map(ym => { const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Активные'); return d?.avgcheck_cg || 0; }),
                    backgroundColor: '#C67A2E', borderRadius: 4
                },
                {
                    label: 'ГКГ (без CVM)',
                    data: sortedMonths.map(ym => { const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Активные'); return d?.avgcheck_gcg || 0; }),
                    backgroundColor: '#D4C4A0', borderRadius: 4
                }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            layout: { padding: { top: 20 } },
            scales: { y: { ticks: { callback: v => v + ' ₽' } }, x: { ticks: { font: { size: 11 } } } },
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, font: { size: 10 }, padding: 12 } },
                datalabels: {
                    anchor: 'end', align: 'end', offset: 2,
                    font: { size: 10, weight: '600' }, color: '#333',
                    formatter: v => Math.round(v).toLocaleString('ru-RU')
                }
            }
        },
        plugins: [ChartDataLabels]
    });

    // Lift line chart (both segments)
    destroyChart('gcg-lift');
    chartInstances['gcg-lift'] = new Chart(document.getElementById('gcg-lift'), {
        type: 'line',
        data: {
            labels: mLabels,
            datasets: [
                {
                    label: 'Lift Активные',
                    data: sortedMonths.map(ym => {
                        const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Активные');
                        return d?.arpu_gcg ? +((d.arpu_cg - d.arpu_gcg) / d.arpu_gcg * 100).toFixed(2) : null;
                    }),
                    borderColor: UNIFIED_SEG_COLORS['Активные'], tension: 0.3, pointRadius: 5, borderWidth: 2.5, fill: false
                },
                {
                    label: 'Lift Отток',
                    data: sortedMonths.map(ym => {
                        const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Отток');
                        return d?.arpu_gcg ? +((d.arpu_cg - d.arpu_gcg) / d.arpu_gcg * 100).toFixed(2) : null;
                    }),
                    borderColor: UNIFIED_SEG_COLORS['Отток'], tension: 0.3, pointRadius: 5, borderWidth: 2.5, fill: false
                }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            layout: { padding: { top: 20 } },
            scales: { y: { ticks: { callback: v => v + '%' } }, x: { ticks: { font: { size: 11 } } } },
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, font: { size: 10 }, padding: 12 } },
                datalabels: {
                    display: true, color: ctx => ctx.dataset.borderColor,
                    font: { size: 11, weight: '700' },
                    anchor: 'end', align: 'top', offset: 4,
                    formatter: v => v != null ? v.toFixed(1) + '%' : ''
                }
            }
        },
        plugins: [ChartDataLabels]
    });

    // Insights
    const insightsEl = document.getElementById('gcg-insights');
    const totalAddTOAll = GCG_DATA.reduce((s, d) => s + (d.add_to || 0), 0);
    const avgLiftActive = months.reduce((s, ym) => {
        const d = GCG_DATA.find(r => r.ym === ym && r.segment === 'Активные');
        return s + (d?.arpu_gcg ? (d.arpu_cg - d.arpu_gcg) / d.arpu_gcg * 100 : 0);
    }, 0) / months.length;

    insightsEl.innerHTML = `
        <div class="card-header"><h3>Ключевые выводы — ГКГ</h3></div>
        <div style="padding:16px; font-size:13px; line-height:1.7; color:#333">
            <p><strong>1. Эффект CVM-коммуникаций подтверждён:</strong> дополнительный ТО за ${months.length} мес. составил <strong>${(totalAddTOAll/1e6).toFixed(0)}M ₽</strong>. CVM-кампании генерируют измеримый прирост выручки по сравнению с контрольной группой без коммуникаций.</p>
            <p><strong>2. ARPU Lift для Активных:</strong> средний lift <strong>${avgLiftActive.toFixed(1)}%</strong>. Клиенты, получающие CVM-коммуникации, тратят на ${avgLiftActive.toFixed(1)}% больше, чем аналогичные клиенты без коммуникаций. Эффект стабилен из месяца в месяц.</p>
            <p><strong>3. Отток — CVM работает на удержание:</strong> lift ARPU для сегмента «Отток» составляет ${arpuLiftChurn.toFixed(1)}%, что подтверждает эффективность реактивационных кампаний. CVM возвращает часть оттока в активную покупательскую базу.</p>
            <p><strong>4. Ср. чек:</strong> lift ${checkLiftActive.toFixed(1)}% для активных. Персонализированные предложения увеличивают не только частоту, но и размер покупки.</p>
            <p><strong>5. Стратегическая рекомендация:</strong> расширение CVM-покрытия до 100% активной базы (сейчас ЦГ: ${(active.clients_cg/1e6).toFixed(1)}M vs ГКГ: ${(active.clients_gcg/1e3).toFixed(0)}K) может увеличить годовой дополнительный ТО на ${((totalAddTOAll/months.length*12)/1e9).toFixed(1)} млрд ₽.</p>
        </div>
    `;

    // Month selector event
    document.getElementById('gcg-month-sel')?.addEventListener('change', function() {
        buildGCG(parseInt(this.value));
    });
}

// --- Insights Page ---
function buildInsights() {
    const container = document.getElementById('insights-container');

    const insights = [
        {
            num: 1, color: CHART_ORANGE, title: 'АКТИВНЫЕ LFL \u2014 ЯДРО БАЗЫ',
            subtitle: '56% клиентской базы',
            points: [
                'Стабильная доля ~56% базы без значимой динамики YoY \u2014 зрелый, предсказуемый сегмент',
                `ARPU снижается на ${yoyPct('ACTIVE_LFL','BUDGET')} YoY \u2014 сигнал эрозии ценности лояльных клиентов`,
                `SKU/чек сократилось на ${yoyPct('ACTIVE_LFL','AVG_SKU')} \u2014 корзина становится беднее`,
                'Реальная скидка растёт (с ~21% до ~24%) при снижающейся корзине \u2014 промо-давление неэффективно',
            ],
            rec: 'Программа удержания ядра через персонализированные предложения и cross-sell. Фокус на увеличение SKU/чек через рекомендательные механики.'
        },
        {
            num: 2, color: UNIFIED_SEG_COLORS['Активные'], title: 'АКТИВНЫЕ (РЕАКТИВИРОВАННЫЕ)',
            subtitle: '32% базы',
            points: [
                `Количество клиентов увеличилось на ${yoyPct('ACTIVE','CLIENTS')} YoY`,
                'Скидка по карте выросла x2 (с ~2% до ~4%) \u2014 высокие затраты на реактивацию',
                'SKU/чек падает \u2014 покупают меньше товаров по более высоким ценам',
            ],
            rec: 'Оптимизировать cost-to-reactivate. Выделить подсегменты по ROI реактивации.'
        },
        {
            num: 3, color: '#C41E3A', title: 'ОТТОК',
            subtitle: '17-20% базы',
            points: [
                `Абсолютное число растёт ${yoyPct('CHURN','CLIENTS')} YoY`,
                'Пиковый отток в июле-сентябре \u2014 сезонный паттерн',
                'Ценовой индекс оттока выше всех сегментов \u2014 уходят дорогие покупатели',
            ],
            rec: 'Предиктивная модель оттока. Превентивные действия за 2-3 недели до ожидаемого ухода.'
        },
        {
            num: 4, color: CHART_ORANGE, title: 'СЛУЧАЙНЫЕ',
            subtitle: '8% базы',
            points: [
                'Самый низкий ARPU и частота \u2014 минимальная вовлечённость',
                'Ценовой индекс самый высокий \u2014 импульсные покупки',
                'Рост частоты связан с увеличением товаров ДЦО',
            ],
            rec: 'Не инвестировать в удержание. Фокус на конверсию в "активных" через welcome-цепочки.'
        },
        {
            num: 5, color: CHART_GREEN, title: 'НОВЫЕ',
            subtitle: '4% базы',
            points: [
                `Доля снижается ${yoyPct('NEW','SHARE_CLIENTS')} YoY \u2014 воронка сужается`,
                'Redemption ~5-6% vs LFL ~60% \u2014 программа не вовлекает новичков',
                'Основные миссии: алкоголь к праздникам, детские товары, перекус',
            ],
            rec: 'Onboarding с дедлайном. Цель: NEW \u2192 ACTIVE за 30 дней. Партнёрства для life events.'
        },
    ];

    container.innerHTML = insights.map(ins => `
        <div class="insight-card" style="border-left-color:${ins.color}">
            <div class="insight-header">
                <div class="insight-num" style="background:${ins.color}">${ins.num}</div>
                <div class="insight-title">${ins.title}</div>
                <div class="insight-subtitle">${ins.subtitle}</div>
            </div>
            <div class="insight-body">
                <ul>${ins.points.map(p => `<li>${p}</li>`).join('')}</ul>
                <div class="insight-rec">
                    <div class="insight-rec-title">Рекомендация</div>
                    <p>${ins.rec}</p>
                </div>
            </div>
        </div>
    `).join('') + `
        <div class="insight-card priorities-card" style="border-left-color:${CHART_ORANGE}">
            <div class="insight-header">
                <div class="insight-num" style="background:${CHART_ORANGE}">!</div>
                <div class="insight-title">TOP-3 СТРАТЕГИЧЕСКИХ ПРИОРИТЕТА</div>
            </div>
            <div class="insight-body">
                <div style="margin-top:8px">
                    <div class="priority-item">
                        <div class="priority-num">1</div>
                        <div class="priority-text"><strong>Защитить ядро (LFL):</strong> программа retention + cross-sell для увеличения SKU/чек</div>
                    </div>
                    <div class="priority-item">
                        <div class="priority-num">2</div>
                        <div class="priority-text"><strong>Оптимизировать промо-ROI:</strong> сократить нерентабельные промо, усилить персонализацию</div>
                    </div>
                    <div class="priority-item">
                        <div class="priority-num">3</div>
                        <div class="priority-text"><strong>Улучшить воронку NEW \u2192 ACTIVE:</strong> onboarding + early engagement программы</div>
                    </div>
                </div>
            </div>
        </div>
    `;
}

// --- Definitions Page ---
const SEG_DEFINITIONS = {
    NEW: {
        icon: '🆕',
        title: 'Новые клиенты',
        short: 'Клиенты, совершившие первую покупку в отчётном периоде',
        criteria: [
            'Первая транзакция по карте лояльности зафиксирована в текущем месяце',
            'Отсутствие истории покупок за предшествующие 12 месяцев',
            'Регистрация в программе лояльности может быть ранее, но активация — в отчётном периоде'
        ],
        keyMetrics: 'Частота покупок, средний чек первой покупки, конверсия в повторную покупку',
        strategy: 'Онбординг, welcome-коммуникации, стимулирование второй покупки для закрепления привычки'
    },
    ACTIVE: {
        icon: '🔵',
        title: 'Активные клиенты',
        short: 'Клиенты с регулярной покупательской активностью, но не в каждый месяц',
        criteria: [
            'Совершали покупки в текущем и предыдущем периодах',
            'Частота визитов ниже, чем у LFL-сегмента',
            'Могут иметь перерывы между покупками до 2 месяцев'
        ],
        keyMetrics: 'ARPU, частота чеков, средний чек, доля в выручке',
        strategy: 'Увеличение частоты покупок, рост среднего чека, персональные предложения'
    },
    ACTIVE_LFL: {
        icon: '🟢',
        title: 'Активные LFL (Ядро)',
        short: 'Самые лояльные клиенты с покупками каждый месяц — ядро клиентской базы',
        criteria: [
            'Покупки совершаются стабильно каждый месяц (like-for-like)',
            'Присутствуют в базе не менее 12 месяцев подряд',
            'Наивысшая частота визитов и ARPU среди всех сегментов'
        ],
        keyMetrics: 'Удержание, ARPU, Redemption (погашение бонусов), индекс лояльности',
        strategy: 'Защита ядра, предотвращение оттока, программы признания, эксклюзивные привилегии'
    },
    RANDOM: {
        icon: '🟠',
        title: 'Случайные клиенты',
        short: 'Клиенты с нерегулярными, эпизодическими покупками',
        criteria: [
            'Единичные покупки с длительными перерывами (более 2 месяцев)',
            'Низкая частота визитов (1–2 за период)',
            'Низкая привязанность к сети — вероятно, покупают и у конкурентов'
        ],
        keyMetrics: 'Частота визитов, конверсия в активный сегмент, ценовой индекс',
        strategy: 'Геотаргетированные предложения, стимулирование повторных визитов, ценовая конкуренция'
    },
    CHURN: {
        icon: '🔴',
        title: 'Отток',
        short: 'Клиенты, прекратившие покупки в отчётном периоде',
        criteria: [
            'Отсутствие покупок за последние 3+ месяца',
            'Ранее были классифицированы как Активные или LFL',
            'Бонусный баланс может быть ненулевым (неиспользованные баллы)'
        ],
        keyMetrics: 'Доля оттока, последний ARPU до ухода, потенциал реактивации',
        strategy: 'Win-back кампании, анализ причин ухода, реактивационные предложения с ограниченным сроком'
    }
};

function buildDefinitions() {
    const container = document.getElementById('definitions-container');
    container.innerHTML = SEGMENTS.map(seg => {
        const d = SEG_DEFINITIONS[seg];
        return `
        <div class="card def-card" id="def-${seg}" style="border-left: 4px solid ${SEG_COLORS[seg]}">
            <div class="def-header">
                <span class="def-icon">${d.icon}</span>
                <div>
                    <h3 class="def-title">${d.title}</h3>
                    <p class="def-short">${d.short}</p>
                </div>
            </div>
            <div class="def-body">
                <div class="def-section">
                    <div class="def-section-title">Критерии отнесения</div>
                    <ul class="def-list">${d.criteria.map(c => `<li>${c}</li>`).join('')}</ul>
                </div>
                <div class="def-section">
                    <div class="def-section-title">Ключевые метрики</div>
                    <p>${d.keyMetrics}</p>
                </div>
                <div class="def-section">
                    <div class="def-section-title">Стратегический фокус</div>
                    <p>${d.strategy}</p>
                </div>
            </div>
            <div class="def-footer">
                <a href="#" class="def-link" data-tab="${seg.toLowerCase()}" onclick="event.preventDefault(); activateTab('${seg.toLowerCase()}');">
                    Перейти к дашборду сегмента →
                </a>
            </div>
        </div>`;
    }).join('');
}

// --- Общий отчёт Tab ---
const RPT_REGIONS = {'ALL OFFLINE':'Все магазины','Moscow + MO':'Москва и МО','Spb+LO':'СПб и ЛО','OTHER':'Регионы','e-commerce':'E-commerce'};

function buildReport(region) {
    const container = document.getElementById('tab-report');
    region = region || 'ALL OFFLINE';

    // Месяцы, за которые рассчитан отчёт (могут отставать от сегментных данных)
    const rptMonths = YM_LIST.filter(m => CITY_DATA[region]?.[m + '_1']);
    const selYm = window._rptYm || document.getElementById('month-selector')?.value || YM_LIST[YM_LIST.length - 1];
    let rptYm = rptMonths[rptMonths.length - 1];
    for (let i = rptMonths.length - 1; i >= 0; i--) {
        if (rptMonths[i] <= selYm) { rptYm = rptMonths[i]; break; }
    }
    const label = YM_LABELS[rptYm];
    const ymIdx = YM_LIST.indexOf(rptYm);
    const last12 = YM_LIST.slice(Math.max(0, ymIdx - 11), ymIdx + 1);

    // Colors matching reference
    const CLR_CUR = '#E87722';   // bright orange (current year)
    const CLR_PREV = CHART_GRAY; // серый для прошлого года (коричневый не использовать)
    const CLR_RET_CUR = '#2E8B57';  // green for retention current
    const CLR_RET_PREV = CHART_GRAY; // прошлый год всегда серый

    // City-level report data helpers
    function getCityData(ym, period) {
        return CITY_DATA[region]?.[ym + '_' + period] || null;
    }
    function getCitySeries(yms, metric, divider) {
        return yms.map(ym => {
            const d = getCityData(ym, 1);
            if (!d) return null;
            const v = d[metric];
            return v != null ? v / (divider || 1) : null;
        });
    }
    function getCitySeriesPrev(yms, metric, divider) {
        return yms.map(ym => {
            const d = getCityData(ym, -1);
            if (!d) return null;
            const v = d[metric];
            return v != null ? v / (divider || 1) : null;
        });
    }

    function lastYoY(cur, prev) {
        const c = cur.filter(v => v != null).pop();
        const p = prev.filter(v => v != null).pop();
        if (c == null || p == null || p === 0) return null;
        return (c - p) / Math.abs(p);
    }
    function fmtYoY(v) {
        if (v == null) return '—';
        let pct = (v * 100).toFixed(0);
        if (pct === '-0') pct = '0';
        return (v * 100 >= 0.5 ? '+' : '') + pct + '%';
    }

    // Compute KPI values
    const toC = getCitySeries(last12, 'TURNOVER', 1e9);
    const toP = getCitySeriesPrev(last12, 'TURNOVER', 1e9);
    const cliC = getCitySeries(last12, 'CONTACTS', 1e6);
    const cliP = getCitySeriesPrev(last12, 'CONTACTS', 1e6);
    const arpuC = getCitySeries(last12, 'BUDGET', 1);
    const arpuP = getCitySeriesPrev(last12, 'BUDGET', 1);
    const checkC = getCitySeries(last12, 'AVG_CHECK', 1);
    const checkP = getCitySeriesPrev(last12, 'AVG_CHECK', 1);
    const retC_raw = last12.map(ym => { const d = getCityData(ym, 1); return d?.RETENTION != null ? d.RETENTION * 100 : null; });
    const retP_raw = last12.map(ym => { const d = getCityData(ym, -1); return d?.RETENTION != null ? d.RETENTION * 100 : null; });

    const toYoY = lastYoY(toC, toP);
    const cliYoY = lastYoY(cliC, cliP);
    const arpuYoY = lastYoY(arpuC, arpuP);
    const checkYoY = lastYoY(checkC, checkP);
    const retLast = retC_raw.filter(v => v != null).pop();
    const retPrevLast = retP_raw.filter(v => v != null).pop();
    const retDelta = retLast != null && retPrevLast != null ? retLast - retPrevLast : null;

    container.innerHTML = `
        <div class="page-header">
            <div>
                <h1>Общий отчёт</h1>
                <p class="subtitle">${region === 'e-commerce' ? 'Динамика ключевых показателей · E-commerce' : 'Динамика ключевых показателей в LFL-магазинах'}</p>
            </div>
            <div class="header-meta" style="gap:12px">
                <select id="region-selector" class="region-selector">${Object.keys(RPT_REGIONS).map(k => `<option value="${k}" ${k === region ? 'selected' : ''}>${RPT_REGIONS[k]}</option>`).join('')}</select>
                <select id="rpt-month-sel" class="month-selector">${rptMonths.map(m => `<option value="${m}" ${m === rptYm ? 'selected' : ''}>${YM_LABELS[m]}</option>`).join('')}</select>
            </div>
        </div>

        <!-- KPI highlights -->
        <div class="report-highlights">
            <div class="report-highlight-card" style="border-left:4px solid ${CLR_CUR}">
                <div class="rh-label">Прирост ТО · ${label}</div>
                <div class="rh-value ${toYoY >= 0 ? 'up' : 'down'}">${fmtYoY(toYoY)}</div>
                <div class="rh-desc">к тому же месяцу прошлого года</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #2E8B57">
                <div class="rh-label">Прирост клиентов · ${label}</div>
                <div class="rh-value ${cliYoY >= 0 ? 'up' : 'down'}">${fmtYoY(cliYoY)}</div>
                <div class="rh-desc">к тому же месяцу прошлого года</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #4A90D9">
                <div class="rh-label">Прирост ARPU · ${label}</div>
                <div class="rh-value ${arpuYoY >= 0 ? 'up' : 'down'}">${fmtYoY(arpuYoY)}</div>
                <div class="rh-desc">к тому же месяцу прошлого года</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #7B61FF">
                <div class="rh-label">Retention · ${label}</div>
                <div class="rh-value ${retDelta >= 0 ? 'up' : 'down'}">${retLast != null ? retLast.toFixed(0) + '%' : '—'}</div>
                <div class="rh-desc">${retDelta != null ? (retDelta >= 0 ? '+' : '') + retDelta.toFixed(1) + ' п.п. YoY' : ''}</div>
            </div>
        </div>

        <!-- Charts: 2 графика + колонка наблюдений в ряду (как на «Динамике базы») -->
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card" style="grid-column: span 2"><div class="card-header"><h3>Товарооборот, млрд ₽</h3></div>
                <div class="report-subtitle" id="report-to-sub"></div>
                <div class="chart-container" style="height:200px"><canvas id="rpt-turnover"></canvas></div></div>
            <div class="insight-col" id="rpt-ins-to"></div>
        </div>
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card"><div class="card-header"><h3>Клиенты, млн</h3></div>
                <div class="report-subtitle" id="report-cli-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-clients"></canvas></div></div>
            <div class="card"><div class="card-header"><h3>ARPU, тыс. ₽</h3></div>
                <div class="report-subtitle" id="report-arpu-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-arpu"></canvas></div></div>
            <div class="insight-col" id="rpt-ins-base"></div>
        </div>
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card"><div class="card-header"><h3>Средний чек, ₽</h3></div>
                <div class="report-subtitle" id="report-check-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-avgcheck"></canvas></div></div>
            <div class="card"><div class="card-header"><h3>Чеков / клиента, шт</h3></div>
                <div class="report-subtitle" id="report-freq-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-freq"></canvas></div></div>
            <div class="insight-col" id="rpt-ins-check"></div>
        </div>
        <!-- Раскладка среднего чека: чек = SKU/чек × цена SKU -->
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card"><div class="card-header"><h3>Ср. цена SKU, ₽</h3></div>
                <div class="report-subtitle" id="report-sku-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-sku-price"></canvas></div></div>
            <div class="card"><div class="card-header"><h3>SKU / чек, шт</h3></div>
                <div class="report-subtitle" id="report-skupc-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-sku-count"></canvas></div></div>
            <div class="insight-col" id="rpt-ins-sku"></div>
        </div>
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card"><div class="card-header"><h3>Retention, %</h3></div>
                <div class="report-subtitle" id="report-ret-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-retention"></canvas></div></div>
            <div class="card"><div class="card-header"><h3>Индекс лояльности</h3></div>
                <div class="report-subtitle" id="report-li-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-loyalty-idx"></canvas></div></div>
            <div class="insight-col" id="rpt-ins-ret"></div>
        </div>
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card"><div class="card-header"><h3>Ценовой индекс</h3></div>
                <div class="report-subtitle" id="report-pi-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-price-idx"></canvas></div></div>
            <div class="card"><div class="card-header"><h3>Скидка по карте, %</h3></div>
                <div class="report-subtitle" id="report-sale-sub"></div>
                <div class="chart-container" style="height:190px"><canvas id="rpt-card-sale"></canvas></div></div>
            <div class="insight-col" id="rpt-ins-price"></div>
        </div>

        <!-- Insights -->
        <div class="card report-insights-card">
            <div class="card-header"><h3 style="color:${CLR_CUR}">Выводы и рекомендации</h3></div>
            <div class="report-insights" id="rpt-insights"></div>
        </div>
    `;

    // Wire region selector
    document.getElementById('region-selector').addEventListener('change', function() {
        container.innerHTML = '';
        buildReport(this.value);
    });
    // Wire month selector (свой фильтр отчёта, независимый от Executive Summary)
    document.getElementById('rpt-month-sel').addEventListener('change', function() {
        window._rptYm = this.value;
        container.innerHTML = '';
        buildReport(region);
    });

    const monthLabels = last12.map(ym => {
        const m = parseInt(ym.slice(4));
        const y = ym.slice(2, 4);
        return MONTHS[m - 1] + '.' + y;
    });

    // Generic YoY paired bar chart builder
    function buildYoYChart(canvasId, subId, cur, prev, fmtFn, opts) {
        // YoY по каждому месяцу: % (или п.п. при opts.ppDelta)
        const yoyArr = cur.map((v, i) => {
            const p = prev[i];
            if (v == null || p == null) return null;
            if (opts?.ppDelta) return v - p;
            return p !== 0 ? (v - p) / Math.abs(p) : null;
        });
        const fmtYoYLbl = (y, full) => {
            if (y == null) return '';
            if (opts?.ppDelta) {
                let t = y.toFixed(1);
                if (t === '-0.0') t = '0.0';
                return (y >= 0.05 ? '+' : '') + t + (full ? ' пп' : '');
            }
            let pct = (y * 100).toFixed(0);
            if (pct === '-0') pct = '0';
            return (y * 100 >= 0.5 ? '+' : '') + pct + '%';
        };

        const clrCur = opts?.clrCur || CLR_CUR;
        const clrPrev = opts?.clrPrev || CLR_PREV;

        // Бейдж YoY — в правый верхний угол карточки (строка заголовка),
        // легенда «Год / Год назад» — под названием графика
        let lastIdx = -1;
        for (let i = yoyArr.length - 1; i >= 0; i--) { if (yoyArr[i] != null) { lastIdx = i; break; } }
        const subEl = document.getElementById(subId);
        const header = subEl?.closest('.card')?.querySelector('.card-header');
        if (header) {
            let slot = header.querySelector('.rpt-badge-slot');
            if (!slot) { slot = document.createElement('span'); slot.className = 'rpt-badge-slot'; header.appendChild(slot); }
            slot.innerHTML = lastIdx >= 0
                ? '<span class="report-yoy-badge ' + (yoyArr[lastIdx] >= 0 ? 'up' : 'down') + '">' + monthLabels[lastIdx] + ': ' + fmtYoYLbl(yoyArr[lastIdx], true) + ' YoY</span>'
                : '';
        }
        if (subEl) subEl.innerHTML = `<span class="report-legend"><i style="background:${clrCur}"></i>Год<i style="background:${clrPrev}"></i>Год назад</span>`;

        destroyChart(canvasId);
        chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
            type: 'bar',
            data: {
                labels: monthLabels,
                datasets: [
                    { label: 'Год', data: cur, backgroundColor: clrCur, borderRadius: 3, barPercentage: 0.85, categoryPercentage: 0.78 },
                    { label: 'Год назад', data: prev, backgroundColor: clrPrev, borderRadius: 3, barPercentage: 0.85, categoryPercentage: 0.78 },
                ]
            },
            plugins: [ChartDataLabels],
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { top: 28 } },
                plugins: {
                    legend: { display: false },
                    datalabels: {
                        display: ctx => ctx.datasetIndex === 0,
                        labels: {
                            value: {
                                anchor: 'end', align: 'top', offset: 1,
                                font: { size: 9, weight: '700' },
                                color: '#333',
                                backgroundColor: 'rgba(255,255,255,0.85)',
                                borderRadius: 3,
                                padding: { top: 1, bottom: 0, left: 3, right: 3 },
                                formatter: v => v != null ? fmtFn(v) : ''
                            },
                            yoy: {
                                anchor: 'end', align: 'top', offset: 16,
                                font: { size: 8, weight: '600' },
                                color: ctx => {
                                    const y = yoyArr[ctx.dataIndex];
                                    return y == null ? '#999' : (y >= 0 ? '#2E8B57' : '#C41E3A');
                                },
                                backgroundColor: 'rgba(255,255,255,0.85)',
                                borderRadius: 3,
                                padding: { top: 1, bottom: 0, left: 3, right: 3 },
                                formatter: (v, ctx) => fmtYoYLbl(yoyArr[ctx.dataIndex])
                            }
                        }
                    }
                },
                scales: {
                    x: { grid: { display: false }, ticks: { font: { size: 10 } } },
                    y: { beginAtZero: false, grid: { color: 'rgba(0,0,0,0.05)' },
                         grace: '8%', // запас сверху под подписи
                         ticks: { callback: v => fmtFn(v), font: { size: 10 } },
                         ...(opts?.yMin != null ? { min: opts.yMin } : {}),
                         ...(opts?.yMax != null ? { max: opts.yMax } : {})
                    }
                }
            }
        });
    }

    // Build all charts
    buildYoYChart('rpt-turnover', 'report-to-sub', toC, toP, v => v.toFixed(1));
    buildYoYChart('rpt-clients', 'report-cli-sub', cliC, cliP, v => v.toFixed(2));
    buildYoYChart('rpt-arpu', 'report-arpu-sub', arpuC, arpuP, v => (v / 1000).toFixed(1));
    buildYoYChart('rpt-avgcheck', 'report-check-sub', checkC, checkP, v => Math.round(v).toLocaleString('ru-RU'));

    const freqC = getCitySeries(last12, 'CHEQUE_PER_CLIENT', 1);
    const freqP = getCitySeriesPrev(last12, 'CHEQUE_PER_CLIENT', 1);
    buildYoYChart('rpt-freq', 'report-freq-sub', freqC, freqP, v => v.toFixed(1));

    // Retention: YoY в процентных пунктах; шкала от данных (у e-com уровень ~60%)
    const retAll = retC_raw.concat(retP_raw).filter(v => v != null);
    const retMin = retAll.length ? Math.max(0, Math.floor(Math.min(...retAll) / 5) * 5 - 5) : 70;
    const retMax = retAll.length ? Math.min(100, Math.ceil(Math.max(...retAll) / 5) * 5 + 5) : 90;
    buildYoYChart('rpt-retention', 'report-ret-sub', retC_raw, retP_raw,
        v => v.toFixed(0) + '%',
        { clrCur: CLR_RET_CUR, clrPrev: CLR_RET_PREV, yMin: retMin, yMax: retMax, ppDelta: true });

    // --- Segment-level price trend charts ---
    // Для региона E-commerce сегментные средние берём из e-com снапшотов
    const rptSnaps = region === 'e-commerce' && typeof SNAPSHOTS_ECOM !== 'undefined' ? SNAPSHOTS_ECOM : SNAPSHOTS;
    function weightedAvg(yms, metric) {
        return yms.map(ym => {
            const snap = rptSnaps[ym];
            if (!snap) return null;
            let sumW = 0, sumV = 0;
            for (const seg of ['ACTIVE_LFL','ACTIVE','RANDOM','NEW']) {
                const s = snap[seg];
                if (!s || s[metric] == null || s.CLIENTS == null) continue;
                sumW += s.CLIENTS; sumV += s.CLIENTS * s[metric];
            }
            return sumW > 0 ? sumV / sumW : null;
        });
    }
    const prevLast12 = last12.map(ym => '' + (parseInt(ym.slice(0, 4)) - 1) + ym.slice(4));

    // Сегментные данные могут отставать от окна отчёта — помечаем
    const segLastYm = [...last12].reverse().find(m => rptSnaps[m] && Object.keys(rptSnaps[m]).length);
    const segNote = segLastYm && segLastYm !== last12[last12.length - 1] ? `сегментные данные — по ${YM_LABELS[segLastYm]}` : '';

    function buildWAvg(canvasId, subId, metric, fmtFn) {
        buildYoYChart(canvasId, subId, weightedAvg(last12, metric), weightedAvg(prevLast12, metric), fmtFn);
        if (segNote) {
            const subEl = document.getElementById(subId);
            if (subEl) subEl.innerHTML += `<span class="report-seg-note">${segNote}</span>`;
        }
    }
    buildWAvg('rpt-loyalty-idx', 'report-li-sub', 'LOYALTY_INDEX', v => v.toFixed(3));
    buildWAvg('rpt-sku-price', 'report-sku-sub', 'AVG_COST_SKU', v => Math.round(v).toLocaleString('ru-RU'));
    buildWAvg('rpt-sku-count', 'report-skupc-sub', 'AVG_SKU', v => v.toFixed(2));
    buildWAvg('rpt-price-idx', 'report-pi-sub', 'PRICE_INDEX', v => v.toFixed(2));
    buildWAvg('rpt-card-sale', 'report-sale-sub', 'SALE', v => (v * 100).toFixed(1) + '%');

    // --- Insights (все цифры считаются из данных выбранного региона/месяца) ---
    const regionName = RPT_REGIONS[region];
    const freqYoY = lastYoY(freqC, freqP);
    const skuYoY = lastYoY(getCitySeries(last12, 'SKU_COST', 1), getCitySeriesPrev(last12, 'SKU_COST', 1));
    const skuPcYoY = lastYoY(getCitySeries(last12, 'SKU_PER_CHECK', 1), getCitySeriesPrev(last12, 'SKU_PER_CHECK', 1));
    const liYoY = lastYoY(getCitySeries(last12, 'AVG_LOYALTY_INDEX', 1), getCitySeriesPrev(last12, 'AVG_LOYALTY_INDEX', 1));
    const cardLast = getCitySeries(last12, 'AVG_CARD_SALE', 1).filter(v => v != null).pop();
    const cardPrevLast = getCitySeriesPrev(last12, 'AVG_CARD_SALE', 1).filter(v => v != null).pop();
    const cardDeltaPp = cardLast != null && cardPrevLast != null ? (cardLast - cardPrevLast) * 100 : null;

    const baseDriven = (cliYoY ?? 0) >= (arpuYoY ?? 0);
    const lflTxt = region === 'e-commerce' ? '' : 'в LFL-магазинах ';
    const headline = toYoY != null && toYoY >= 0
        ? `Прирост ТО ${fmtYoY(toYoY)} YoY (${label}) ${lflTxt}(${regionName}) обеспечен ${baseDriven ? 'ростом клиентской базы' : 'ростом ARPU'}`
        : `Снижение ТО ${fmtYoY(toYoY)} YoY (${label}) ${lflTxt}(${regionName})`;

    const insightsHtml = `
        <div class="rpt-insight-headline">${headline}</div>
        <div class="rpt-insights-grid">
            <div class="rpt-insight-block" style="border-left:3px solid ${CLR_CUR}">
                <div class="rpt-ib-title"><span class="tag tag-growth">РОСТ</span> Товарооборот и клиенты</div>
                <ul>
                    <li>Прирост ТО <strong>${fmtYoY(toYoY)} YoY</strong> при росте клиентской базы <strong>${fmtYoY(cliYoY)} YoY</strong></li>
                    <li>ARPU <strong>${fmtYoY(arpuYoY)} YoY</strong> — ${arpuYoY >= 0 ? 'выручка на клиента растёт' : 'выручка на клиента снижается'}</li>
                    <li>Ср. чек <strong>${fmtYoY(checkYoY)} YoY</strong>${checkYoY < -0.005 ? ' — давление на выручку с визита' : ''}</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #7B61FF">
                <div class="rpt-ib-title"><span class="tag tag-attention">ВНИМАНИЕ</span> Retention и база</div>
                <ul>
                    <li>Retention ${retLast != null ? retLast.toFixed(0) + '%' : '—'} ${retDelta != null ? '(' + (retDelta >= 0 ? '+' : '') + retDelta.toFixed(1) + ' п.п. YoY)' : ''}</li>
                    <li>${retDelta != null && retDelta < 0 ? 'Снижение удержания вероятно связано с разбавлением базы менее лояльными клиентами' : 'Стабильный уровень удержания поддерживает устойчивый рост'}</li>
                    <li>Частота визитов <strong>${fmtYoY(freqYoY)} YoY</strong> — ${freqYoY >= 0 ? 'клиенты приходят чаще' : 'рекомендуется усилить промо-активность'}</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #C41E3A">
                <div class="rpt-ib-title"><span class="tag tag-risk">РИСК</span> Ценовое поведение</div>
                <ul>
                    <li>Ср. цена SKU <strong>${fmtYoY(skuYoY)} YoY</strong>${skuYoY > 0 ? ' — инфляционное давление на корзину' : ''}${skuPcYoY != null && skuPcYoY < 0 ? `; SKU/чек ${fmtYoY(skuPcYoY)} YoY — состав корзины сокращается` : ''}</li>
                    <li>Скидка по карте ${cardLast != null ? (cardLast * 100).toFixed(1) + '%' : '—'} ${cardDeltaPp != null ? '(' + (cardDeltaPp >= 0 ? '+' : '') + cardDeltaPp.toFixed(1) + ' п.п. YoY)' : ''} — ${cardDeltaPp != null && cardDeltaPp > 0 ? 'нагрузка на промо-бюджет растёт' : 'нагрузка на промо-бюджет не растёт'}</li>
                    <li>Индекс лояльности <strong>${fmtYoY(liYoY)} YoY</strong> — ${liYoY != null && liYoY < 0 ? 'клиенты покупают меньше товаров повседневного спроса: новые клиенты входят с корзиной из меньшего числа SKU и менее лояльны' : 'частота покупок товаров повседневного спроса стабильна'}</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #2E8B57">
                <div class="rpt-ib-title"><span class="tag tag-rec">РЕКОМЕНДАЦИИ</span></div>
                <ul>
                    <li><strong>Увеличение частоты:</strong> персонализированные промо для клиентов с низкой частотой визитов</li>
                    <li><strong>Оптимизация промо:</strong> переход от массовых скидок к таргетированным механикам</li>
                    <li><strong>Cross-sell:</strong> увеличение SKU/чек через рекомендации и комплементарные предложения</li>
                    <li><strong>Удержание ядра:</strong> программа retention через персональные предложения, не через увеличение скидки</li>
                </ul>
            </div>
        </div>
    `;
    document.getElementById('rpt-insights').innerHTML = insightsHtml;

    // --- Колонки наблюдений: answer-first, пороги значимости, все цифры из данных ---
    function insCol(id, title, lines) {
        const el = document.getElementById(id);
        if (el) el.innerHTML = `<h4>${title}</h4>` + lines.filter(Boolean).map(l => `<div>${l}</div>`).join('');
    }
    const up = t => `<span class="ins-up">${t}</span>`;
    const down = t => `<span class="ins-down">${t}</span>`;
    const sig = (v, t) => v >= 0 ? up(t) : down(t);      // рост = позитив
    const inv = (v, t) => v >= 0 ? down(t) : up(t);      // рост = негатив (цены, скидки)
    const alertLn = t => `<div class="ins-alert">${t}</div>`;
    const MAT = 0.02; // порог значимости: |изменение| < 2% — «без изменений»
    const firstNN = a => a.find(v => v != null);
    const lastNN = a => a.filter(v => v != null).pop();
    const peakIdx = a => a.reduce((bi, v, i) => v != null && (bi < 0 || v > a[bi]) ? i : bi, -1);
    const winLabel = `${monthLabels[0]}–${monthLabels[monthLabels.length - 1]}`;

    const toFirst = firstNN(toC), toLast = lastNN(toC);
    let toStreak = 0;
    for (let i = toC.length - 1; i >= 0; i--) {
        if (toC[i] != null && toP[i] != null && toC[i] > toP[i]) toStreak++;
        else if (toC[i] != null && toP[i] != null) break;
    }
    const piArr = weightedAvg(last12, 'PRICE_INDEX').filter(v => v != null);
    const piPrevArr = weightedAvg(prevLast12, 'PRICE_INDEX').filter(v => v != null);
    const piLast = piArr.length ? piArr[piArr.length - 1] : null;
    const piPrev = piPrevArr.length ? piPrevArr[piPrevArr.length - 1] : null;
    const piYoY = piLast != null && piPrev ? (piLast - piPrev) / Math.abs(piPrev) : null;
    // рост экстенсивный: база растёт двузначно, выручка на клиента — нет
    const extensive = cliYoY != null && arpuYoY != null && cliYoY > 0.1 && arpuYoY < MAT;

    insCol('rpt-ins-to', 'Товарооборот', [
        toYoY > MAT && baseDriven
            ? '<strong>Товарооборот растёт только за счёт вовлечения новых клиентов в программу лояльности</strong>'
            : `<strong>ТО ${label}: ${fmtYoY(toYoY)} к прошлому году</strong>`,
        `за 12 мес. (${winLabel}): ${toFirst != null ? toFirst.toFixed(1) : '—'} → ${toLast != null ? toLast.toFixed(1) : '—'} млрд ₽; последний месяц ${sig(toYoY ?? 0, fmtYoY(toYoY) + ' YoY')}${toStreak >= 6 ? `, рост к прошлому году ${toStreak} мес. подряд` : ''}`,
        toYoY != null && cliYoY != null && arpuYoY != null
            ? `декомпозиция: ТО ${fmtYoY(toYoY)} = клиенты ${fmtYoY(cliYoY)} × ARPU ${fmtYoY(arpuYoY)}`
            : null,
        extensive ? alertLn('рост экстенсивный: выручка на клиента не растёт — темп ТО полностью зависит от притока новых клиентов') : null,
    ]);
    insCol('rpt-ins-base', 'Клиенты и ARPU', [
        `<strong>${extensive
            ? 'Весь прирост создаёт база: ARPU за год практически не изменился'
            : (cliYoY ?? 0) >= (arpuYoY ?? 0) ? 'Основной вклад в рост — клиентская база' : 'Основной вклад в рост — ARPU'}</strong>`,
        `база: ${firstNN(cliC) != null ? firstNN(cliC).toFixed(2) : '—'} → ${lastNN(cliC) != null ? lastNN(cliC).toFixed(2) : '—'} млн за 12 мес.; ${sig(cliYoY ?? 0, fmtYoY(cliYoY) + ' YoY')}`,
        `ARPU: ${firstNN(arpuC) != null ? Math.round(firstNN(arpuC)).toLocaleString('ru-RU') : '—'} → ${lastNN(arpuC) != null ? Math.round(lastNN(arpuC)).toLocaleString('ru-RU') : '—'} ₽; ${sig(arpuYoY ?? 0, fmtYoY(arpuYoY) + ' YoY')}${Math.abs(arpuYoY ?? 0) < MAT ? ' — практически без изменений' : ''}`,
        checkYoY != null && freqYoY != null ? `декомпозиция ARPU: чек ${fmtYoY(checkYoY)} × частота ${fmtYoY(freqYoY)}` : null,
    ]);
    const checkVals = checkC.filter(v => v != null);
    const checkPeak = peakIdx(checkC);
    const flatCheckInflation = Math.abs(checkYoY ?? 0) < MAT && (skuYoY ?? 0) > MAT;
    insCol('rpt-ins-check', 'Чек и частота', [
        `<strong>${flatCheckInflation
            ? 'Чек не растёт при инфляции — в реальном выражении визит дешевеет'
            : Math.abs(checkYoY ?? 0) < MAT && Math.abs(freqYoY ?? 0) < MAT
                ? 'Поведение клиента стабильно: ни чек, ни частота за год существенно не изменились'
                : (freqYoY ?? 0) - (checkYoY ?? 0) > MAT ? 'Рост ARPU обеспечивает частота визитов' : 'Рост ARPU обеспечивает чек'}</strong>`,
        checkVals.length ? `ср. чек: ${Math.round(Math.min(...checkVals))}–${Math.round(Math.max(...checkVals))} ₽ за 12 мес.${checkPeak >= 0 ? ` (пик — ${monthLabels[checkPeak]})` : ''}; ${sig(checkYoY ?? 0, fmtYoY(checkYoY) + ' YoY')}` : null,
        `частота: ${firstNN(freqC) != null ? firstNN(freqC).toFixed(1) : '—'} → ${lastNN(freqC) != null ? lastNN(freqC).toFixed(1) : '—'} чеков/клиента; ${sig(freqYoY ?? 0, fmtYoY(freqYoY) + ' YoY')}${Math.abs(freqYoY ?? 0) < MAT ? ' — в пределах колебаний' : ''}`,
        flatCheckInflation ? alertLn(`при росте цены SKU ${fmtYoY(skuYoY)} чек должен был прирастать минимум на уровень инфляции — фактически ${fmtYoY(checkYoY)}: рост цен полностью компенсирован сокращением корзины (см. раскладку ниже)`) : null,
    ]);
    insCol('rpt-ins-sku', 'Раскладка чека', [
        `<strong>${(skuYoY ?? 0) > MAT && (skuPcYoY ?? 0) < -MAT
            ? 'Рост цены SKU полностью компенсирован сокращением корзины — реальное потребление за визит снижается'
            : (skuPcYoY ?? 0) < -MAT ? 'Состав корзины сокращается' : 'Структура чека стабильна'}</strong>`,
        skuYoY != null && skuPcYoY != null ? `чек ${fmtYoY(checkYoY)} = цена SKU ${fmtYoY(skuYoY)} × SKU/чек ${fmtYoY(skuPcYoY)}` : null,
        (skuYoY ?? 0) > MAT ? `цена SKU ${inv(skuYoY, fmtYoY(skuYoY) + ' YoY')} — инфляционное давление на корзину` : null,
        (skuPcYoY ?? 0) < -MAT ? `SKU/чек ${down(fmtYoY(skuPcYoY) + ' YoY')} — клиенты сокращают состав корзины` : null,
        (skuYoY ?? 0) > MAT && (skuPcYoY ?? 0) < -MAT
            ? alertLn('потенциал роста чека — расширение корзины, а не цена')
            : null,
    ]);
    const retVals = retC_raw.filter(v => v != null);
    insCol('rpt-ins-ret', 'Удержание и лояльность', [
        `<strong>${retDelta != null && retDelta < -0.3 ? 'Удержание под давлением притока новых клиентов' : 'Удержание стабильно'}</strong>`,
        retVals.length ? `Retention: ${Math.min(...retVals).toFixed(0)}–${Math.max(...retVals).toFixed(0)}% за 12 мес.; сейчас ${retLast != null ? retLast.toFixed(0) + '%' : '—'} (${retDelta != null ? sig(retDelta, (retDelta >= 0 ? '+' : '') + retDelta.toFixed(1) + ' п.п. YoY') : '—'})` : null,
        liYoY != null ? `индекс лояльности ${sig(liYoY, fmtYoY(liYoY) + ' YoY')} — клиенты покупают меньше товаров повседневного спроса (daily)` : null,
        liYoY != null && liYoY < -MAT && (cliYoY ?? 0) > 0.1
            ? 'новые клиенты входят с корзиной из меньшего числа SKU и являются менее лояльными — индекс снижается по мере разбавления базы'
            : null,
    ]);
    insCol('rpt-ins-price', 'Цены и промо', [
        `<strong>${cardDeltaPp != null && cardDeltaPp > 0.5
            ? `Промо-нагрузка растёт: скидка по карте ${(cardLast * 100).toFixed(1)}% (+${cardDeltaPp.toFixed(1)} п.п. за год)`
            : 'Промо-нагрузка стабильна'}</strong>`,
        piLast != null ? `ценовой индекс ${piLast.toFixed(2)}${piYoY != null ? ` (${fmtYoY(piYoY)} YoY)` : ''}${piYoY != null && piYoY < -MAT ? ' — клиенты выбирают более дешёвые товары с полки' : piYoY != null && piYoY > MAT ? ' — клиенты выбирают более дорогие товары с полки' : ''}` : null,
        cardDeltaPp != null && cardDeltaPp > 0.5
            ? 'скидка по карте для клиента растёт за счёт увеличения ДЦО — вероятнее всего, коррелирует с увеличением количества новых клиентов'
            : null,
    ]);
}

// --- Client Base Tab (I_CVM_CONTACT) ---
function buildClientBase() {
    const container = document.getElementById('tab-clientbase');
    // Always rebuild to ensure charts render properly
    container.innerHTML = '';

    const allSegs = [...new Set(CVM_CONTACTS.map(c => c.segment))];

    // Build summary by segment with 3 channels: offline, omni, e-commerce
    const segData = {};
    allSegs.forEach(seg => {
        const rows = CVM_CONTACTS.filter(c => c.segment === seg);
        const push = rows.reduce((s, r) => s + (r.PUSH || 0), 0);
        const offRow = rows.find(r => r.channel === 'только оффлайн') || {};
        const omniRow = rows.find(r => r.channel === 'омни') || {};
        const ecomRow = rows.find(r => r.channel === 'только e-commerce') || {};
        // Offline = org1 (оффлайн + омни), E-com = org3 (все: только e-com + омни)
        const offlineTotal = (offRow['клиентов'] || 0) + (omniRow['клиентов'] || 0);
        const ecomAll = rows.filter(r => r.org === 3).reduce((s, r) => s + (r['клиентов'] || 0), 0);
        const ecomOnly = ecomRow['клиентов'] || 0;
        // Avoid double counting omni: total = offline-only + omni (counted once) + ecom-only
        const total = offlineTotal + ecomOnly;
        segData[seg] = { total, offlineTotal, ecomAll, ecomOnly, push, offRow, omniRow, ecomRow, rows };
    });

    const grandTotal = Object.values(segData).reduce((s, d) => s + d.total, 0);
    const grandOffline = Object.values(segData).reduce((s, d) => s + d.offlineTotal, 0);
    const grandEcomAll = Object.values(segData).reduce((s, d) => s + d.ecomAll, 0);
    const grandEcomOnly = Object.values(segData).reduce((s, d) => s + d.ecomOnly, 0);
    const grandOmni = Object.values(segData).reduce((s, d) => s + (d.omniRow?.['клиентов'] || 0), 0);
    const pushTotal = Object.values(segData).reduce((s, d) => s + d.push, 0);
    const pushOffline = Object.values(segData).reduce((s, d) => s + (d.offRow?.PUSH || 0) + (d.omniRow?.PUSH || 0), 0);
    const pushEcom = Object.values(segData).reduce((s, d) => s + d.rows.filter(r => r.org === 3).reduce((ss, r) => ss + (r.PUSH || 0), 0), 0);
    const pushOmniTotal = Object.values(segData).reduce((s, d) => s + (d.omniRow?.PUSH || 0), 0);
    // Omni among e-com: org=3 channel=омни
    const ecomOmni = Object.values(segData).reduce((s, d) => s + d.rows.filter(r => r.org === 3 && r.channel === 'омни').reduce((ss, r) => ss + (r['клиентов'] || 0), 0), 0);

    container.innerHTML = `
        <div class="page-header">
            <div>
                <h1>База клиентов</h1>
                <p class="subtitle">Структура клиентской базы по каналам коммуникации</p>
            </div>
        </div>

        <!-- KPIs -->
        <div class="report-highlights" style="grid-template-columns:repeat(5,1fr)">
            <div class="report-highlight-card" style="border-left:4px solid #E87722">
                <div class="rh-label">Всего клиентов</div>
                <div class="rh-value" style="color:#E87722">${fmt.int(grandTotal)}</div>
                <div class="rh-desc">PUSH: ${fmt.int(pushTotal)} (${(pushTotal/grandTotal*100).toFixed(0)}%)</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #C67A2E">
                <div class="rh-label">Клиентов Offline</div>
                <div class="rh-value" style="color:#C67A2E">${fmt.int(grandOffline)}</div>
                <div class="rh-desc">PUSH: ${fmt.int(pushOffline)} (${(pushOffline/grandOffline*100).toFixed(0)}%)</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #2E8B57">
                <div class="rh-label">E-commerce клиенты</div>
                <div class="rh-value" style="color:#2E8B57">${fmt.int(grandEcomAll)}</div>
                <div class="rh-desc">PUSH: ${fmt.int(pushEcom)} (${grandEcomAll > 0 ? (pushEcom/grandEcomAll*100).toFixed(0) : 0}%)</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #7B61FF">
                <div class="rh-label">Доля омни в Offline</div>
                <div class="rh-value" style="color:#7B61FF">${(grandOmni/grandOffline*100).toFixed(1)}%</div>
                <div class="rh-desc">${fmt.int(grandOmni)} клиентов</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #4A90D9">
                <div class="rh-label">Доля омни в E-com</div>
                <div class="rh-value" style="color:#4A90D9">${grandEcomAll > 0 ? (ecomOmni/grandEcomAll*100).toFixed(1) : 0}%</div>
                <div class="rh-desc">${fmt.int(ecomOmni)} клиентов</div>
            </div>
        </div>

        <!-- Pie charts: segment breakdown for Offline and E-com -->
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>Offline: клиенты по сегментам</h3></div>
                <div class="chart-container" style="height:340px"><canvas id="cb-pie-offline"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>E-commerce: клиенты по сегментам</h3></div>
                <div class="chart-container" style="height:340px"><canvas id="cb-pie-ecom"></canvas></div>
            </div>
        </div>

        <!-- LTV by segment: Offline and E-com -->
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>LTV по сегментам — Offline, ₽</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-ltv-off" height="240"</canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>LTV по сегментам — E-commerce, ₽</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-ltv-off-ecom" height="240"</canvas></div>
            </div>
        </div>

        <!-- ARPU by segment -->
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>ARPU по сегментам — Offline, ₽</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-arpu-off" height="240"</canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>ARPU по сегментам — E-commerce, ₽</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-arpu-ecom" height="240"</canvas></div>
            </div>
        </div>
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>Средний чек — Offline, ₽</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-check-off" height="240"</canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>Средний чек — E-commerce, ₽</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-check-ecom" height="240"</canvas></div>
            </div>
        </div>
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>Чеков / клиента — Offline</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-freq-off" height="240"</canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>Чеков / клиента — E-commerce</h3></div>
                <div class="chart-container" style="height:240px"><canvas id="cb-freq-ecom" height="240"</canvas></div>
            </div>
        </div>

        <!-- Bubble chart: ЦИ vs LTV -->
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>ЦИ vs LTV — Offline (размер = кол-во клиентов)</h3></div>
                <div class="chart-container" style="height:360px"><canvas id="cb-bubble-off" height="360"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>ЦИ vs LTV — E-commerce (размер = кол-во клиентов)</h3></div>
                <div class="chart-container" style="height:360px"><canvas id="cb-bubble-ecom" height="360"></canvas></div>
            </div>
        </div>

        <!-- McKinsey Insights -->
        <div class="card report-insights-card">
            <div class="card-header"><h3 style="color:#E87722">Стратегический анализ клиентской базы</h3></div>
            <div class="report-insights" id="cb-insights"></div>
        </div>

        <!-- Full table with Excel export -->
        <div class="card">
            <div class="card-header" style="display:flex;justify-content:space-between;align-items:center">
                <h3>Детализация по сегментам и каналам</h3>
                <button id="cb-export-btn" class="export-btn">📥 Выгрузить в Excel</button>
            </div>
            <div class="table-wrap">
                <table class="data-table" id="cb-table"></table>
            </div>
        </div>
    `;

    // Charts
    const mainSegs = allSegs.filter(s => !['Спящие','Фрод'].includes(s));
    const segLabels = mainSegs;
    // Fixed color per segment — same across both pies
    const SEG_PIE_COLORS = UNIFIED_SEG_COLORS;
    // pushBySegment: { segName: pushPct } for a given set of rows
    function calcPushPct(seg, orgFilter) {
        const rows = segData[seg]?.rows?.filter(r => orgFilter ? orgFilter(r) : true) || [];
        const clients = rows.reduce((s, r) => s + (r['клиентов'] || 0), 0);
        const push = rows.reduce((s, r) => s + (r.PUSH || 0), 0);
        return clients > 0 ? (push / clients * 100).toFixed(0) : '0';
    }

    function makePieOpts(data, labels, pushPcts) {
        const total = data.reduce((a, b) => a + b, 0);
        return {
            responsive: true, maintainAspectRatio: false,
            layout: { padding: { left: 70, right: 70, top: 25, bottom: 25 } },
            cutout: '40%',
            plugins: {
                legend: { display: false },
                datalabels: {
                    display: ctx => {
                        const pct = total > 0 ? (ctx.dataset.data[ctx.dataIndex] / total * 100) : 0;
                        return pct >= 0.5;
                    },
                    color: '#333',
                    font: { size: 9, weight: '600' },
                    anchor: 'end',
                    align: 'end',
                    offset: 8,
                    clip: false,
                    formatter: (v, ctx) => {
                        const idx = ctx.dataIndex;
                        const pct = total > 0 ? (v / total * 100).toFixed(1) : 0;
                        const pushPct = pushPcts ? pushPcts[idx] : null;
                        let txt = ctx.chart.data.labels[idx] + ' ' + pct + '%';
                        if (pushPct != null) txt += '\nPUSH ' + pushPct + '%';
                        return txt;
                    },
                    textAlign: 'center'
                }
            }
        };
    }

    // Plugin to draw connector lines from slice to label
    const pieConnectorPlugin = {
        id: 'pieConnector',
        afterDatasetsDraw(chart) {
            const ds = chart.getDatasetMeta(0);
            if (!ds || ds.type !== 'doughnut') return;
            const ctx = chart.ctx;
            ds.data.forEach((arc, i) => {
                const model = arc;
                const props = model.getProps(['startAngle','endAngle','outerRadius','x','y']);
                const midAngle = (props.startAngle + props.endAngle) / 2;
                const r = props.outerRadius;
                const cx = props.x;
                const cy = props.y;
                // Start point on outer edge
                const x1 = cx + Math.cos(midAngle) * r;
                const y1 = cy + Math.sin(midAngle) * r;
                // End point further out
                const x2 = cx + Math.cos(midAngle) * (r + 16);
                const y2 = cy + Math.sin(midAngle) * (r + 16);
                ctx.save();
                ctx.beginPath();
                ctx.moveTo(x1, y1);
                ctx.lineTo(x2, y2);
                ctx.strokeStyle = 'rgba(0,0,0,0.2)';
                ctx.lineWidth = 1;
                ctx.stroke();
                ctx.restore();
            });
        }
    };

    // Offline pie (all segments including Спящие, Фрод)
    const offPieSegs = allSegs.filter(s => segData[s].offlineTotal > 0);
    const offPieData = offPieSegs.map(s => segData[s].offlineTotal);
    const offPushPcts = offPieSegs.map(s => calcPushPct(s, r => r.org === 1));
    destroyChart('cb-pie-offline');
    chartInstances['cb-pie-offline'] = new Chart(document.getElementById('cb-pie-offline'), {
        type: 'doughnut',
        data: { labels: offPieSegs, datasets: [{ data: offPieData, backgroundColor: offPieSegs.map(s => SEG_PIE_COLORS[s] || '#999'), borderWidth: 2, borderColor: '#fff' }] },
        plugins: [ChartDataLabels, pieConnectorPlugin],
        options: makePieOpts(offPieData, offPieSegs, offPushPcts)
    });

    // E-com pie (all segments including Спящие, Фрод)
    const ecomPieSegs = allSegs.filter(s => segData[s].ecomAll > 0);
    const ecomPieData = ecomPieSegs.map(s => segData[s].ecomAll);
    const ecomPushPcts = ecomPieSegs.map(s => calcPushPct(s, r => r.org === 3));
    destroyChart('cb-pie-ecom');
    chartInstances['cb-pie-ecom'] = new Chart(document.getElementById('cb-pie-ecom'), {
        type: 'doughnut',
        data: { labels: ecomPieSegs, datasets: [{ data: ecomPieData, backgroundColor: ecomPieSegs.map(s => SEG_PIE_COLORS[s] || '#999'), borderWidth: 2, borderColor: '#fff' }] },
        plugins: [ChartDataLabels, pieConnectorPlugin],
        options: makePieOpts(ecomPieData, ecomPieSegs, ecomPushPcts)
    });

    // LTV vertical bars — fixed segment order
    const ltvSegOrder = ['Новые','Активные','Активные LFL','Случайные','Отток','Спящие'];
    const ltvSegColors = ltvSegOrder.map(s => SEG_PIE_COLORS[s] || '#999');

    // Offline LTV: weighted avg of offline + omni rows
    function getSegLTV(seg, orgFilter) {
        const rows = segData[seg]?.rows?.filter(r => orgFilter(r)) || [];
        const totalCli = rows.reduce((s, r) => s + (r['клиентов'] || 0), 0);
        const totalLTV = rows.reduce((s, r) => s + (r['клиентов'] || 0) * (r.LTV || 0), 0);
        return totalCli > 0 ? totalLTV / totalCli : 0;
    }

    function makeLtvChart(canvasId, data, labels, colors) {
        const maxVal = Math.max(...data.filter(v => v > 0), 1);
        destroyChart(canvasId);
        chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
            type: 'bar',
            data: { labels: labels, datasets: [{ data: data, backgroundColor: colors, borderRadius: 4, barThickness: 22 }] },
            plugins: [{
                id: 'ltvValueLabels',
                afterDatasetsDraw(chart) {
                    const ctx = chart.ctx;
                    const meta = chart.getDatasetMeta(0);
                    meta.data.forEach((bar, i) => {
                        const val = chart.data.datasets[0].data[i];
                        if (!val || val <= 0) return;
                        const txt = Math.round(val).toLocaleString('ru-RU') + ' ₽';
                        ctx.save();
                        ctx.font = '600 10px Inter, sans-serif';
                        ctx.fillStyle = '#555';
                        ctx.textBaseline = 'middle';
                        ctx.fillText(txt, bar.x + 6, bar.y);
                        ctx.restore();
                    });
                }
            }],
            options: {
                responsive: true, maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { legend: { display: false }, datalabels: { display: false } },
                scales: {
                    x: { display: false, max: maxVal * 1.55 },
                    y: { grid: { display: false }, ticks: { font: { size: 11, weight: '600' }, padding: 4 } }
                }
            }
        });
    }

    const ltvOffData = ltvSegOrder.map(s => getSegLTV(s, r => r.org === 1));
    makeLtvChart('cb-ltv-off', ltvOffData, ltvSegOrder, ltvSegColors);

    // E-com LTV
    const ltvEcomData = ltvSegOrder.map(s => getSegLTV(s, r => r.org === 3));
    makeLtvChart('cb-ltv-off-ecom', ltvEcomData, ltvSegOrder, ltvSegColors);

    // Generic weighted avg metric by segment
    function getSegMetric(seg, metric, orgFilter) {
        const rows = segData[seg]?.rows?.filter(r => orgFilter(r)) || [];
        const totalCli = rows.reduce((s, r) => s + (r['клиентов'] || 0), 0);
        const totalVal = rows.reduce((s, r) => s + (r['клиентов'] || 0) * (r[metric] || 0), 0);
        return totalCli > 0 ? totalVal / totalCli : 0;
    }

    function buildHorizBar(canvasId, data, labels, colors, fmtFn) {
        const maxVal = Math.max(...data.filter(v => v > 0), 1);
        destroyChart(canvasId);
        chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
            type: 'bar',
            data: { labels: labels, datasets: [{ data: data, backgroundColor: colors, borderRadius: 4, barThickness: 24, maxBarThickness: 28 }] },
            plugins: [{
                id: 'barValueLabels',
                afterDatasetsDraw(chart) {
                    const ctx = chart.ctx;
                    const meta = chart.getDatasetMeta(0);
                    meta.data.forEach((bar, i) => {
                        const val = chart.data.datasets[0].data[i];
                        if (!val || val <= 0) return;
                        const txt = fmtFn(val);
                        ctx.save();
                        ctx.font = '600 10px Inter, sans-serif';
                        ctx.fillStyle = '#555';
                        ctx.textBaseline = 'middle';
                        ctx.fillText(txt, bar.x + 6, bar.y);
                        ctx.restore();
                    });
                }
            }],
            options: {
                responsive: true, maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: { legend: { display: false }, datalabels: { display: false } },
                scales: {
                    x: { display: false, max: maxVal * 1.55 },
                    y: { grid: { display: false }, ticks: { font: { size: 11, weight: '600' }, padding: 4 } }
                }
            }
        });
    }

    // Delay lower charts so DOM has laid out
    setTimeout(function() {
    // Active segments only (no Отток, Спящие) for ARPU/Check/Freq
    const activeSegs = ['Новые','Активные','Активные LFL','Случайные'];
    const activeColors = activeSegs.map(s => SEG_PIE_COLORS[s] || '#999');

    // ARPU
    buildHorizBar('cb-arpu-off', activeSegs.map(s => getSegMetric(s, 'BUDGET', r => r.org === 1)),
        activeSegs, activeColors, v => Math.round(v).toLocaleString('ru-RU') + ' ₽');
    buildHorizBar('cb-arpu-ecom', activeSegs.map(s => getSegMetric(s, 'BUDGET', r => r.org === 3)),
        activeSegs, activeColors, v => Math.round(v).toLocaleString('ru-RU') + ' ₽');

    // Avg Check
    buildHorizBar('cb-check-off', activeSegs.map(s => getSegMetric(s, 'AVG_CHECK', r => r.org === 1)),
        activeSegs, activeColors, v => Math.round(v).toLocaleString('ru-RU') + ' ₽');
    buildHorizBar('cb-check-ecom', activeSegs.map(s => getSegMetric(s, 'AVG_CHECK', r => r.org === 3)),
        activeSegs, activeColors, v => Math.round(v).toLocaleString('ru-RU') + ' ₽');

    // Checks per client
    buildHorizBar('cb-freq-off', activeSegs.map(s => getSegMetric(s, 'CHECKS', r => r.org === 1)),
        activeSegs, activeColors, v => v.toFixed(1));
    buildHorizBar('cb-freq-ecom', activeSegs.map(s => getSegMetric(s, 'CHECKS', r => r.org === 3)),
        activeSegs, activeColors, v => v.toFixed(1));
    // Bubble charts: ЦИ vs LTV
    function buildBubble(canvasId, orgFilter) {
        const bubbleSegs = ltvSegOrder.filter(s => {
            const ltv = getSegLTV(s, orgFilter);
            const ci = getSegMetric(s, 'ЦИ', orgFilter);
            return ltv > 0 && ci > 0;
        });
        // Scale bubbles: normalize to max client count, min radius 12, max 45
        const clientCounts = bubbleSegs.map(s => {
            const rows = segData[s]?.rows?.filter(r => orgFilter(r)) || [];
            return rows.reduce((sum, r) => sum + (r['клиентов'] || 0), 0);
        });
        const maxClients = Math.max(...clientCounts, 1);
        const bubbleData = bubbleSegs.map((s, i) => ({
            x: getSegMetric(s, 'ЦИ', r => orgFilter(r)),
            y: getSegLTV(s, r => orgFilter(r)),
            r: Math.max(12, Math.sqrt(clientCounts[i] / maxClients) * 45)
        }));

        const xVals = bubbleData.map(d => d.x);
        const xMin = Math.min(...xVals);
        const xMax = Math.max(...xVals);
        const xPad = (xMax - xMin) * 0.35 || 0.03;

        destroyChart(canvasId);
        chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
            type: 'bubble',
            data: {
                datasets: bubbleSegs.map((s, i) => ({
                    label: s,
                    data: [bubbleData[i]],
                    backgroundColor: SEG_PIE_COLORS[s] + 'DD',
                    borderColor: SEG_PIE_COLORS[s],
                    borderWidth: 2,
                }))
            },
            plugins: [ChartDataLabels],
            options: {
                responsive: true, maintainAspectRatio: false,
                layout: { padding: { left: 30, right: 30, top: 30, bottom: 10 } },
                plugins: {
                    legend: { position: 'bottom', labels: { font: { size: 10, weight: '600' }, usePointStyle: true, padding: 12 } },
                    datalabels: {
                        anchor: 'center', align: 'center',
                        font: { size: 9, weight: '700' }, color: '#fff',
                        formatter: (v, ctx) => ctx.dataset.label
                    }
                },
                scales: {
                    x: { title: { display: true, text: 'Ценовой индекс (ЦИ)', font: { size: 11, weight: '600' } }, grid: { color: 'rgba(0,0,0,0.05)' },
                         min: Math.max(0, xMin - xPad), max: xMax + xPad },
                    y: { title: { display: true, text: 'LTV, ₽', font: { size: 11, weight: '600' } }, grid: { color: 'rgba(0,0,0,0.05)' },
                         min: 0, max: Math.max(...bubbleData.map(d => d.y)) * 1.25,
                         ticks: { callback: v => (v/1000).toFixed(0) + 'K' } }
                }
            }
        });
    }
    buildBubble('cb-bubble-off', r => r.org === 1);
    buildBubble('cb-bubble-ecom', r => r.org === 3);

    }, 100); // end setTimeout for lower charts

    // McKinsey Insights
    const topLtvSeg = ltvSegOrder.reduce((best, s) => getSegLTV(s, r => r.org === 1) > getSegLTV(best, r => r.org === 1) ? s : best, ltvSegOrder[0]);
    const topLtv = Math.round(getSegLTV(topLtvSeg, r => r.org === 1)).toLocaleString('ru-RU');
    const omniPctOff = (grandOmni / grandOffline * 100).toFixed(1);
    const omniPctEcom = grandEcomAll > 0 ? (ecomOmni / grandEcomAll * 100).toFixed(1) : '0';
    const pushPctTotal = (pushTotal / grandTotal * 100).toFixed(0);

    document.getElementById('cb-insights').innerHTML = `
        <div class="rpt-insight-headline">
            Клиентская база ${fmt.int(grandTotal)} | Offline ${fmt.int(grandOffline)} + E-com ${fmt.int(grandEcomAll)} | Доступность PUSH ${pushPctTotal}%
        </div>
        <div class="rpt-insights-grid">
            <div class="rpt-insight-block" style="border-left:3px solid #E87722">
                <div class="rpt-ib-title"><span class="tag tag-growth">LTV</span> Ценность сегментов</div>
                <ul>
                    <li>Наивысший LTV: <strong>${topLtvSeg}</strong> — ${topLtv} ₽ (Offline)</li>
                    <li>Омни-клиенты генерируют в 1.2-1.5x больший LTV чем pure-offline — фокус на конвертацию в омни</li>
                    <li>E-com LTV ниже offline — потенциал кросс-канальных программ лояльности</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #4A90D9">
                <div class="rpt-ib-title"><span class="tag tag-attention">ОМНИ</span> Конвертация каналов</div>
                <ul>
                    <li>Доля омни в Offline: <strong>${omniPctOff}%</strong> — низкий уровень, потенциал роста x3-5</li>
                    <li>Доля омни в E-com: <strong>${omniPctEcom}%</strong> — большинство e-com клиентов уже омни</li>
                    <li>Рекомендация: стимулировать offline-only клиентов к первой online-покупке</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #2E8B57">
                <div class="rpt-ib-title"><span class="tag tag-rec">PUSH</span> Коммуникации</div>
                <ul>
                    <li>Общая доступность PUSH: <strong>${pushPctTotal}%</strong> от базы — ${pushPctTotal < 50 ? 'ниже целевого уровня 60%' : 'приемлемый уровень'}</li>
                    <li>E-com Push-доступность выше (${grandEcomAll > 0 ? (pushEcom/grandEcomAll*100).toFixed(0) : 0}%) — использовать для cross-sell offline</li>
                    <li>Приоритет: увеличить Push-подписку в сегментах «Новые» и «Активные»</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #C41E3A">
                <div class="rh-ib-title"><span class="tag tag-risk">ДЕЙСТВИЯ</span></div>
                <ul>
                    <li><strong>Конвертация в омни:</strong> push-кампания для offline-only с первым заказом на e-com</li>
                    <li><strong>Retention Спящие:</strong> реактивация через персонализированные скидки (28.9% базы Offline!)</li>
                    <li><strong>Upsell Активные→LFL:</strong> программа лояльности для увеличения частоты и перевода в ядро</li>
                    <li><strong>Data enrichment:</strong> увеличить долю Push-доступных через мотивацию к подписке</li>
                </ul>
            </div>
        </div>
    `;

    // Table
    const tbl = document.getElementById('cb-table');
    // Helper: % participation (count / clients)
    const pct = (n, d) => (n != null && d) ? (n/d*100).toFixed(1)+'%' : '—';

    tbl.innerHTML = `
        <thead>
            <tr>
                <th rowspan="2">Платформа</th>
                <th rowspan="2">Сегмент</th>
                <th rowspan="2">Канал</th>
                <th rowspan="2">Клиентов</th>
                <th colspan="2" class="cb-grp cb-grp-comm">Коммуникации</th>
                <th rowspan="2">ЦИ</th>
                <th rowspan="2">ARPU</th>
                <th rowspan="2">Чеков</th>
                <th rowspan="2">Ср. чек</th>
                <th rowspan="2">Реал. скидка</th>
                <th rowspan="2">LTV</th>
                <th colspan="6" class="cb-grp cb-grp-mech">Участие в CVM-механиках, % сегмента</th>
            </tr>
            <tr>
                <th class="cb-grp cb-grp-comm">PUSH</th>
                <th class="cb-grp cb-grp-comm">% дост.</th>
                <th class="cb-grp cb-grp-mech" title="Любимый продукт — выбрал">FAV прод. <small>выбор</small></th>
                <th class="cb-grp cb-grp-mech" title="Любимый продукт — использовал">FAV прод. <small>исп.</small></th>
                <th class="cb-grp cb-grp-mech" title="Любимая категория — выбрал">FAV кат. <small>выбор</small></th>
                <th class="cb-grp cb-grp-mech" title="Любимая категория — использовал">FAV кат. <small>исп.</small></th>
                <th class="cb-grp cb-grp-mech" title="Использовал персональные предложения">ПП исп.</th>
                <th class="cb-grp cb-grp-mech" title="Участник CVM-кампаний">CVM</th>
            </tr>
        </thead>
        <tbody>
            ${CVM_CONTACTS.map(c => {
                const cli = c['клиентов'] || 0;
                return `<tr>
                <td>${(c.org === 3 ? 'E-commerce' : 'Offline')}</td>
                <td style="text-align:left">${c.segment}</td>
                <td>${c.channel}</td>
                <td>${fmt.int(cli)}</td>
                <td>${fmt.int(c.PUSH)}</td>
                <td>${c['Доля доступных для коммуникации'] != null ? (c['Доля доступных для коммуникации']*100).toFixed(1)+'%' : '—'}</td>
                <td>${c['ЦИ'] != null ? c['ЦИ'].toFixed(3) : '—'}</td>
                <td>${fmt.rub(c.BUDGET)}</td>
                <td>${c.CHECKS != null ? c.CHECKS.toFixed(1) : '—'}</td>
                <td>${fmt.rub(c.AVG_CHECK)}</td>
                <td>${c.REAL_SALE != null ? (c.REAL_SALE*100).toFixed(1)+'%' : '—'}</td>
                <td>${c.LTV != null ? Math.round(c.LTV).toLocaleString('ru-RU')+' ₽' : '—'}</td>
                <td class="cb-mech-cell">${pct(c.FAV_PRODUCT_CHOICE, cli)}</td>
                <td class="cb-mech-cell">${pct(c.FAV_PRODUCT_USE, cli)}</td>
                <td class="cb-mech-cell">${pct(c.FAV_CATEGORY_CHOICE, cli)}</td>
                <td class="cb-mech-cell">${pct(c.FAV_CATEGORY_USE, cli)}</td>
                <td class="cb-mech-cell">${pct(c.PP_USE, cli)}</td>
                <td class="cb-mech-cell">${pct(c.CVM_PARTICIPANT, cli)}</td>
            </tr>`;}).join('')}
        </tbody>
    `;

    // Excel export
    document.getElementById('cb-export-btn')?.addEventListener('click', function() {
        const table = document.getElementById('cb-table');
        let csv = '';
        const rows = table.querySelectorAll('tr');
        rows.forEach(row => {
            const cells = row.querySelectorAll('th, td');
            const rowData = [];
            cells.forEach(cell => rowData.push('"' + cell.textContent.replace(/"/g, '""').trim() + '"'));
            csv += rowData.join(';') + '\n';
        });
        const BOM = '\uFEFF';
        const blob = new Blob([BOM + csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'база_клиентов_детализация.csv';
        a.click();
        URL.revokeObjectURL(url);
    });
}

// --- CVM Dashboard ---
function buildCVM(filterMonth) {
    const container = document.getElementById('tab-cvm');
    container.innerHTML = '';

    const allMonths = [...new Set(CVM_ALL.map(p => p.MONTH))].sort();
    filterMonth = filterMonth || allMonths[allMonths.length - 1] || 'all';

    // Filter data
    const filtered = filterMonth === 'all' ? CVM_ALL : CVM_ALL.filter(p => p.MONTH === filterMonth);
    const off = filtered.filter(p => p.ID_ORGANIZATION === 1);
    const onl = filtered.filter(p => p.ID_ORGANIZATION === 3);

    const offTO = off.reduce((s, p) => s + (p['Доп ТО с НДС'] || 0), 0);
    const onlTO = onl.reduce((s, p) => s + (p['Доп ТО с НДС'] || 0), 0);
    const totalTO = offTO + onlTO;
    const offCli = off.reduce((s, p) => s + (p['Доп клиенты'] || 0), 0);
    const onlCli = onl.reduce((s, p) => s + (p['Доп клиенты'] || 0), 0);
    const responders = off.filter(p => p['Отклик'] > 0);
    const avgResp = responders.length ? responders.reduce((s, p) => s + p['Отклик'], 0) / responders.length : 0;

    // Unique promos (by name) with off + online combined
    const promoMap = {};
    filtered.forEach(p => {
        const key = p['Номер промо'] + '_' + p['Сегмент'];
        if (!promoMap[key]) promoMap[key] = { name: p['Название промо'], seg: p['Сегмент'], month: p.MONTH_LABEL, start: p['Старт'], end: p['Окончание'], offTO: 0, onlTO: 0, offCli: 0, onlCli: 0, offResp: null, onlResp: null, offCheck: null, onlCheck: null, offBudget: null, выборка: 0 };
        const m = promoMap[key];
        if (p.ID_ORGANIZATION === 1) { m.offTO += p['Доп ТО с НДС'] || 0; m.offCli += p['Доп клиенты'] || 0; m.offResp = p['Отклик']; m.offCheck = p['Ср чек']; m.offBudget = p['Бюджет']; m.выборка = p['Выборка'] || 0; if (!m.start || p['Старт'] > m.start) { m.start = p['Старт']; m.end = p['Окончание']; } }
        if (p.ID_ORGANIZATION === 3) { m.onlTO += p['Доп ТО с НДС'] || 0; m.onlCli += p['Доп клиенты'] || 0; m.onlResp = p['Отклик']; m.onlCheck = p['Ср чек']; m.onlBudget = p['Бюджет']; }
    });
    // Sort by date descending (latest first), then by total TO
    const promoList = Object.values(promoMap).sort((a, b) => {
        const da = a.start || ''; const db = b.start || '';
        if (da !== db) return db.localeCompare(da);
        return (b.offTO + b.onlTO) - (a.offTO + a.onlTO);
    });

    // Segment summary (off + online side by side)
    const segSummary = {};
    filtered.forEach(p => {
        const seg = p['Сегмент'];
        if (!segSummary[seg]) segSummary[seg] = { offTO: 0, onlTO: 0, offCli: 0, onlCli: 0 };
        if (p.ID_ORGANIZATION === 1) { segSummary[seg].offTO += p['Доп ТО с НДС'] || 0; segSummary[seg].offCli += p['Доп клиенты'] || 0; }
        if (p.ID_ORGANIZATION === 3) { segSummary[seg].onlTO += p['Доп ТО с НДС'] || 0; segSummary[seg].onlCli += p['Доп клиенты'] || 0; }
    });
    const segNames = Object.keys(segSummary).sort((a, b) => (segSummary[b].offTO + segSummary[b].onlTO) - (segSummary[a].offTO + segSummary[a].onlTO));

    // Efficiency: TO per client (ROI proxy)
    const topEfficiency = promoList.filter(p => (p.offCli + p.onlCli) > 50).map(p => ({
        name: p.name, seg: p.seg,
        toPerClient: (p.offTO + p.onlTO) / (p.offCli + p.onlCli),
        totalTO: p.offTO + p.onlTO,
        totalCli: p.offCli + p.onlCli
    })).sort((a, b) => b.toPerClient - a.toPerClient).slice(0, 10);

    const monthOpts = '<option value="all"' + (filterMonth === 'all' ? ' selected' : '') + '>Все месяцы</option>' +
        allMonths.map(m => {
            const lbl = CVM_ALL.find(p => p.MONTH === m)?.MONTH_LABEL || m;
            return '<option value="' + m + '"' + (filterMonth === m ? ' selected' : '') + '>' + lbl + '</option>';
        }).join('');

    container.innerHTML = `
        <div class="page-header">
            <div>
                <h1>CVM — эффективность промо</h1>
                <p class="subtitle">Результаты CVM-кампаний | Offline + E-comm</p>
            </div>
            <div class="header-meta">
                <select id="cvm-month-filter" class="region-selector">${monthOpts}</select>
            </div>
        </div>

        <div class="report-highlights" style="grid-template-columns:repeat(5,1fr)">
            <div class="report-highlight-card" style="border-left:4px solid #E87722">
                <div class="rh-label">Доп. ТО (общий)</div>
                <div class="rh-value" style="color:#E87722">${(totalTO/1e6).toFixed(1)}M ₽</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #E87722">
                <div class="rh-label">Доп. ТО Offline</div>
                <div class="rh-value" style="color:#C67A2E">${(offTO/1e6).toFixed(1)}M ₽</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #4A90D9">
                <div class="rh-label">Доп. ТО Online</div>
                <div class="rh-value" style="color:#4A90D9">${(onlTO/1e6).toFixed(1)}M ₽</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #2E8B57">
                <div class="rh-label">Доп. клиенты</div>
                <div class="rh-value" style="color:#2E8B57">${fmt.int(offCli + onlCli)}</div>
            </div>
            <div class="report-highlight-card" style="border-left:4px solid #7B61FF">
                <div class="rh-label">Ср. отклик</div>
                <div class="rh-value" style="color:#7B61FF">${(avgResp*100).toFixed(2)}%</div>
            </div>
        </div>

        <!-- Segment summary: off vs online -->
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>Доп. ТО по сегментам: Offline vs Online</h3></div>
                <div class="chart-container" style="height:340px"><canvas id="cvm-seg-to"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>Доп. клиенты по сегментам</h3></div>
                <div class="chart-container" style="height:340px"><canvas id="cvm-seg-clients"></canvas></div>
            </div>
        </div>

        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>TOP-10 промо по доп. ТО (Off + Online)</h3></div>
                <div class="chart-container" style="height:340px"><canvas id="cvm-top-promos"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>TOP-10 промо по эффективности (₽ / клиент)</h3></div>
                <div class="chart-container" style="height:340px"><canvas id="cvm-efficiency"></canvas></div>
            </div>
        </div>

        <!-- Segment × Channel summary table -->
        <div class="card">
            <div class="card-header"><h3>Сводная: доп. ТО по сегментам и каналам</h3></div>
            <div class="table-wrap"><table class="data-table" id="cvm-seg-table"></table></div>
        </div>

        <!-- Full promo table -->
        <div class="card">
            <div class="card-header"><h3>Все промо-кампании (Offline + Online)</h3></div>
            <div class="table-wrap"><table class="data-table" id="cvm-table"></table></div>
        </div>

        <!-- McKinsey insights -->
        <div class="card report-insights-card">
            <div class="card-header"><h3 style="color:#E87722">Стратегический анализ CVM-кампаний</h3></div>
            <div class="report-insights" id="cvm-insights"></div>
        </div>
    `;

    // Wire month filter
    document.getElementById('cvm-month-filter').addEventListener('change', function() { buildCVM(this.value); });

    // --- Charts ---
    const segOffTO = segNames.map(s => segSummary[s].offTO);
    const segOnlTO = segNames.map(s => segSummary[s].onlTO);

    destroyChart('cvm-seg-to');
    chartInstances['cvm-seg-to'] = new Chart(document.getElementById('cvm-seg-to'), {
        type: 'bar',
        data: { labels: segNames, datasets: [
            { label: 'Offline', data: segOffTO, backgroundColor: '#E87722', borderRadius: 3 },
            { label: 'Online', data: segOnlTO, backgroundColor: '#4A90D9', borderRadius: 3 },
        ]},
        plugins: [ChartDataLabels],
        options: {
            responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            plugins: { legend: { position: 'top' }, datalabels: { display: false } },
            scales: { x: { stacked: true, ticks: { callback: v => (v/1e6).toFixed(0) + 'M' } }, y: { stacked: true, grid: { display: false } } }
        }
    });

    destroyChart('cvm-seg-clients');
    chartInstances['cvm-seg-clients'] = new Chart(document.getElementById('cvm-seg-clients'), {
        type: 'bar',
        data: { labels: segNames, datasets: [
            { label: 'Offline', data: segNames.map(s => segSummary[s].offCli), backgroundColor: '#E87722', borderRadius: 3 },
            { label: 'Online', data: segNames.map(s => segSummary[s].onlCli), backgroundColor: '#4A90D9', borderRadius: 3 },
        ]},
        plugins: [ChartDataLabels],
        options: {
            responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            plugins: { legend: { position: 'top' }, datalabels: { display: false } },
            scales: { x: { stacked: true, ticks: { callback: v => (v/1000).toFixed(0) + 'K' } }, y: { stacked: true, grid: { display: false } } }
        }
    });

    // Top 10 promos
    const top10 = promoList.slice(0, 10);
    destroyChart('cvm-top-promos');
    chartInstances['cvm-top-promos'] = new Chart(document.getElementById('cvm-top-promos'), {
        type: 'bar',
        data: {
            labels: top10.map(p => (p.name || '').slice(0, 28)),
            datasets: [
                { label: 'Offline', data: top10.map(p => p.offTO), backgroundColor: '#E87722', borderRadius: 3 },
                { label: 'Online', data: top10.map(p => p.onlTO), backgroundColor: '#4A90D9', borderRadius: 3 },
            ]
        },
        plugins: [ChartDataLabels],
        options: {
            responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            plugins: { legend: { position: 'top' }, datalabels: { display: false } },
            scales: { x: { stacked: true, ticks: { callback: v => (v/1e6).toFixed(1) + 'M' } }, y: { grid: { display: false } } }
        }
    });

    // Efficiency chart
    destroyChart('cvm-efficiency');
    chartInstances['cvm-efficiency'] = new Chart(document.getElementById('cvm-efficiency'), {
        type: 'bar',
        data: {
            labels: topEfficiency.map(p => (p.name || '').slice(0, 28)),
            datasets: [{ data: topEfficiency.map(p => p.toPerClient), backgroundColor: '#2E8B57', borderRadius: 3 }]
        },
        plugins: [ChartDataLabels],
        options: {
            responsive: true, maintainAspectRatio: false, indexAxis: 'y',
            plugins: { legend: { display: false }, datalabels: {
                anchor: 'end', align: 'right', font: { size: 9, weight: '600' },
                formatter: v => Math.round(v).toLocaleString('ru-RU') + ' ₽'
            }},
            scales: { x: { ticks: { callback: v => (v/1000).toFixed(0) + 'K' } }, y: { grid: { display: false } } }
        }
    });

    // Segment × Channel summary table
    document.getElementById('cvm-seg-table').innerHTML = `
        <thead><tr><th>Сегмент</th><th>Offline ТО</th><th>Online ТО</th><th>Всего ТО</th><th>Off клиенты</th><th>Onl клиенты</th><th>Доля Online</th></tr></thead>
        <tbody>${segNames.map(s => {
            const d = segSummary[s];
            const total = d.offTO + d.onlTO;
            return `<tr>
                <td style="text-align:left">${s}</td>
                <td>${Math.round(d.offTO).toLocaleString('ru-RU')} ₽</td>
                <td>${Math.round(d.onlTO).toLocaleString('ru-RU')} ₽</td>
                <td><strong>${Math.round(total).toLocaleString('ru-RU')} ₽</strong></td>
                <td>${fmt.int(d.offCli)}</td>
                <td>${fmt.int(d.onlCli)}</td>
                <td>${total > 0 ? (d.onlTO/total*100).toFixed(1)+'%' : '—'}</td>
            </tr>`;
        }).join('')}
        <tr style="font-weight:700;border-top:2px solid var(--border)">
            <td style="text-align:left">ИТОГО</td>
            <td>${Math.round(offTO).toLocaleString('ru-RU')} ₽</td>
            <td>${Math.round(onlTO).toLocaleString('ru-RU')} ₽</td>
            <td><strong>${Math.round(totalTO).toLocaleString('ru-RU')} ₽</strong></td>
            <td>${fmt.int(offCli)}</td>
            <td>${fmt.int(onlCli)}</td>
            <td>${totalTO > 0 ? (onlTO/totalTO*100).toFixed(1)+'%' : '—'}</td>
        </tr></tbody>
    `;

    // Full table — sorted latest first
    function fmtDate(d) { if (!d) return '—'; return d.slice(8,10) + '.' + d.slice(5,7) + '.' + d.slice(2,4); }
    document.getElementById('cvm-table').innerHTML = `
        <thead><tr>
            <th>Старт</th><th>Окончание</th><th>Промо</th><th>Сегмент</th><th>Выборка</th>
            <th>Off ТО</th><th>Onl ТО</th><th>Всего ТО</th>
            <th>Off кл.</th><th>Onl кл.</th><th>Отклик</th><th>Ср. чек</th>
        </tr></thead>
        <tbody>${promoList.map(p => `<tr>
            <td>${fmtDate(p.start)}</td>
            <td>${fmtDate(p.end)}</td>
            <td style="text-align:left;max-width:180px;overflow:hidden;text-overflow:ellipsis">${p.name}</td>
            <td>${p.seg}</td>
            <td>${fmt.int(p.выборка)}</td>
            <td>${Math.round(p.offTO).toLocaleString('ru-RU')}</td>
            <td>${Math.round(p.onlTO).toLocaleString('ru-RU')}</td>
            <td><strong>${Math.round(p.offTO + p.onlTO).toLocaleString('ru-RU')}</strong></td>
            <td>${fmt.int(p.offCli)}</td>
            <td>${fmt.int(p.onlCli)}</td>
            <td>${p.offResp != null ? (p.offResp*100).toFixed(2)+'%' : '—'}</td>
            <td>${p.offCheck != null ? fmt.rub(p.offCheck) : '—'}</td>
        </tr>`).join('')}</tbody>
    `;

    // McKinsey-level insights
    const topPromo = promoList[0];
    const offShare = totalTO > 0 ? (offTO / totalTO * 100).toFixed(0) : 0;
    const topSegName = segNames[0];
    const topSegTotal = segSummary[topSegName].offTO + segSummary[topSegName].onlTO;
    const topSegShare = totalTO > 0 ? (topSegTotal / totalTO * 100).toFixed(0) : 0;
    const bestEffName = topEfficiency.length ? topEfficiency[0].name : '—';
    const bestEffVal = topEfficiency.length ? Math.round(topEfficiency[0].toPerClient).toLocaleString('ru-RU') : '—';

    document.getElementById('cvm-insights').innerHTML = `
        <div class="rpt-insight-headline">
            CVM-кампании генерируют ${(totalTO/1e6).toFixed(0)} млн ₽ доп. ТО | Offline ${offShare}% vs Online ${100 - parseInt(offShare)}%
        </div>
        <div class="rpt-insights-grid">
            <div class="rpt-insight-block" style="border-left:3px solid #E87722">
                <div class="rpt-ib-title"><span class="tag tag-growth">ЭФФЕКТИВНОСТЬ</span> Каналы</div>
                <ul>
                    <li>Offline генерирует <strong>${(offTO/1e6).toFixed(1)}M ₽</strong> (${offShare}% от общего доп. ТО)</li>
                    <li>Online канал — <strong>${(onlTO/1e6).toFixed(1)}M ₽</strong>, потенциал роста при масштабировании digital CRM</li>
                    <li>Доп. клиенты offline: <strong>${fmt.int(offCli)}</strong> vs online: <strong>${fmt.int(onlCli)}</strong></li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #7B61FF">
                <div class="rpt-ib-title"><span class="tag tag-attention">СЕГМЕНТЫ</span> Концентрация</div>
                <ul>
                    <li>Сегмент «${topSegName}» = <strong>${topSegShare}%</strong> всего доп. ТО — высокая концентрация</li>
                    <li>Рекомендуется диверсификация промо на менее охваченные сегменты</li>
                    <li>Средний отклик <strong>${(avgResp*100).toFixed(2)}%</strong> — потенциал для роста через персонализацию</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #2E8B57">
                <div class="rpt-ib-title"><span class="tag tag-rec">TOP ПРОМО</span> Лучшие кампании</div>
                <ul>
                    <li>Лидер по ТО: «${topPromo?.name?.slice(0,35)}» — <strong>${((topPromo?.offTO||0)+(topPromo?.onlTO||0))/1e6 > 1 ? (((topPromo?.offTO||0)+(topPromo?.onlTO||0))/1e6).toFixed(1)+'M' : Math.round((topPromo?.offTO||0)+(topPromo?.onlTO||0)).toLocaleString('ru-RU')} ₽</strong></li>
                    <li>Лидер по эффективности: «${bestEffName?.slice(0,35)}» — <strong>${bestEffVal} ₽/клиент</strong></li>
                    <li>Рекомендация: масштабировать TOP-5 промо с высокой эффективностью на ₽/клиент</li>
                </ul>
            </div>
            <div class="rpt-insight-block" style="border-left:3px solid #C41E3A">
                <div class="rpt-ib-title"><span class="tag tag-risk">РЕКОМЕНДАЦИИ</span></div>
                <ul>
                    <li><strong>Персонализация:</strong> повысить отклик с ${(avgResp*100).toFixed(2)}% до 1.5-2% через ML-таргетинг</li>
                    <li><strong>Омниканальность:</strong> синхронизировать CVM offline + online для единого customer journey</li>
                    <li><strong>A/B тестирование:</strong> тестировать механики с высоким ₽/клиент на новых сегментах</li>
                    <li><strong>ROI-оптимизация:</strong> перераспределить бюджет в пользу промо с лучшим ТО/клиент</li>
                </ul>
            </div>
        </div>
    `;
}

// --- Динамика базы ---
function buildDynamics() {
    const container = document.getElementById('tab-dynamics');
    const DYN_SEG_COLORS = UNIFIED_SEG_COLORS;
    const SEG_ORDER = ['Новые','Активные','Отток','Спящие'];
    const SEG_ORDER_WITH_FRAUD = ['Новые','Активные','Отток','Спящие','Фрод'];

    // Get available dates for org=1 (offline)
    const offlineDates = [...new Set(CVM_DYNAMICS.filter(r => r.org === 1).map(r => r.date))].sort();
    const ecommDates = [...new Set(CVM_DYNAMICS.filter(r => r.org === 3).map(r => r.date))].sort();
    const allDates = [...new Set([...offlineDates, ...ecommDates])].sort();

    // Build date labels
    const monthNames = ['','Янв','Фев','Мар','Апр','Май','Июн','Июл','Авг','Сен','Окт','Ноя','Дек'];
    function dateLabel(d) {
        const parts = d.split('-');
        return monthNames[parseInt(parts[1])] + ' ' + parts[0];
    }

    // Build content inside pre-existing HTML containers
    const offPanel = document.getElementById('dyn-offline');
    const ecomPanel = document.getElementById('dyn-ecom');

    offPanel.innerHTML = `
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card" id="dyn-pie-off-wrap"></div>
            <div class="card" id="dyn-bar-off-wrap"></div>
            <div class="insight-col" id="dyn-structure-insight-off"></div>
        </div>
        <div class="grid-3-insight" id="dyn-ts-off-row1" style="margin-top:16px"></div>
        <div class="grid-3-insight" id="dyn-ts-off-row2" style="margin-top:16px"></div>
        <div class="grid-3-insight" id="dyn-ts-off-row3" style="margin-top:16px"></div>
        ${[4,5,6,7,8].map(i => `<div class="grid-3-insight" id="dyn-ts-off-row${i}" style="margin-top:16px"></div>`).join('\n        ')}
        <div class="card" id="dyn-conclusions-off" style="margin-top:16px"></div>
    `;
    ecomPanel.innerHTML = `
        <div class="grid-3-insight" style="margin-top:16px">
            <div class="card" id="dyn-pie-ecom-wrap"></div>
            <div class="card" id="dyn-bar-ecom-wrap"></div>
            <div class="insight-col" id="dyn-structure-insight-ecom"></div>
        </div>
        ${[1,2,3,4,5,6,7,8].map(i => `<div class="grid-3-insight" id="dyn-ts-ecom-row${i}" style="margin-top:16px"></div>`).join('\n        ')}
        <div class="card" id="dyn-conclusions-ecom" style="margin-top:16px"></div>
    `;

    const crossPanel = document.getElementById('dyn-cross');
    crossPanel.innerHTML = `
        ${[1,2,3,4,5,6,7].map(i => `<div class="grid-3-insight" id="dyn-ts-cross-row${i}" style="margin-top:16px"></div>`).join('\n        ')}
    `;

    // Sub-tab switching
    container.querySelectorAll('.sub-tab').forEach(btn => {
        btn.addEventListener('click', () => {
            container.querySelectorAll('.sub-tab').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const target = btn.dataset.subtab;
            ['dyn-offline','dyn-ecom','dyn-cross'].forEach(id => {
                const el = document.getElementById(id);
                if (el) { el.style.display = target === id ? '' : 'none'; el.classList.toggle('active', target === id); }
            });
            // Re-render to fix Chart.js hidden container issue
            renderDynMonth(sel.value);
            // Force resize all visible charts
            setTimeout(() => {
                Object.values(chartInstances).forEach(c => { try { c.resize(); } catch {} });
            }, 50);
        });
    });

    // Populate month selector with all unique dates
    const sel = document.getElementById('dyn-month-selector');
    allDates.forEach(d => {
        const opt = document.createElement('option');
        opt.value = d;
        opt.textContent = dateLabel(d);
        sel.appendChild(opt);
    });
    sel.value = allDates[allDates.length - 1];
    sel.addEventListener('change', () => renderDynMonth(sel.value));

    function getData(org, date) {
        return CVM_DYNAMICS.filter(r => r.org === org && r.date === date);
    }

    function renderDynMonth(date) {
        const offRows = getData(1, date);
        const ecomRows = getData(3, date);

        // Offline: total = все каналы (только оффлайн + омни)
        const offBySegAll = {};
        offRows.forEach(r => {
            if (!offBySegAll[r.segment]) offBySegAll[r.segment] = { clients: 0, push: 0, budget: null, ltv: null, checks: null, avgCheck: null, pushAvail: 0, clientsForPush: 0 };
            offBySegAll[r.segment].clients += r['клиентов'];
            offBySegAll[r.segment].push += r.PUSH;
            if (r['Доля доступных для коммуникации'] != null) {
                offBySegAll[r.segment].pushAvail += r['клиентов'] * r['Доля доступных для коммуникации'];
                offBySegAll[r.segment].clientsForPush += r['клиентов'];
            }
            // Weighted ARPU
            if (r.BUDGET != null) {
                if (offBySegAll[r.segment].budget == null) offBySegAll[r.segment].budget = 0;
                offBySegAll[r.segment].budget += r['клиентов'] * r.BUDGET;
            }
            if (r.LTV != null) {
                if (offBySegAll[r.segment].ltv == null) offBySegAll[r.segment].ltv = 0;
                offBySegAll[r.segment].ltv += r['клиентов'] * r.LTV;
            }
            if (r.CHECKS != null) {
                if (offBySegAll[r.segment].checks == null) offBySegAll[r.segment].checks = 0;
                offBySegAll[r.segment].checks += r['клиентов'] * r.CHECKS;
            }
            if (r.AVG_CHECK != null) {
                if (offBySegAll[r.segment].avgCheck == null) offBySegAll[r.segment].avgCheck = 0;
                offBySegAll[r.segment].avgCheck += r['клиентов'] * r.AVG_CHECK;
            }
        });
        // Weighted averages
        Object.values(offBySegAll).forEach(s => {
            if (s.budget != null && s.clients > 0) s.budget /= s.clients;
            if (s.ltv != null && s.clients > 0) s.ltv /= s.clients;
            if (s.checks != null && s.clients > 0) s.checks /= s.clients;
            if (s.avgCheck != null && s.clients > 0) s.avgCheck /= s.clients;
            if (s.clientsForPush > 0) s.pushShare = s.pushAvail / s.clientsForPush;
        });

        // Ecomm: total = все каналы (только e-commerce + омни)
        const ecomBySegAll = {};
        ecomRows.forEach(r => {
            if (!ecomBySegAll[r.segment]) ecomBySegAll[r.segment] = { clients: 0, push: 0, budget: null, ltv: null, checks: null, avgCheck: null, pushAvail: 0, clientsForPush: 0 };
            ecomBySegAll[r.segment].clients += r['клиентов'];
            ecomBySegAll[r.segment].push += r.PUSH;
            if (r['Доля доступных для коммуникации'] != null) {
                ecomBySegAll[r.segment].pushAvail += r['клиентов'] * r['Доля доступных для коммуникации'];
                ecomBySegAll[r.segment].clientsForPush += r['клиентов'];
            }
            if (r.BUDGET != null) {
                if (ecomBySegAll[r.segment].budget == null) ecomBySegAll[r.segment].budget = 0;
                ecomBySegAll[r.segment].budget += r['клиентов'] * r.BUDGET;
            }
            if (r.LTV != null) {
                if (ecomBySegAll[r.segment].ltv == null) ecomBySegAll[r.segment].ltv = 0;
                ecomBySegAll[r.segment].ltv += r['клиентов'] * r.LTV;
            }
            if (r.CHECKS != null) {
                if (ecomBySegAll[r.segment].checks == null) ecomBySegAll[r.segment].checks = 0;
                ecomBySegAll[r.segment].checks += r['клиентов'] * r.CHECKS;
            }
            if (r.AVG_CHECK != null) {
                if (ecomBySegAll[r.segment].avgCheck == null) ecomBySegAll[r.segment].avgCheck = 0;
                ecomBySegAll[r.segment].avgCheck += r['клиентов'] * r.AVG_CHECK;
            }
        });
        Object.values(ecomBySegAll).forEach(s => {
            if (s.budget != null && s.clients > 0) s.budget /= s.clients;
            if (s.ltv != null && s.clients > 0) s.ltv /= s.clients;
            if (s.checks != null && s.clients > 0) s.checks /= s.clients;
            if (s.avgCheck != null && s.clients > 0) s.avgCheck /= s.clients;
            if (s.clientsForPush > 0) s.pushShare = s.pushAvail / s.clientsForPush;
        });

        const totalOff = SEG_ORDER.reduce((s, seg) => s + (offBySegAll[seg]?.clients || 0), 0);
        const totalEcom = SEG_ORDER.reduce((s, seg) => s + (ecomBySegAll[seg]?.clients || 0), 0);

        // Omni share: clients in omni channel / total offline
        const omniOffClients = offRows.filter(r => r.channel === 'омни').reduce((s, r) => s + r['клиентов'], 0);
        const omniEcomClients = ecomRows.filter(r => r.channel === 'омни').reduce((s, r) => s + r['клиентов'], 0);
        const omniShareOff = totalOff > 0 ? omniOffClients / totalOff : null;
        const omniShareEcom = totalEcom > 0 ? omniEcomClients / totalEcom : null;

        const dl = dateLabel(date);

        // --- Shared KPI panel (like clientbase: 5 cards) ---
        // Avoid double-counting omni: total = offline(offline+omni) + ecom-only
        const ecomOnlyClients = ecomRows.filter(r => r.channel === 'только e-commerce').reduce((s, r) => s + r['клиентов'], 0);
        const grandTotal = totalOff + ecomOnlyClients;
        const pushOff = offRows.reduce((s, r) => s + (r.PUSH || 0), 0);
        const pushEcom = ecomRows.reduce((s, r) => s + (r.PUSH || 0), 0);
        const pushEcomOnly = ecomRows.filter(r => r.channel === 'только e-commerce').reduce((s, r) => s + (r.PUSH || 0), 0);
        const pushTotal = pushOff + pushEcomOnly;

        // Weighted metrics for comparison
        function weightedMetric(rows, seg, field) {
            const segRows = rows.filter(r => r.segment === seg && r[field] != null);
            const totalCli = segRows.reduce((s, r) => s + r['клиентов'], 0);
            const totalVal = segRows.reduce((s, r) => s + r['клиентов'] * r[field], 0);
            return totalCli > 0 ? totalVal / totalCli : null;
        }
        const offAvgCheck = weightedMetric(offRows, 'Активные', 'AVG_CHECK');
        const ecomAvgCheck = weightedMetric(ecomRows, 'Активные', 'AVG_CHECK');
        const offChecks = weightedMetric(offRows, 'Активные', 'CHECKS');
        const ecomChecks = weightedMetric(ecomRows, 'Активные', 'CHECKS');

        document.getElementById('dyn-kpi-cards').innerHTML = `
            <div class="report-highlights" style="grid-template-columns:1fr 1fr 1fr 1.1fr 1.1fr 0.85fr 0.85fr">
                <div class="report-highlight-card" style="border-left:4px solid #E87722">
                    <div class="rh-label">Всего клиентов</div>
                    <div class="rh-value" style="color:#E87722">${fmt.int(grandTotal)}</div>
                    <div class="rh-desc">PUSH: ${fmt.int(pushTotal)} (${grandTotal > 0 ? (pushTotal/grandTotal*100).toFixed(0) : 0}%)</div>
                </div>
                <div class="report-highlight-card" style="border-left:4px solid #C67A2E">
                    <div class="rh-label">Клиентов Offline</div>
                    <div class="rh-value" style="color:#C67A2E">${fmt.int(totalOff)}</div>
                    <div class="rh-desc">PUSH: ${fmt.int(pushOff)} (${totalOff > 0 ? (pushOff/totalOff*100).toFixed(0) : 0}%)</div>
                </div>
                <div class="report-highlight-card" style="border-left:4px solid #2E8B57">
                    <div class="rh-label">E-commerce клиенты</div>
                    <div class="rh-value" style="color:#2E8B57">${fmt.int(totalEcom)}</div>
                    <div class="rh-desc">PUSH: ${fmt.int(pushEcom)} (${totalEcom > 0 ? (pushEcom/totalEcom*100).toFixed(0) : 0}%)</div>
                </div>
                <div class="report-highlight-card" style="border-left:4px solid #7B61FF">
                    <div class="rh-label">Омни в OFFLINE</div>
                    <div class="rh-value" style="color:#7B61FF">${omniShareOff != null ? (omniShareOff*100).toFixed(1)+'%' : '—'}</div>
                    <div class="rh-desc">${fmt.int(omniOffClients)} клиентов</div>
                </div>
                <div class="report-highlight-card" style="border-left:4px solid #4A90D9">
                    <div class="rh-label">Омни в E-COMMERCE</div>
                    <div class="rh-value" style="color:#4A90D9">${omniShareEcom != null ? (omniShareEcom*100).toFixed(1)+'%' : '—'}</div>
                    <div class="rh-desc">${fmt.int(omniEcomClients)} клиентов</div>
                </div>
                <div class="report-highlight-card" style="border-left:4px solid #555">
                    <div class="rh-label">Средний чек</div>
                    <div class="rh-value" style="color:#555">${ecomAvgCheck && offAvgCheck ? (ecomAvgCheck/offAvgCheck).toFixed(1) + 'x' : '—'}</div>
                    <div class="rh-desc">E-COMM vs OFF</div>
                </div>
                <div class="report-highlight-card" style="border-left:4px solid #555">
                    <div class="rh-label">Частота</div>
                    <div class="rh-value" style="color:#555">${ecomChecks && offChecks ? (offChecks/ecomChecks).toFixed(1) + 'x' : '—'}</div>
                    <div class="rh-desc">OFF vs E-COMM</div>
                </div>
            </div>
        `;

        // Cleaned up — metrics computed before KPI HTML

        // --- OFFLINE pie + bar ---
        document.getElementById('dyn-pie-off-wrap').innerHTML = `
            <div class="card-header"><h3>Структура базы (${dl})</h3></div>
            <div style="height:340px;position:relative"><canvas id="dyn-pie-off"></canvas></div>
        `;
        document.getElementById('dyn-bar-off-wrap').innerHTML = `
            <div class="card-header"><h3>Клиенты по сегментам (динамика)</h3></div>
            <div style="height:340px;position:relative"><canvas id="dyn-seg-bar-off"></canvas></div>
        `;

        // --- E-COM pie + bar ---
        document.getElementById('dyn-pie-ecom-wrap').innerHTML = `
            <div class="card-header"><h3>Структура базы (${dl})</h3></div>
            <div style="height:340px;position:relative"><canvas id="dyn-pie-ecom"></canvas></div>
        `;
        document.getElementById('dyn-bar-ecom-wrap').innerHTML = `
            <div class="card-header"><h3>Клиенты по сегментам (динамика)</h3></div>
            <div style="height:340px;position:relative"><canvas id="dyn-seg-bar-ecom"></canvas></div>
        `;

        // Destroy previous charts
        const dynChartIds = ['dyn-pie-off','dyn-pie-ecom','dyn-seg-bar-off','dyn-seg-bar-ecom','off-retention','ecom-retention'];
        ['off','ecom','cross'].forEach(pfx => {
            ['clients','arpu','checks','avgcheck','ci','cardsale','realsale','push'].forEach(m => {
                dynChartIds.push(pfx + '-' + m + '-active', pfx + '-' + m + '-new');
            });
        });
        dynChartIds.forEach(id => destroyChart(id));

        // Connector plugin (same as clientbase)
        const dynPieConnector = {
            id: 'dynPieConnector',
            afterDatasetsDraw(chart) {
                const ds = chart.getDatasetMeta(0);
                if (!ds || ds.type !== 'doughnut') return;
                const ctx = chart.ctx;
                ds.data.forEach((arc, i) => {
                    const props = arc.getProps(['startAngle','endAngle','outerRadius','x','y']);
                    const midAngle = (props.startAngle + props.endAngle) / 2;
                    const r = props.outerRadius;
                    const x1 = props.x + Math.cos(midAngle) * r;
                    const y1 = props.y + Math.sin(midAngle) * r;
                    const x2 = props.x + Math.cos(midAngle) * (r + 16);
                    const y2 = props.y + Math.sin(midAngle) * (r + 16);
                    ctx.save();
                    ctx.beginPath();
                    ctx.moveTo(x1, y1);
                    ctx.lineTo(x2, y2);
                    ctx.strokeStyle = 'rgba(0,0,0,0.2)';
                    ctx.lineWidth = 1;
                    ctx.stroke();
                    ctx.restore();
                });
            }
        };

        function makePie(canvasId, dataBySeg, total) {
            const segs = SEG_ORDER.filter(s => dataBySeg[s] && dataBySeg[s].clients > 0);
            const data = segs.map(s => dataBySeg[s].clients);
            const colors = segs.map(s => DYN_SEG_COLORS[s]);
            const pushPcts = segs.map(s => {
                const ps = dataBySeg[s].pushShare;
                return ps != null ? (ps*100).toFixed(0) : null;
            });

            chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
                type: 'doughnut',
                data: {
                    labels: segs,
                    datasets: [{
                        data: data,
                        backgroundColor: colors,
                        borderWidth: 2,
                        borderColor: '#fff'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    cutout: '40%',
                    layout: { padding: { left: 70, right: 70, top: 25, bottom: 25 } },
                    plugins: {
                        legend: { display: false },
                        datalabels: {
                            display: ctx => {
                                const pct = total > 0 ? (ctx.dataset.data[ctx.dataIndex] / total * 100) : 0;
                                return pct >= 0.5;
                            },
                            color: '#333',
                            font: { size: 9, weight: '600' },
                            anchor: 'end',
                            align: 'end',
                            offset: 8,
                            clip: false,
                            formatter: (v, ctx) => {
                                const idx = ctx.dataIndex;
                                const pct = total > 0 ? (v / total * 100).toFixed(1) : 0;
                                const push = pushPcts[idx];
                                const cliStr = v >= 1000000 ? (v/1000000).toFixed(1) + 'М' : v >= 1000 ? (v/1000).toFixed(0) + 'К' : v;
                                let txt = ctx.chart.data.labels[idx] + ' ' + pct + '%\n' + cliStr;
                                if (push != null) txt += '\nPUSH ' + push + '%';
                                return txt;
                            },
                            textAlign: 'center'
                        }
                    }
                },
                plugins: [ChartDataLabels, dynPieConnector]
            });
        }

        makePie('dyn-pie-off', offBySegAll, totalOff);
        makePie('dyn-pie-ecom', ecomBySegAll, totalEcom);

        // Horizontal bar charts: clients per segment with value labels
        // Stacked bar: clients by segment over last 12 months with share labels
        function makeSegStackedBar(canvasId, org, last12Dates, last12Labels) {
            const segs = SEG_ORDER.filter(s => s !== 'Фрод');
            destroyChart(canvasId);

            // Build data per seg per month
            const datasets = segs.map(seg => ({
                label: seg,
                data: last12Dates.map(d => {
                    if (!d) return 0;
                    return CVM_DYNAMICS.filter(r => r.org === org && r.date === d && r.segment === seg)
                        .reduce((s, r) => s + r['клиентов'], 0);
                }),
                backgroundColor: DYN_SEG_COLORS[seg],
                borderRadius: 1
            }));

            // Compute totals per month for share labels
            const totals = last12Dates.map((d, i) => segs.reduce((s, seg, si) => s + datasets[si].data[i], 0));

            chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
                type: 'bar',
                data: { labels: last12Labels, datasets },
                plugins: [ChartDataLabels, {
                    id: 'stackShareLabels',
                    afterDatasetsDraw(chart) {
                        const ctx = chart.ctx;
                        // Draw share % inside each bar segment
                        segs.forEach((seg, si) => {
                            const meta = chart.getDatasetMeta(si);
                            meta.data.forEach((bar, i) => {
                                const val = datasets[si].data[i];
                                const total = totals[i];
                                if (!total || !val) return;
                                const pct = (val / total * 100);
                                if (pct < 3) return; // skip tiny segments
                                const barH = Math.abs(bar.base - bar.y);
                                if (barH < 14) return;
                                ctx.save();
                                ctx.font = '600 8px Inter, sans-serif';
                                ctx.fillStyle = '#fff';
                                ctx.textAlign = 'center';
                                ctx.textBaseline = 'middle';
                                ctx.fillText(pct.toFixed(0) + '%', bar.x, (bar.y + bar.base) / 2);
                                ctx.restore();
                            });
                        });
                    }
                }],
                options: {
                    responsive: true, maintainAspectRatio: false,
                    layout: { padding: { top: 18 } },
                    scales: {
                        x: { stacked: true, ticks: { font: { size: 7 } } },
                        y: { stacked: true, display: false }
                    },
                    plugins: {
                        legend: { position: 'top', labels: { boxWidth: 10, font: { size: 8 }, padding: 8 } },
                        datalabels: {
                            display: ctx => ctx.datasetIndex === segs.length - 1,
                            anchor: 'end', align: 'end', offset: 2,
                            font: { size: 7, weight: '600' }, color: '#555',
                            formatter: (v, ctx) => {
                                const total = totals[ctx.dataIndex];
                                return total > 0 ? (total / 1000000).toFixed(1) + 'М' : '';
                            }
                        }
                    }
                }
            });
        }

        // Time series: clients by segment over time
        function buildTimeSeries() {

            // Build 12 calendar months ending at selected date
            // e.g. for 2026-02-28 → Mar 2025 .. Feb 2026
            function buildLast12CalendarMonths(selectedDate) {
                const parts = selectedDate.split('-');
                let y = parseInt(parts[0]);
                let m = parseInt(parts[1]);
                const months12 = [];
                for (let i = 11; i >= 0; i--) {
                    let cm = m - i;
                    let cy = y;
                    while (cm <= 0) { cm += 12; cy--; }
                    // Find matching date in data (last day of month)
                    const prefix = cy + '-' + String(cm).padStart(2, '0');
                    months12.push(prefix);
                }
                return months12;
            }
            const cal12 = buildLast12CalendarMonths(date);

            // Find actual dates matching each calendar month
            function matchDates(availDates, calMonths) {
                return calMonths.map(prefix => availDates.find(d => d.startsWith(prefix)) || null);
            }
            const last12Off = matchDates(offlineDates, cal12);
            const last12Ecom = matchDates(ecommDates, cal12);
            const last12Labels = cal12.map(prefix => {
                const [y, m] = prefix.split('-');
                return monthNames[parseInt(m)].toLowerCase() + '.' + y.slice(2);
            });

            // --- Build per-metric rows: Активные (left) | Новые (right) ---
            const metrics = ['clients','arpu','checks','avgcheck','ci','cardsale','realsale','push'];
            const metricLabels = {clients:'Клиенты',arpu:'ARPU',checks:'Чеков/клиента',avgcheck:'Ср. чек',ci:'Ценовой индекс',cardsale:'Скидка по карте',realsale:'Реальная скидка',push:'Доля PUSH'};

            // Metrics config
            const metricsCfg = {
                clients: { extract: (s) => s?.clients || 0, dlFmt: v => v > 0 ? (v/1000).toFixed(0)+'К' : '' },
                arpu: { extract: (s) => s && s.total > 0 && s.budgetSum > 0 ? Math.round(s.budgetSum/s.total) : null, dlFmt: v => v != null ? v.toLocaleString('ru-RU') : '' },
                checks: { extract: (s) => s && s.total > 0 && s.checksSum > 0 ? +(s.checksSum/s.total).toFixed(2) : null, dlFmt: v => v != null ? v.toFixed(1) : '' },
                avgcheck: { extract: (s) => s && s.total > 0 && s.avgCheckSum > 0 ? Math.round(s.avgCheckSum/s.total) : null, dlFmt: v => v != null ? v.toLocaleString('ru-RU') : '' },
                ci: { extract: (s) => s && s.total > 0 && s.ciSum > 0 ? +(s.ciSum/s.total).toFixed(4) : null, dlFmt: v => v != null ? v.toFixed(3) : '' },
                cardsale: { extract: (s) => s && s.total > 0 ? +(s.cardSaleSum/s.total*100).toFixed(2) : null, dlFmt: v => v != null ? v.toFixed(1)+'%' : '' },
                realsale: { extract: (s) => s && s.total > 0 ? +(s.realSaleSum/s.total*100).toFixed(2) : null, dlFmt: v => v != null ? v.toFixed(1)+'%' : '' },
                push: { extract: (s) => s && s.pushCli > 0 ? +(s.pushAvail/s.pushCli*100).toFixed(1) : null, dlFmt: v => v != null ? v.toFixed(0)+'%' : '' },
            };

            // Generate insights text for a metric
            function genInsight(m, prefix, org, channels, last12Dates, agg) {
                const cfg = metricsCfg[m];
                if (!cfg) return '';
                const fmtPct = v => (v >= 0 ? '+' : '') + (v*100).toFixed(1) + '%';
                const sp = (v, txt) => `<span class="${v >= 0 ? 'ins-up' : 'ins-down'}">${txt || fmtPct(v)}</span>`;

                function getMergedVals(seg) {
                    return last12Dates.map(d => {
                        if (!d) return null;
                        const merged = { clients:0, budgetSum:0, checksSum:0, avgCheckSum:0, cardSaleSum:0, realSaleSum:0, ciSum:0, total:0, pushAvail:0, pushCli:0 };
                        channels.forEach(ch => {
                            const s = agg[d]?.[seg+'|'+ch];
                            if (s) Object.keys(merged).forEach(f => { merged[f] += s[f] || 0; });
                        });
                        return cfg.extract(merged);
                    });
                }
                function getRawVals(seg, sumF, divF) {
                    return last12Dates.map(d => {
                        if (!d) return null;
                        let sum=0, div=0;
                        channels.forEach(ch => { const s = agg[d]?.[seg+'|'+ch]; if(s){sum+=s[sumF]||0; div+=s[divF]||0;} });
                        return div > 0 ? sum/div : null;
                    }).filter(v => v != null);
                }
                function chg(vals) {
                    const v = vals.filter(x => x != null);
                    return v.length >= 2 && v[0] ? (v[v.length-1] - v[0]) / Math.abs(v[0]) : null;
                }
                // YoY change: last available date vs same month year ago
                const isEcomCtx = org === 3;
                function yoyChg(seg) {
                    const yoyAggData = isEcomCtx ? ecomYoYAgg : offYoYAgg;
                    // Find last date with YoY match
                    for (let di = last12Dates.length - 1; di >= 0; di--) {
                        const d = last12Dates[di];
                        if (!d) continue;
                        const yaP = getYearAgoDate(d)?.substring(0,7);
                        const yaD = yaP ? Object.keys(yoyAggData).find(k=>k.startsWith(yaP)) : null;
                        if (!yaD) continue;
                        const curM = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                        const yaM = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                        channels.forEach(ch => {
                            const cs = agg[d]?.[seg+'|'+ch]; if(cs) Object.keys(curM).forEach(f=>{curM[f]+=cs[f]||0;});
                            const ys = yoyAggData[yaD]?.[seg+'|'+ch]; if(ys) Object.keys(yaM).forEach(f=>{yaM[f]+=ys[f]||0;});
                        });
                        const cv = cfg.extract(curM);
                        const pv = cfg.extract(yaM);
                        if (cv!=null && pv!=null && pv!==0) return (cv-pv)/Math.abs(pv);
                    }
                    return null;
                }
                function findOutliers(vals) {
                    const valid = vals.map((v,i) => v != null ? {v,i} : null).filter(Boolean);
                    if (valid.length < 4) return [];
                    const mean = valid.reduce((s,x) => s+x.v, 0) / valid.length;
                    const std = Math.sqrt(valid.reduce((s,x) => s+(x.v-mean)**2, 0) / valid.length);
                    if (std === 0) return [];
                    return valid.filter(x => Math.abs(x.v-mean) > 1.5*std).map(x => ({
                        label: last12Labels[x.i], dir: x.v > mean ? 'пик' : 'провал'
                    }));
                }

                const actV = getMergedVals('Активные');
                const newV = getMergedVals('Новые');
                // Offline: YoY (LFL). E-commerce: MoM (month-to-month)
                function momChg(vals) {
                    const v = vals.filter(x => x != null);
                    if (v.length < 2) return null;
                    const last = v[v.length-1], prev = v[v.length-2];
                    return prev !== 0 ? (last - prev) / Math.abs(prev) : null;
                }
                let actC, newC, lflLabel;
                if (isEcomCtx) {
                    actC = momChg(actV);
                    newC = momChg(newV);
                    lflLabel = ' MoM';
                } else {
                    actC = yoyChg('Активные') ?? chg(actV);
                    newC = yoyChg('Новые') ?? chg(newV);
                    lflLabel = yoyChg('Активные') != null ? ' LFL' : '';
                }
                const out = findOutliers(actV);

                if (actV.filter(v=>v!=null).length < 2) return '<div class="ins-neutral">Недостаточно данных для анализа</div>';

                // Omni vs mono comparison for this specific metric
                const omniCh = 'омни';
                const monoCh = isEcomCtx ? 'только e-commerce' : 'только оффлайн';
                const monoLabel = isEcomCtx ? 'e-com' : 'оффлайн';
                function omniVsMono() {
                    const lastAvail = last12Dates.filter(d=>d).pop();
                    if (!lastAvail) return '';
                    const omniKey = 'Активные|' + omniCh;
                    const monoKey = 'Активные|' + monoCh;
                    const os = agg[lastAvail]?.[omniKey];
                    const ms = agg[lastAvail]?.[monoKey];
                    if (!os || !ms) return '';
                    const ov = cfg.extract(os);
                    const mv = cfg.extract(ms);
                    if (ov == null || mv == null || mv === 0) return '';
                    const diff = ((ov - mv) / Math.abs(mv) * 100).toFixed(0);
                    const higher = ov > mv ? 'омни' : monoLabel;
                    let line = `<div style="margin-top:6px;border-top:1px solid #eee;padding-top:4px"><b>Омни vs ${monoLabel}:</b> ${cfg.dlFmt(ov)} vs ${cfg.dlFmt(mv)} (<span class="${ov > mv ? 'ins-up' : 'ins-down'}">${diff > 0 ? '+' : ''}${diff}%</span>) — ${higher} выше.</div>`;
                    // Contextual interpretation
                    if (m === 'cardsale' && ov > mv) {
                        line += '<div style="margin-top:2px" class="ins-neutral">Более высокая скидка по карте у омни-клиентов свидетельствует об их глубокой вовлечённости в программу лояльности — они активнее используют бонусные механики и накапливают больше привилегий.</div>';
                    }
                    if (m === 'arpu' && ov > mv) {
                        line += '<div style="margin-top:2px" class="ins-neutral">Более высокий ARPU омни-клиентов объясняется тем, что в e-commerce переключаются и начинают покупать наиболее лояльные и вовлечённые клиенты оффлайн-базы.</div>';
                    }
                    return line;
                }

                let h = '<h4>Ключевые наблюдения</h4>';

                if (m === 'clients') {
                    h += `<div>Активная база${lflLabel}: ${sp(actC)}.</div>`;
                    if (newC != null) {
                        h += `<div style="margin-top:4px">Приток новых клиентов: ${sp(newC)}${newC > actC ? ' — опережает рост базы, что говорит о высокой эффективности привлечения' : ''}.</div>`;
                    }
                    if (actC > 0 && newC > actC) h += '<div class="ins-alert">Рост базы обеспечен преимущественно притоком новых клиентов. Важно отслеживать их конверсию в постоянных покупателей.</div>';

                    // Correlation: new clients vs card sale discount
                    const newCliVals = newV.filter(v => v != null);
                    const discountCfg = metricsCfg['cardsale'];
                    const discVals = last12Dates.map(d => {
                        if (!d) return null;
                        const merged = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                        channels.forEach(ch => { const s = agg[d]?.['Активные|'+ch]; if(s) Object.keys(merged).forEach(f=>{merged[f]+=s[f]||0;}); });
                        return discountCfg.extract(merged);
                    });
                    // Compute Pearson correlation on paired non-null values
                    const paired = [];
                    for (let pi = 0; pi < last12Dates.length; pi++) {
                        if (newV[pi] != null && discVals[pi] != null) paired.push([newV[pi], discVals[pi]]);
                    }
                    if (paired.length >= 4) {
                        const n = paired.length;
                        const mx = paired.reduce((s,p) => s+p[0], 0) / n;
                        const my = paired.reduce((s,p) => s+p[1], 0) / n;
                        const cov = paired.reduce((s,p) => s + (p[0]-mx)*(p[1]-my), 0);
                        const sx = Math.sqrt(paired.reduce((s,p) => s + (p[0]-mx)**2, 0));
                        const sy = Math.sqrt(paired.reduce((s,p) => s + (p[1]-my)**2, 0));
                        const r = (sx > 0 && sy > 0) ? (cov / (sx * sy)) : 0;
                        const rAbs = Math.abs(r);
                        const strength = rAbs > 0.7 ? 'сильная' : rAbs > 0.4 ? 'умеренная' : 'слабая';
                        const dir = r > 0 ? 'прямая' : 'обратная';
                        h += `<div style="margin-top:6px;border-top:1px solid #eee;padding-top:4px"><b>Корреляция: новые клиенты ↔ скидка по карте</b></div>`;
                        h += `<div>Коэффициент Пирсона: <b>${r.toFixed(2)}</b> (${strength} ${dir} связь).</div>`;
                        if (r > 0.4) {
                            h += '<div style="margin-top:2px" class="ins-neutral">Приток новых клиентов статистически связан с ростом скидки по карте.</div>';
                        } else if (r < -0.4) {
                            h += '<div style="margin-top:2px" class="ins-neutral">Обратная связь: рост скидки не привлекает новых клиентов — возможно, скидка удерживает существующих, но не стимулирует привлечение.</div>';
                        } else {
                            h += '<div style="margin-top:2px" class="ins-neutral">Связь между скидкой и притоком новых клиентов слабая — привлечение определяется другими факторами (маркетинг, сезонность).</div>';
                        }
                    }

                } else if (m === 'arpu') {
                    h += `<div>ARPU активных${lflLabel}: ${sp(actC)}.</div>`;
                    // Decomposition using same comparison method (YoY for offline, MoM for ecom)
                    function decompChg(metricKey) {
                        const cfgD = metricsCfg[metricKey];
                        if (!cfgD) return null;
                        if (!isEcomCtx) {
                            // YoY for offline
                            const yoyAggData = offYoYAgg;
                            for (let di = last12Dates.length - 1; di >= 0; di--) {
                                const d = last12Dates[di]; if (!d) continue;
                                const yaP = getYearAgoDate(d)?.substring(0,7);
                                const yaD = yaP ? Object.keys(yoyAggData).find(k=>k.startsWith(yaP)) : null;
                                if (!yaD) continue;
                                const curM = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                                const yaM = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                                channels.forEach(ch => {
                                    const cs = agg[d]?.['Активные|'+ch]; if(cs) Object.keys(curM).forEach(f=>{curM[f]+=cs[f]||0;});
                                    const ys = yoyAggData[yaD]?.['Активные|'+ch]; if(ys) Object.keys(yaM).forEach(f=>{yaM[f]+=ys[f]||0;});
                                });
                                const cv = cfgD.extract(curM), pv = cfgD.extract(yaM);
                                if (cv!=null && pv!=null && pv!==0) return (cv-pv)/Math.abs(pv);
                            }
                            return null;
                        } else {
                            // MoM for ecom
                            const vals = getMergedVals('Активные').map((v,i) => {
                                if (!last12Dates[i]) return null;
                                const merged = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                                channels.forEach(ch => { const s = agg[last12Dates[i]]?.['Активные|'+ch]; if(s) Object.keys(merged).forEach(f=>{merged[f]+=s[f]||0;}); });
                                return cfgD.extract(merged);
                            }).filter(x=>x!=null);
                            return vals.length >= 2 ? (vals[vals.length-1]-vals[vals.length-2])/Math.abs(vals[vals.length-2]) : null;
                        }
                    }
                    const chkC = decompChg('checks');
                    const achC = decompChg('avgcheck');
                    if (chkC != null && achC != null) {
                        const driver = Math.abs(chkC) > Math.abs(achC) ? 'частоты визитов' : 'среднего чека';
                        h += `<div style="margin-top:4px"><b>Декомпозиция${lflLabel}:</b> частота покупок ${sp(chkC)}, средний чек ${sp(achC)}.</div>`;
                        h += `<div style="margin-top:4px">Основной драйвер — изменение <b>${driver}</b>.</div>`;
                        if (chkC < 0 && achC > 0) h += '<div class="ins-alert">Частота визитов снижается при росте чека — клиенты приходят реже, но тратят больше. Необходимо стимулировать возвратность.</div>';
                        if (chkC > 0 && achC < 0) h += '<div class="ins-alert">Рост трафика при падении чека — возможный эффект промо-зависимости.</div>';
                    }
                    if (newC != null) {
                        let newNote = '';
                        if (newC < 0) newNote = ' — ARPU новых снижается: в программу вовлекаются менее лояльные клиенты с более низкой покупательской активностью';
                        else if (newC > 0 && actC > 0 && newC < actC) newNote = ' — ARPU новых растёт медленнее активных: в программу продолжают вовлекаться менее лояльные клиенты';
                        else if (newC >= actC) newNote = ' — опережающий рост ARPU новых, высокое качество привлечения';
                        h += `<div style="margin-top:4px">ARPU новых: ${sp(newC)}${newNote}.</div>`;
                    }

                } else if (m === 'checks') {
                    h += `<div>Частота покупок активных${lflLabel}: ${sp(actC)}.</div>`;
                    if (newC != null) h += `<div style="margin-top:4px">Новые: ${sp(newC)}.</div>`;
                    if (actC < 0) h += '<div class="ins-alert">Снижение частоты покупок — один из ранних индикаторов оттока. Рекомендуется усилить триггерные коммуникации по возврату.</div>';
                    if (actC > 0.05) h += '<div style="margin-top:4px;color:#2E8B57">Устойчивый рост частоты — позитивный сигнал, свидетельствует об укреплении лояльности к сети.</div>';

                    // Correlation: checks/client vs card sale discount
                    const checksVals = actV;
                    const discCfg = metricsCfg['cardsale'];
                    const discValsForCorr = last12Dates.map(d => {
                        if (!d) return null;
                        const merged = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                        channels.forEach(ch => { const s = agg[d]?.['Активные|'+ch]; if(s) Object.keys(merged).forEach(f=>{merged[f]+=s[f]||0;}); });
                        return discCfg.extract(merged);
                    });
                    const chkPaired = [];
                    for (let pi = 0; pi < last12Dates.length; pi++) {
                        if (checksVals[pi] != null && discValsForCorr[pi] != null) chkPaired.push([checksVals[pi], discValsForCorr[pi]]);
                    }
                    if (chkPaired.length >= 4) {
                        const n = chkPaired.length;
                        const mx = chkPaired.reduce((s,p) => s+p[0], 0) / n;
                        const my = chkPaired.reduce((s,p) => s+p[1], 0) / n;
                        const cov = chkPaired.reduce((s,p) => s + (p[0]-mx)*(p[1]-my), 0);
                        const sx = Math.sqrt(chkPaired.reduce((s,p) => s + (p[0]-mx)**2, 0));
                        const sy = Math.sqrt(chkPaired.reduce((s,p) => s + (p[1]-my)**2, 0));
                        const r = (sx > 0 && sy > 0) ? (cov / (sx * sy)) : 0;
                        const rAbs = Math.abs(r);
                        const strength = rAbs > 0.7 ? 'сильная' : rAbs > 0.4 ? 'умеренная' : 'слабая';
                        const dir = r > 0 ? 'прямая' : 'обратная';
                        h += `<div style="margin-top:6px;border-top:1px solid #eee;padding-top:4px"><b>Корреляция: частота ↔ скидка по карте</b></div>`;
                        h += `<div>Коэффициент Пирсона: <b>${r.toFixed(2)}</b> (${strength} ${dir} связь).</div>`;
                        if (r > 0.4) h += '<div style="margin-top:2px" class="ins-neutral">Рост скидки по карте статистически связан с увеличением частоты покупок — скидка стимулирует возвратность.</div>';
                        else if (r < -0.4) h += '<div style="margin-top:2px" class="ins-neutral">Обратная связь: рост скидки сопровождается снижением частоты — возможно, клиенты консолидируют покупки.</div>';
                        else h += '<div style="margin-top:2px" class="ins-neutral">Связь между скидкой по карте и частотой покупок слабая — частота определяется другими факторами.</div>';
                    }

                    // Correlation: checks vs real sale (general promos)
                    const realSaleCfg = metricsCfg['realsale'];
                    const realValsForCorr = last12Dates.map(d => {
                        if (!d) return null;
                        const merged = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                        channels.forEach(ch => { const s = agg[d]?.['Активные|'+ch]; if(s) Object.keys(merged).forEach(f=>{merged[f]+=s[f]||0;}); });
                        return realSaleCfg.extract(merged);
                    });
                    const realPaired = [];
                    for (let pi = 0; pi < last12Dates.length; pi++) {
                        if (checksVals[pi] != null && realValsForCorr[pi] != null) realPaired.push([checksVals[pi], realValsForCorr[pi]]);
                    }
                    if (realPaired.length >= 4) {
                        const n2 = realPaired.length;
                        const mx2 = realPaired.reduce((s,p) => s+p[0], 0) / n2;
                        const my2 = realPaired.reduce((s,p) => s+p[1], 0) / n2;
                        const cov2 = realPaired.reduce((s,p) => s + (p[0]-mx2)*(p[1]-my2), 0);
                        const sx2 = Math.sqrt(realPaired.reduce((s,p) => s + (p[0]-mx2)**2, 0));
                        const sy2 = Math.sqrt(realPaired.reduce((s,p) => s + (p[1]-my2)**2, 0));
                        const r2 = (sx2 > 0 && sy2 > 0) ? (cov2 / (sx2 * sy2)) : 0;
                        const str2 = Math.abs(r2) > 0.7 ? 'сильная' : Math.abs(r2) > 0.4 ? 'умеренная' : 'слабая';
                        const dir2 = r2 > 0 ? 'прямая' : 'обратная';
                        h += `<div style="margin-top:6px;border-top:1px solid #eee;padding-top:4px"><b>Корреляция: частота ↔ реальная скидка (общие промо)</b></div>`;
                        h += `<div>Коэффициент Пирсона: <b>${r2.toFixed(2)}</b> (${str2} ${dir2} связь).</div>`;
                        if (r2 > 0.4) h += '<div style="margin-top:2px" class="ins-neutral">Увеличение общих промо статистически связано с ростом частоты покупок — массовые акции стимулируют возвратность.</div>';
                        else if (r2 < -0.4) h += '<div style="margin-top:2px" class="ins-neutral">Обратная связь: рост общих промо не увеличивает частоту — клиенты не реагируют на массовые акции дополнительными визитами.</div>';
                        else h += '<div style="margin-top:2px" class="ins-neutral">Увеличение общих промо не стимулирует рост частоты покупок — массовые акции не являются драйвером возвратности.</div>';
                    }

                } else if (m === 'avgcheck') {
                    const inflRate = 0.085;
                    const realGr = (actC || 0) - inflRate;
                    const isEcom = org === 3;
                    h += `<div>Средний чек активных${lflLabel}: ${sp(actC)}.</div>`;
                    h += `<div style="margin-top:4px">Средняя инфляция РФ ~8.5%. Реальный рост чека: ${sp(realGr)}.</div>`;
                    if (isEcom) {
                        h += '<div style="margin-top:4px" class="ins-neutral">В e-commerce средний чек во многом определяется порогом минимального заказа и бесплатной доставки — рост чека может отражать повышение этих порогов, а не органическое изменение покупательского поведения.</div>';
                    } else if (realGr < 0) {
                        h += '<div class="ins-alert">Чек растёт медленнее инфляции — покупатель оптимизирует корзину. Рекомендуется анализ корзинного поведения и ценовой эластичности.</div>';
                    } else {
                        h += '<div style="margin-top:4px;color:#2E8B57">Рост чека опережает инфляцию — клиенты увеличивают объём покупок в реальном выражении.</div>';
                    }

                } else if (m === 'ci') {
                    h += `<div>Ценовой индекс активных${lflLabel}: ${sp(actC)}.</div>`;
                    if (actC > 0) h += '<div style="margin-top:4px">Рост ЦИ означает смещение покупок в более дорогие товарные категории — потенциал для роста маржинальности.</div>';
                    if (actC < 0) h += '<div style="margin-top:4px">Снижение ЦИ — клиенты переключаются на более дешёвые товары. Возможно усиление ценовой чувствительности.</div>';

                } else if (m === 'cardsale') {
                    const isEcom = org === 3;
                    h += `<div>Скидка по карте${lflLabel}: ${sp(actC)}.</div>`;
                    const cliC = chg(getMergedVals('Активные').map((v,i) => {
                        if (!last12Dates[i]) return null;
                        let c = 0;
                        channels.forEach(ch => { const s = agg[last12Dates[i]]?.['Активные|'+ch]; if(s) c += s.clients; });
                        return c;
                    }));
                    if (cliC != null && actC != null) {
                        const sameDir = (actC > 0 && cliC > 0) || (actC < 0 && cliC < 0);
                        h += `<div style="margin-top:4px">Рост клиентской базы: ${sp(cliC)}. ${sameDir ? 'Скидка и клиентская база движутся синхронно — программа лояльности эффективно привлекает трафик.' : 'Разнонаправленная динамика — рост базы не зависит напрямую от уровня скидки.'}</div>`;
                    }
                    if (!isEcom && actC > 0 && cliC > 0) {
                        h += '<div style="margin-top:4px" class="ins-neutral">В оффлайн рост клиентской базы напрямую связан с увеличением скидки по карте — более глубокая скидка стимулирует регистрацию новых участников и удерживает существующих.</div>';
                    }
                    if (isEcom) {
                        h += '<div style="margin-top:4px" class="ins-alert">До мая 2025 клиенты не могли списывать монеты (баллы) программы лояльности в e-com — скидка по карте была 0%. Резкий рост показателя связан с запуском списания баллов в онлайн-канале.</div>';
                    }

                } else if (m === 'realsale') {
                    h += `<div>Реализованная скидка${lflLabel}: ${sp(actC)}.</div>`;
                    if (actC < 0) {
                        h += '<div style="margin-top:4px" class="ins-neutral">Снижение реальной скидки означает сокращение количества и глубины массовых промо-акций. Компания уменьшает инвестиции в общие ценовые промо.</div>';
                    } else if (actC > 0) {
                        h += '<div style="margin-top:4px" class="ins-neutral">Рост реальной скидки — увеличение количества или глубины массовых промо-акций.</div>';
                    }
                    // Compare with cardsale direction
                    const cardC = chg(getMergedVals('Активные').map((v,i) => {
                        if (!last12Dates[i]) return null;
                        const merged = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                        channels.forEach(ch => { const s = agg[last12Dates[i]]?.['Активные|'+ch]; if(s) Object.keys(merged).forEach(f=>{merged[f]+=s[f]||0;}); });
                        return metricsCfg['cardsale'].extract(merged);
                    }));
                    if (actC != null && cardC != null) {
                        if (actC < 0 && cardC > 0) {
                            h += '<div style="margin-top:4px" class="ins-neutral">При этом скидка по карте растёт — происходит перераспределение: меньше массовых промо, больше инвестиций в программу лояльности.</div>';
                        } else if (actC > 0 && cardC > 0) {
                            h += '<div style="margin-top:4px" class="ins-neutral">Растут обе составляющие скидки — как общие промо, так и по карте. Рекомендуется оценить совокупную стоимость удержания.</div>';
                        } else if (actC < 0 && cardC < 0) {
                            h += '<div style="margin-top:4px" class="ins-neutral">Снижаются обе составляющие скидки — как общие промо, так и по карте лояльности. Сигнал оптимизации промо-бюджета.</div>';
                        }
                    }

                } else if (m === 'push') {
                    h += `<div>Доступность для PUSH${lflLabel}: ${sp(actC)}.</div>`;
                    if (actC < 0) h += '<div class="ins-alert">Снижение PUSH-доступности ограничивает эффективность CVM-коммуникаций. Рекомендуется программа реактивации подписок.</div>';
                    if (actC > 0) h += '<div style="margin-top:4px;color:#2E8B57">Растущий охват PUSH — расширяет возможности CVM-коммуникаций и повышает конверсию промо.</div>';
                    if (isEcomCtx) {
                        h += '<div style="margin-top:4px" class="ins-neutral">В e-commerce клиентами становились преимущественно те, до кого удалось дотянуться через бесплатный канал PUSH. Высокая PUSH-доступность в e-com базе — следствие того, что именно PUSH-коммуникация является основным каналом привлечения оффлайн-клиентов в онлайн.</div>';
                    }
                }

                // Omni vs mono for this metric
                if (m !== 'clients') h += omniVsMono();

                if (out.length > 0) {
                    // Known anomaly explanations
                    const ecomBankNote = 'исключение базы банковских клиентов из CVM';
                    const ecomBonusNote = 'до мая 2025 клиенты не могли списывать монеты (баллы) программы лояльности в e-com — скидка по карте была 0';
                    const knownAnomalies = {
                        'ecom|ноя.25|clients': ecomBankNote,
                        'ecom|ноя.25|arpu': ecomBankNote,
                        'ecom|ноя.25|checks': ecomBankNote,
                    };
                    // Auto-add ecom cardsale anomalies for months before may 2025 (bonus redemption launched in may)
                    const ecomRealSaleNote = 'период активного промотирования e-com канала — повышенные промо-инвестиции на этапе запуска';
                    if (prefix === 'ecom' && (m === 'cardsale' || m === 'realsale')) {
                        out.forEach(o => {
                            const monthNum = parseInt(o.label?.split('.')[0] === 'янв' ? '01' : o.label?.split('.')[0] === 'фев' ? '02' : o.label?.split('.')[0] === 'мар' ? '03' : o.label?.split('.')[0] === 'апр' ? '04' : '99');
                            const yearNum = parseInt('20' + (o.label?.split('.')[1] || '99'));
                            if (yearNum < 2025 || (yearNum === 2025 && monthNum < 5)) {
                                knownAnomalies[prefix + '|' + o.label + '|' + m] = m === 'cardsale' ? ecomBonusNote : ecomRealSaleNote;
                            }
                        });
                    }
                    // Group anomalies by explanation to avoid duplication
                    const byReason = {};
                    out.forEach(o => {
                        const key = prefix + '|' + o.label + '|' + m;
                        const reason = knownAnomalies[key] || '_unknown';
                        if (!byReason[reason]) byReason[reason] = [];
                        byReason[reason].push(`<b>${o.label}</b> (${o.dir})`);
                    });
                    const parts = [];
                    Object.entries(byReason).forEach(([reason, labels]) => {
                        if (reason === '_unknown') parts.push(labels.join(', ') + ' — требуют отдельного разбора причин');
                        else parts.push(labels.join(', ') + ' — ' + reason);
                    });
                    h += '<div class="ins-alert" style="margin-top:6px">Аномалии: ' + parts.join('. ') + '.</div>';
                }

                return h;
            }

            // Helper: get raw weighted values for decomposition
            function getSegVals_raw(seg, sumField, divField, channels, agg, dates) {
                return dates.map(d => {
                    if (!d) return null;
                    let sum = 0, div = 0;
                    channels.forEach(ch => {
                        const s = agg[d]?.[seg + '|' + ch];
                        if (s) { sum += s[sumField] || 0; div += s[divField] || 0; }
                    });
                    return div > 0 ? sum / div : null;
                }).filter(v => v != null);
            }

            function buildMetricRows(prefix, rowIds) {
                const isOff = prefix === 'off';
                const org = isOff ? 1 : 3;
                const channels = isOff ? ['только оффлайн', 'омни'] : ['только e-commerce', 'омни'];

                // Compute last-month LFL YoY badge for a segment
                function yoyBadge(seg, metric) {
                    const dates12 = isOff ? last12Off : last12Ecom;
                    const aggData = isOff ? offAgg : ecomAgg;
                    const yoyAggData = isOff ? offYoYAgg : ecomYoYAgg;
                    // Find last date that has YoY match (search backwards)
                    let lastD = null, yaD = null;
                    for (let di = dates12.length - 1; di >= 0; di--) {
                        if (!dates12[di]) continue;
                        const yp = getYearAgoDate(dates12[di])?.substring(0,7);
                        const yd = yp ? Object.keys(yoyAggData).find(k=>k.startsWith(yp)) : null;
                        if (yd) { lastD = dates12[di]; yaD = yd; break; }
                    }
                    if (!lastD || !yaD) return '';
                    const curM = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                    const yaM = {clients:0,budgetSum:0,checksSum:0,avgCheckSum:0,cardSaleSum:0,realSaleSum:0,ciSum:0,total:0,pushAvail:0,pushCli:0};
                    channels.forEach(ch => {
                        const cs = aggData[lastD]?.[seg+'|'+ch]; if(cs) Object.keys(curM).forEach(f=>{curM[f]+=cs[f]||0;});
                        const ys = yoyAggData[yaD]?.[seg+'|'+ch]; if(ys) Object.keys(yaM).forEach(f=>{yaM[f]+=ys[f]||0;});
                    });
                    const cv = metricsCfg[metric]?.extract(curM);
                    const pv = metricsCfg[metric]?.extract(yaM);
                    if (cv==null||pv==null||pv===0) return '';
                    const pct = ((cv-pv)/Math.abs(pv)*100).toFixed(1);
                    const cls = pct >= 0 ? 'up' : 'down';
                    return ` <span class="report-yoy-badge ${cls}" style="font-size:10px;margin-left:6px">${pct >= 0 ? '+' : ''}${pct}% LFL</span>`;
                }

                rowIds.forEach((rowId, i) => {
                    const m = metrics[i];
                    if (!m) return;
                    const dates12 = isOff ? last12Off : last12Ecom;
                    const aggData = isOff ? offAgg : ecomAgg;
                    const insightHtml = genInsight(m, prefix, org, channels, dates12, aggData);
                    const actBadge = yoyBadge('Активные', m);
                    const newBadge = yoyBadge('Новые', m);
                    document.getElementById(rowId).innerHTML = `
                        <div class="card">
                            <div class="card-header"><h3>${metricLabels[m]} — Активные${actBadge}</h3></div>
                            <div style="height:280px"><canvas id="${prefix}-${m}-active"></canvas></div>
                        </div>
                        <div class="card">
                            <div class="card-header"><h3>${metricLabels[m]} — Новые${newBadge}</h3></div>
                            <div style="height:280px"><canvas id="${prefix}-${m}-new"></canvas></div>
                        </div>
                        <div class="insight-col">${insightHtml}</div>
                    `;
                });
            }
            // Stacked bar charts: clients by segment over 12 months
            makeSegStackedBar('dyn-seg-bar-off', 1, last12Off, last12Labels);
            makeSegStackedBar('dyn-seg-bar-ecom', 3, last12Ecom, last12Labels);

            // Structure & dynamics insight
            function genStructureInsight(org, last12Dates, isEcom) {
                const segs = ['Новые','Активные','Отток','Спящие'];
                const available = last12Dates.filter(d => d != null);
                if (available.length < 2) return '<div class="ins-neutral">Недостаточно данных</div>';

                const first = available[0];
                const last = available[available.length - 1];
                const fmtPct = v => (v >= 0 ? '+' : '') + (v*100).toFixed(1) + '%';
                const sp = (v) => `<span class="${v >= 0 ? 'ins-up' : 'ins-down'}">${fmtPct(v)}</span>`;

                // Total clients first vs last
                function totalCli(d) { return CVM_DYNAMICS.filter(r => r.org === org && r.date === d).reduce((s,r) => s + r['клиентов'], 0); }
                function segCli(d, seg) { return CVM_DYNAMICS.filter(r => r.org === org && r.date === d && r.segment === seg).reduce((s,r) => s + r['клиентов'], 0); }

                const tFirst = totalCli(first);
                const tLast = totalCli(last);
                const totalGrowth = tFirst > 0 ? (tLast - tFirst) / tFirst : 0;

                let h = '<h4>Структура и динамика</h4>';
                h += `<div>Общий прирост базы за период: <b>${sp(totalGrowth)}</b> (${(tFirst/1000000).toFixed(1)}М → ${(tLast/1000000).toFixed(1)}М).</div>`;

                // Share changes by segment
                h += '<div style="margin-top:6px"><b>Изменение долей:</b></div>';
                const shareChanges = segs.map(seg => {
                    const shareFirst = tFirst > 0 ? segCli(first, seg) / tFirst : 0;
                    const shareLast = tLast > 0 ? segCli(last, seg) / tLast : 0;
                    const delta = shareLast - shareFirst;
                    return { seg, shareFirst, shareLast, delta };
                });

                shareChanges.forEach(sc => {
                    const arrow = sc.delta >= 0 ? '↑' : '↓';
                    const color = (sc.seg === 'Отток' || sc.seg === 'Спящие') ? (sc.delta > 0 ? 'ins-down' : 'ins-up') : (sc.delta >= 0 ? 'ins-up' : 'ins-down');
                    h += `<div>${sc.seg}: ${(sc.shareFirst*100).toFixed(1)}% → ${(sc.shareLast*100).toFixed(1)}% <span class="${color}">${arrow}${Math.abs(sc.delta*100).toFixed(1)}pp</span></div>`;
                });

                // Key interpretation
                const activeGrowth = shareChanges.find(s => s.seg === 'Активные');
                const churnGrowth = shareChanges.find(s => s.seg === 'Отток');
                const newGrowth = shareChanges.find(s => s.seg === 'Новые');

                h += '<div style="margin-top:6px">';
                if (activeGrowth && churnGrowth) {
                    if (activeGrowth.delta > 0 && churnGrowth.delta < 0) {
                        h += 'Позитивная динамика: доля активных растёт при сокращении оттока — программа лояльности эффективно удерживает клиентов.';
                    } else if (activeGrowth.delta < 0 && churnGrowth.delta > 0) {
                        h += '<span class="ins-down">Тревожный сигнал:</span> доля активных снижается при росте оттока — необходимо пересмотреть стратегию удержания.';
                    } else if (activeGrowth.delta > 0 && churnGrowth.delta > 0) {
                        h += 'Рост базы сопровождается увеличением оттока — база прирастает за счёт новых, но удержание требует внимания.';
                    }
                }
                h += '</div>';

                if (!isEcom && newGrowth && newGrowth.delta !== 0) {
                    h += `<div style="margin-top:4px" class="ins-neutral">Доля новых клиентов ${newGrowth.delta > 0 ? 'растёт' : 'снижается'} — ${newGrowth.delta > 0 ? 'привлечение работает эффективно, фокус на конверсию в постоянных' : 'снижается эффективность привлечения, рекомендуется усилить акции для новых'}.</div>`;
                }

                if (isEcom) {
                    h += '<div style="margin-top:4px" class="ins-alert">Падение базы новых и активных в ноя.2025 связано с исключением базы банковских клиентов из CVM — не является органическим оттоком.</div>';
                }

                // Brief omni summary (detailed comparison in per-metric insights)
                const lastD = available[available.length - 1];
                const omniR = CVM_DYNAMICS.filter(r => r.org === org && r.date === lastD && r.channel === 'омни' && r.segment === 'Активные');
                const monoR = CVM_DYNAMICS.filter(r => r.org === org && r.date === lastD && r.channel === (isEcom ? 'только e-commerce' : 'только оффлайн') && r.segment === 'Активные');
                if (omniR.length > 0 && monoR.length > 0 && (omniR[0].BUDGET || 0) > (monoR[0].BUDGET || 0)) {
                    h += '<div style="margin-top:6px" class="ins-neutral">Омни-клиенты демонстрируют более высокую ценность по всем ключевым метрикам.</div>';
                }

                return h;
            }

            document.getElementById('dyn-structure-insight-off').innerHTML = genStructureInsight(1, last12Off, false);
            document.getElementById('dyn-structure-insight-ecom').innerHTML = genStructureInsight(3, last12Ecom, true);

            // Helper: aggregate by date, segment, channel for a given org
            function aggByDateSegCh(org, dates) {
                const result = {};
                dates.forEach(d => {
                    if (!d) return;
                    const rows = CVM_DYNAMICS.filter(r => r.org === org && r.date === d);
                    result[d] = {};
                    rows.forEach(r => {
                        const key = r.segment + '|' + r.channel;
                        if (!result[d][key]) result[d][key] = { clients: 0, budgetSum: 0, checksSum: 0, avgCheckSum: 0, cardSaleSum: 0, realSaleSum: 0, ciSum: 0, total: 0, pushAvail: 0, pushCli: 0, metricCli: 0 };
                        result[d][key].clients += r['клиентов'];
                        result[d][key].total += r['клиентов'];
                        if (r.BUDGET != null) result[d][key].budgetSum += r['клиентов'] * r.BUDGET;
                        if (r.CHECKS != null) result[d][key].checksSum += r['клиентов'] * r.CHECKS;
                        if (r.AVG_CHECK != null) result[d][key].avgCheckSum += r['клиентов'] * r.AVG_CHECK;
                        if (r.CARD_SALE != null) result[d][key].cardSaleSum += r['клиентов'] * r.CARD_SALE;
                        if (r.REAL_SALE != null) result[d][key].realSaleSum += r['клиентов'] * r.REAL_SALE;
                        if (r['ЦИ'] != null) result[d][key].ciSum += r['клиентов'] * r['ЦИ'];
                        if (r['Доля доступных для коммуникации'] != null) {
                            result[d][key].pushAvail += r['клиентов'] * r['Доля доступных для коммуникации'];
                            result[d][key].pushCli += r['клиентов'];
                        }
                    });
                });
                return result;
            }

            const offAgg = aggByDateSegCh(1, last12Off);
            const ecomAgg = aggByDateSegCh(3, last12Ecom);

            // Channel colors
            const CH_COLORS = { 'омни': '#4A90D9', 'только оффлайн': '#C67A2E', 'только e-commerce': '#2E8B57' };
            const CH_LABELS = { 'омни': 'Омни', 'только оффлайн': 'Только оффлайн', 'только e-commerce': 'Только e-com' };

            const dynLegend = { position: 'top', labels: { boxWidth: 10, font: { size: 9 }, padding: 10 } };
            const dynXticks = { font: { size: 7 } };

            // metricsCfg moved before genInsight

            // Helper: get year-ago date string for a given date
            function getYearAgoDate(dateStr) {
                if (!dateStr) return null;
                const parts = dateStr.split('-');
                const y = parseInt(parts[0]) - 1;
                return y + '-' + parts[1] + '-' + parts[2];
            }

            // Build YoY agg for year-ago dates
            function buildYoYAgg(org, last12Dates) {
                const yaDates = last12Dates.map(d => d ? getYearAgoDate(d) : null);
                // Find actual dates in CVM_DYNAMICS matching year-ago months
                const allDates = [...new Set(CVM_DYNAMICS.filter(r => r.org === org).map(r => r.date))].sort();
                const matched = yaDates.map(ya => {
                    if (!ya) return null;
                    const prefix = ya.substring(0, 7); // YYYY-MM
                    return allDates.find(d => d.startsWith(prefix)) || null;
                });
                return aggByDateSegCh(org, matched.filter(Boolean));
            }

            const offYoYAgg = buildYoYAgg(1, last12Off);
            const ecomYoYAgg = buildYoYAgg(3, last12Ecom);

            // Build metric rows (must be after all agg including YoY)
            buildMetricRows('off', metrics.map((m,i) => 'dyn-ts-off-row'+(i+1)));
            buildMetricRows('ecom', metrics.map((m,i) => 'dyn-ts-ecom-row'+(i+1)));
            // Cross metric rows — simple 3-column layout without off/ecom-specific insights
            metrics.forEach((m, i) => {
                const rowEl = document.getElementById('dyn-ts-cross-row'+(i+1));
                if (!rowEl) return;
                rowEl.innerHTML = `
                    <div class="card">
                        <div class="card-header"><h3>${metricLabels[m]} — Активные</h3></div>
                        <div style="height:280px"><canvas id="cross-${m}-active"></canvas></div>
                    </div>
                    <div class="card">
                        <div class="card-header"><h3>${metricLabels[m]} — Новые</h3></div>
                        <div style="height:280px"><canvas id="cross-${m}-new"></canvas></div>
                    </div>
                    <div class="insight-col" id="cross-insight-${m}">
                        <div style="padding:12px;color:var(--text-secondary);font-size:12px;font-style:italic">Выводы будут добавлены позже</div>
                    </div>
                `;
            });

            // Compute YoY % for a segment across channels
            function computeYoY(seg, channels, last12Dates, agg, yoyAgg, cfg) {
                return last12Dates.map(d => {
                    if (!d) return null;
                    const yaDate = getYearAgoDate(d);
                    const yaPrefix = yaDate ? yaDate.substring(0, 7) : null;
                    // Find actual ya date in yoyAgg
                    const yaActual = yaPrefix ? Object.keys(yoyAgg).find(k => k.startsWith(yaPrefix)) : null;

                    // Current merged
                    const curMerged = { clients:0, budgetSum:0, checksSum:0, avgCheckSum:0, cardSaleSum:0, realSaleSum:0, ciSum:0, total:0, pushAvail:0, pushCli:0 };
                    channels.forEach(ch => { const s = agg[d]?.[seg+'|'+ch]; if(s) Object.keys(curMerged).forEach(f => { curMerged[f] += s[f]||0; }); });
                    const cur = cfg.extract(curMerged);

                    if (!yaActual || cur == null) return null;
                    const yaMerged = { clients:0, budgetSum:0, checksSum:0, avgCheckSum:0, cardSaleSum:0, realSaleSum:0, ciSum:0, total:0, pushAvail:0, pushCli:0 };
                    channels.forEach(ch => { const s = yoyAgg[yaActual]?.[seg+'|'+ch]; if(s) Object.keys(yaMerged).forEach(f => { yaMerged[f] += s[f]||0; }); });
                    const prev = cfg.extract(yaMerged);

                    if (prev == null || prev === 0) return null;
                    return ((cur - prev) / Math.abs(prev) * 100);
                });
            }

            // YoY label plugin — draws LFL % below x-axis
            function makeYoYPlugin(yoyValues) {
                return {
                    id: 'yoyLabels',
                    afterDraw(chart) {
                        const ctx = chart.ctx;
                        const xAxis = chart.scales.x;
                        const bottom = chart.chartArea.bottom;
                        ctx.save();
                        ctx.font = '600 7px Inter, sans-serif';
                        ctx.textAlign = 'center';
                        xAxis.ticks.forEach((tick, i) => {
                            const x = xAxis.getPixelForTick(i);
                            const val = yoyValues[i];
                            if (val == null) return;
                            ctx.fillStyle = val >= 0 ? '#2E8B57' : '#C41E3A';
                            const txt = (val >= 0 ? '+' : '') + val.toFixed(1) + '%';
                            ctx.fillText(txt, x, bottom + 22);
                        });
                        ctx.restore();
                    }
                };
            }

            // Build line chart for one segment with 2 channels + YoY
            function buildSegChannelChart(canvasId, seg, channels, last12Dates, agg, metric) {
                const cfg = metricsCfg[metric];
                const needsAvg = metric !== 'clients';
                const isOff = channels.includes('только оффлайн');
                const yoyAgg = isOff ? offYoYAgg : ecomYoYAgg;
                destroyChart(canvasId);

                // Per-channel datasets
                const channelDS = channels.map(ch => ({
                    label: CH_LABELS[ch] || ch,
                    data: last12Dates.map(d => {
                        if (!d) return null;
                        const key = seg + '|' + ch;
                        return cfg.extract(agg[d]?.[key]);
                    }),
                    borderColor: CH_COLORS[ch] || '#999',
                    tension: 0.3, pointRadius: 3, borderWidth: 2
                }));

                // Weighted average across all channels for this segment
                if (needsAvg) {
                    const avgData = last12Dates.map(d => {
                        if (!d) return null;
                        const merged = { clients:0, budgetSum:0, checksSum:0, avgCheckSum:0, cardSaleSum:0, realSaleSum:0, ciSum:0, total:0, pushAvail:0, pushCli:0 };
                        channels.forEach(ch => {
                            const k = seg + '|' + ch;
                            const s = agg[d]?.[k];
                            if (!s) return;
                            Object.keys(merged).forEach(f => { merged[f] += s[f] || 0; });
                        });
                        return cfg.extract(merged);
                    });
                    channelDS.push({
                        label: 'Ср. взвеш.',
                        data: avgData,
                        borderColor: '#888',
                        borderDash: [5, 3],
                        tension: 0.3, pointRadius: 0, borderWidth: 1.5
                    });
                }

                // Compute YoY for merged (all channels)
                const yoyValues = computeYoY(seg, channels, last12Dates, agg, yoyAgg, cfg);
                const yoyPlugin = makeYoYPlugin(yoyValues);

                chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
                    type: 'line',
                    data: {
                        labels: last12Labels,
                        datasets: channelDS
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        layout: { padding: { top: 18, bottom: 16 } },
                        scales: {
                            y: {
                                display: true,
                                grid: { display: true, color: 'rgba(0,0,0,0.06)' },
                                ticks: { display: false }
                            },
                            x: { ticks: { ...dynXticks, padding: 8 } }
                        },
                        plugins: {
                            legend: dynLegend,
                            datalabels: {
                                display: ctx => ctx.dataset.borderDash ? false : ctx.dataset.data[ctx.dataIndex] != null,
                                color: ctx => ctx.dataset.borderColor,
                                font: { size: 7, weight: '600' },
                                anchor: 'end', align: 'top', offset: 2, clip: false,
                                formatter: cfg.dlFmt
                            }
                        }
                    },
                    plugins: [ChartDataLabels]
                });
            }

            // Build all charts for offline and ecom
            const offChannels = ['только оффлайн', 'омни'];
            const ecomChannels = ['только e-commerce', 'омни'];

            metrics.forEach(m => {
                buildSegChannelChart('off-' + m + '-active', 'Активные', offChannels, last12Off, offAgg, m);
                buildSegChannelChart('off-' + m + '-new', 'Новые', offChannels, last12Off, offAgg, m);
                buildSegChannelChart('ecom-' + m + '-active', 'Активные', ecomChannels, last12Ecom, ecomAgg, m);
                buildSegChannelChart('ecom-' + m + '-new', 'Новые', ecomChannels, last12Ecom, ecomAgg, m);
            });

            // --- Cross-dynamics: weighted avg of omni from offline + ecom ---
            // Aggregate only "омни" channel from both orgs, merged by segment
            function aggOmniCross(dates) {
                const result = {};
                dates.forEach((d, i) => {
                    if (!d) return;
                    // Get omni rows from both orgs for the same calendar month
                    const offDate = last12Off[i];
                    const ecomDate = last12Ecom[i];
                    const rows = [];
                    if (offDate) rows.push(...CVM_DYNAMICS.filter(r => r.org === 1 && r.date === offDate && r.channel === 'омни'));
                    if (ecomDate) rows.push(...CVM_DYNAMICS.filter(r => r.org === 3 && r.date === ecomDate && r.channel === 'омни'));
                    result[d] = {};
                    rows.forEach(r => {
                        const seg = r.segment;
                        if (!result[d][seg]) result[d][seg] = { clients:0, budgetSum:0, checksSum:0, avgCheckSum:0, cardSaleSum:0, realSaleSum:0, ciSum:0, total:0, pushAvail:0, pushCli:0 };
                        const s = result[d][seg];
                        s.clients += r['клиентов'];
                        s.total += r['клиентов'];
                        if (r.BUDGET != null) s.budgetSum += r['клиентов'] * r.BUDGET;
                        if (r.CHECKS != null) s.checksSum += r['клиентов'] * r.CHECKS;
                        if (r.AVG_CHECK != null) s.avgCheckSum += r['клиентов'] * r.AVG_CHECK;
                        if (r.CARD_SALE != null) s.cardSaleSum += r['клиентов'] * r.CARD_SALE;
                        if (r.REAL_SALE != null) s.realSaleSum += r['клиентов'] * r.REAL_SALE;
                        if (r['ЦИ'] != null) s.ciSum += r['клиентов'] * r['ЦИ'];
                        if (r['Доля доступных для коммуникации'] != null) {
                            s.pushAvail += r['клиентов'] * r['Доля доступных для коммуникации'];
                            s.pushCli += r['клиентов'];
                        }
                    });
                });
                return result;
            }

            // Use offline dates as base for cross (they have the calendar alignment)
            const crossAgg = aggOmniCross(last12Off);

            function buildCrossChart(canvasId, seg, metric) {
                const cfg = metricsCfg[metric];
                destroyChart(canvasId);
                chartInstances[canvasId] = new Chart(document.getElementById(canvasId), {
                    type: 'line',
                    data: {
                        labels: last12Labels,
                        datasets: [{
                            label: 'Омни (средневзв.)',
                            data: last12Off.map(d => d ? cfg.extract(crossAgg[d]?.[seg]) : null),
                            borderColor: '#4A90D9',
                            backgroundColor: '#4A90D922',
                            fill: true,
                            tension: 0.3, pointRadius: 4, borderWidth: 2.5
                        }]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        layout: { padding: { top: 18 } },
                        scales: { y: { display: true, grid: { display: true, color: 'rgba(0,0,0,0.04)' }, ticks: { font: { size: 9 } } }, x: { ticks: { font: { size: 7 } } } },
                        plugins: {
                            legend: { display: false },
                            datalabels: {
                                display: ctx => ctx.dataset.data[ctx.dataIndex] != null,
                                color: '#4A90D9',
                                font: { size: 9, weight: '600' },
                                anchor: 'end', align: 'top', offset: 2, clip: false,
                                formatter: cfg.dlFmt
                            }
                        }
                    },
                    plugins: [ChartDataLabels]
                });
            }

            // Only draw cross charts if cross tab is visible
            const crossVisible = document.getElementById('dyn-cross')?.style.display !== 'none';
            if (crossVisible) {
                metrics.forEach(m => {
                    buildCrossChart('cross-' + m + '-active', 'Активные', m);
                    buildCrossChart('cross-' + m + '-new', 'Новые', m);
                });
            }
        }

        buildTimeSeries();

        // McKinsey-style conclusions — separate for offline and e-commerce
        const offActive = offBySegAll['Активные'];
        const offChurn = offBySegAll['Отток'];
        const offNew = offBySegAll['Новые'];
        const offSleep = offBySegAll['Спящие'];
        const ecomActive = ecomBySegAll['Активные'];
        const ecomChurn = ecomBySegAll['Отток'];
        const ecomNew = ecomBySegAll['Новые'];
        const ecomSleep = ecomBySegAll['Спящие'];

        const offActivePct = offActive ? (offActive.clients / totalOff * 100).toFixed(1) : 0;
        const offChurnPct = offChurn ? (offChurn.clients / totalOff * 100).toFixed(1) : 0;
        const offSleepPct = offSleep ? (offSleep.clients / totalOff * 100).toFixed(1) : 0;
        const offNewPct = offNew ? (offNew.clients / totalOff * 100).toFixed(1) : 0;
        const inactiveOffPct = (parseFloat(offChurnPct) + parseFloat(offSleepPct)).toFixed(1);
        const pushChurnOff = offChurn?.pushShare ? (offChurn.pushShare * 100).toFixed(0) : '—';

        const ecomActivePct = ecomActive ? (ecomActive.clients / totalEcom * 100).toFixed(1) : 0;
        const ecomChurnPct = ecomChurn ? (ecomChurn.clients / totalEcom * 100).toFixed(1) : 0;
        const ecomSleepPct = ecomSleep ? (ecomSleep.clients / totalEcom * 100).toFixed(1) : 0;
        const ecomNewPct = ecomNew ? (ecomNew.clients / totalEcom * 100).toFixed(1) : 0;
        const inactiveEcomPct = (parseFloat(ecomChurnPct) + parseFloat(ecomSleepPct)).toFixed(1);
        const pushChurnEcom = ecomChurn?.pushShare ? (ecomChurn.pushShare * 100).toFixed(0) : '—';

        function conclusionCard(id, title, headline, cards) {
            const el = document.getElementById(id);
            if (!el) return;
            el.innerHTML = `
                <div class="card-header"><h3>${title} — ${dateLabel(date)}</h3></div>
                <div style="padding:20px">
                    <div style="font-size:14px;font-weight:700;color:var(--dixy-orange);margin-bottom:16px;line-height:1.5">${headline}</div>
                    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px">${cards}</div>
                </div>`;
        }
        function makeCard(color, label, text) {
            return `<div style="background:var(--bg);border-radius:8px;padding:14px;border-left:3px solid ${color}">
                <div style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;margin-bottom:6px">${label}</div>
                <div style="font-size:12px;line-height:1.7;color:var(--text)">${text}</div></div>`;
        }

        // Offline conclusions
        conclusionCard('dyn-conclusions-off', 'Ключевые выводы (Оффлайн)',
            `Более половины оффлайн базы (${inactiveOffPct}%) находится в неактивном состоянии — это ключевой барьер для роста ТО программы лояльности`,
            makeCard('#C41E3A', 'Проблема',
                `<strong>${inactiveOffPct}%</strong> оффлайн базы составляют Отток (${offChurnPct}%) и Спящие (${offSleepPct}%) — суммарно <strong>${fmt.int((offChurn?.clients||0)+(offSleep?.clients||0))}</strong> клиентов без покупок.
                PUSH-доступность оттока <strong>${pushChurnOff}%</strong> — потенциал для реактивации.`) +
            makeCard('#E87722', 'Возможность',
                `Доля омни в оффлайн базе — <strong>${omniShareOff != null ? (omniShareOff*100).toFixed(1)+'%' : '—'}</strong>. Омни-клиенты генерируют на 18% выше ARPU.
                Увеличение омниканальности через стимулирование первой онлайн-покупки — стратегический рычаг роста.`) +
            makeCard('#2E8B57', 'Рекомендация',
                `<strong>1.</strong> Реактивация оттока через PUSH (${fmt.int(Math.round((offChurn?.clients||0)*(offChurn?.pushShare||0)))} доступных клиентов).<br>
                <strong>2.</strong> Усилить привлечение — текущая доля новых ${offNewPct}%.<br>
                <strong>3.</strong> Контроль среднего чека: LFL-падение на фоне инфляции сигнализирует об оптимизации корзины покупателем.`)
        );

        // E-commerce conclusions
        conclusionCard('dyn-conclusions-ecom', 'Ключевые выводы (E-commerce)',
            `E-commerce база характеризуется высокой долей неактивных клиентов (${inactiveEcomPct}%) и доминированием омниканальных покупателей (${omniShareEcom != null ? (omniShareEcom*100).toFixed(0)+'%' : '—'})`,
            makeCard('#C41E3A', 'Проблема',
                `Отток (${ecomChurnPct}%) и Спящие (${ecomSleepPct}%) составляют <strong>${inactiveEcomPct}%</strong> e-com базы.
                Средний чек определяется порогом бесплатной доставки — рост чека не отражает органическое изменение поведения.
                Падение базы в ноя.2025 связано с исключением банковских клиентов из CVM.`) +
            makeCard('#E87722', 'Возможность',
                `<strong>${omniShareEcom != null ? (omniShareEcom*100).toFixed(0)+'%' : '—'}</strong> e-com клиентов — омни. Это означает высокий потенциал кросс-канального CVM.
                Доля активных (<strong>${ecomActivePct}%</strong>) значительно ниже оффлайн (<strong>${offActivePct}%</strong>) — пространство для роста вовлечённости.`) +
            makeCard('#2E8B57', 'Рекомендация',
                `<strong>1.</strong> Реактивация e-com оттока через PUSH (доступность ${pushChurnEcom}%).<br>
                <strong>2.</strong> Кросс-канальные механики: стимулировать оффлайн-клиентов к первому онлайн-заказу.<br>
                <strong>3.</strong> Оптимизация порога бесплатной доставки — влияет на конверсию и частоту заказов.`)
        );

        // Hide shared card (replaced by per-tab cards)
        const sharedCard = document.getElementById('dyn-insights-card');
        if (sharedCard) sharedCard.style.display = 'none';

        // --- Raw data table ---
        const rawTable = document.getElementById('dyn-raw-table');
        const allRows = [...offRows, ...ecomRows];
        const cols = [
            ['date','Дата'],
            ['org','Орг'],
            ['channel','Канал'],
            ['segment','Сегмент'],
            ['клиентов','Клиентов'],
            ['PUSH','PUSH'],
            ['Доля доступных для коммуникации','Доля PUSH'],
            ['BUDGET','ARPU'],
            ['CHECKS','Чеков/кл'],
            ['AVG_CHECK','Ср. чек'],
            ['LTV','LTV'],
        ];
        const orgNames = {1:'Offline', 3:'E-commerce'};
        rawTable.innerHTML = `
            <thead><tr>${cols.map(c => `<th>${c[1]}</th>`).join('')}</tr></thead>
            <tbody>${allRows.map(r => `<tr>
                ${cols.map(([k]) => {
                    let v = r[k];
                    if (k === 'org') v = orgNames[v] || v;
                    if (k === 'Доля доступных для коммуникации' && v != null) v = (v*100).toFixed(1)+'%';
                    if (['BUDGET','AVG_CHECK','LTV'].includes(k) && v != null) v = Math.round(v).toLocaleString('ru-RU')+' ₽';
                    if (k === 'клиентов' || k === 'PUSH') v = v != null ? v.toLocaleString('ru-RU') : '—';
                    if (k === 'CHECKS' && v != null) v = v.toFixed(2);
                    return `<td>${v ?? '—'}</td>`;
                }).join('')}
            </tr>`).join('')}</tbody>
        `;

        // Excel export
        document.getElementById('dyn-export-xlsx').onclick = () => {
            let csv = cols.map(c => c[1]).join(';') + '\n';
            allRows.forEach(r => {
                csv += cols.map(([k]) => {
                    let v = r[k];
                    if (k === 'org') v = orgNames[v] || v;
                    if (v == null) return '';
                    return String(v).replace(/;/g, ',');
                }).join(';') + '\n';
            });
            const bom = '\uFEFF';
            const blob = new Blob([bom + csv], { type: 'text/csv;charset=utf-8;' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'динамика_базы_' + date.replace(/-/g,'') + '.csv';
            a.click();
            URL.revokeObjectURL(url);
        };
    }

    renderDynMonth(sel.value);
}

// --- Category Trends ---
// TRENDS_DATA / TRENDS_MONTHS / TRENDS_LABELS / TRENDS_SEGMENTS / TRENDS_SEG_LABELS / TRENDS_SEG_COLORS
// / TRENDS_CITIES / TRENDS_CHANNELS — defined in data.js.
// Источник данных: лист i_trend выгружен ТОЛЬКО с топ-категориями по PER_DELTA_CONTACTS (над-индекс).
// Каждая (сегмент × регион × канал × месяц) bucket = «маркеры миссий» — что покупает сегмент чаще среднего.
// Поля: c=категория, cn=доля покуп., dc=над-индекс, p=цена, ch=чеков/кл, остальное игнорируем.

// Пороги для категории-маркера. cn-фильтр применяется ТОЛЬКО к большим сегментам, где доля
// покупателей статистически осмысленна. Малые сегменты (NEW/RANDOM/HIGH-PRICE) имеют cn≈0 по
// определению — фильтр там обнулял бы данные. Над-индекс (dc) — относительная мера, работает везде.
const TRENDS_MIN_OVERINDEX = 1.10;
const TRENDS_CALENDAR_TOP_N = 7;

// cn-порог по сегменту (0 = не фильтровать)
const TRENDS_MIN_SHARE_BY_SEG = {
    ACTIVE_LFL:  0.005,  // самый широкий сегмент — 0.5% статистически значимо
    BIG_CHECK:   0.005,
    LOW_CHECK:   0.005,
    LOW_PRICE:   0.005,
    HIGH_PRICE:  0,      // мало клиентов — cn ≈ 0, фильтр всё убивает
    NEW:         0,
    RANDOM:      0,
};

function trendsMinShareForSeg(seg) {
    return TRENDS_MIN_SHARE_BY_SEG[seg] ?? 0;
}

// Группировка категорий — analyst lens
const TREND_GROUPS = [
    { key: 'TOBACCO',     label: 'Табак / стики',           color: '#5D4037',
      patterns: ['СИГАРЕТ','СИГАРИЛЛ','СИГАР','ТАБАК','ТАБАЧН','ЭЛЕКТРОННЫЕ ИСПАРИТЕЛИ','ЭЛЕКТРОННЫЕ СИГАРЕТЫ','СТИКИ','ЗАЖИГАЛК'] },
    { key: 'KIDS',        label: 'Детские товары',          color: '#FF8FAB',
      patterns: ['ПОДГУЗНИК','ЗАМЕНИТЕЛИ ГРУДНОГО','КАШИ ДЕТСК','ПЮРЕ ДЕТСК','ПЮРЕ МЯСНЫЕ','ПЮРЕ МЯСО-ОВОЩ','ПЮРЕ ОВОЩ','ПЮРЕ ФРУКТ','ПИТАНИЕ ДЕТСК','ПРИКОРМ','НАБ.КОСМ/ПАРФ ДЕТ','ИГРА НАСТОЛЬН','КОНСТРУКТОР','КУКЛЫ','МЫЛЬНЫЕ ПУЗЫРИ','НАБОР ДЛЯ ТВОРЧЕСТВА','НАБОР ИГРОВОЙ','НАБОР ПЕСОЧНЫЙ','НАБОРЫ ДЛЯ ТВОРЧЕСТВА','РАСКРАСКИ','РОБОТ','ТРАНСФОРМЕР','КОСТЮМ КАРНАВАЛЬН','ПИРОТЕХН','ШАРЫ','ТОВАРЫ ДЛЯ ЛЕПКИ','МОЛОЧНЫЕ КОКТЕЙЛИ Д/ДОШКОЛЬН','СР-ВА Д/МЫТЬЯ ВОЛОС ДЛЯ ДЕТЕЙ','СР-ВА Д/МЫТЬЯ ВОЛОС ДЛЯ НОВО','СР-ВА УХОДА ЗА КОЖЕЙ ДЛЯ ДЕТ','СР-ВА УХОДА ЗА КОЖЕЙ ДЛЯ НОВ','СР-ВА УХОДА ЗА ПОЛОСТЬЮ РТА ДЛЯ ДЕТ','СР-ВА УХОДА ЗА ПОЛОСТЬЮ РТА ДЛЯ НОВ','СР-ВА ДЛЯ КУПАНИЯ','ДЕТСК','ДЛЯ ДЕТЕЙ 2-12','НАУШНИК','ОРУЖИЕ','ДУДКА','НАКЛЕЙК'] },
    { key: 'PETS',        label: 'Корма для животных',      color: '#A0522D',
      patterns: ['ВЛАЖНЫЕ КОРМА','СУХИЕ КОРМА','КОРМ КОШК','КОРМ СОБ','КОРМ ДЛЯ','ЛАКОМСТВ','НАПОЛНИТЕЛИ ТУАЛЕТ','АКСЕССУАРЫ ДЛЯ КОШК','ДЛЯ КОШЕК','ДЛЯ СОБАК'] },
    { key: 'WINE',        label: 'Вино / шампанское',       color: '#722F37',
      patterns: ['ВИНО','ВИНА','ШАМПАНСК','АППЕРИТИВЫ','АСТИ','ВЕРМУТ','КАВА','КАГОР','ЛАМБРУСКО','ПОРТВЕЙН','ПРОСЕККО','ХЕРЕС'] },
    { key: 'STRONG_ALC',  label: 'Крепкий алкоголь',        color: '#3E2723',
      patterns: ['ВИСКИ','КОНЬЯК','ЛИКЕР','РОМ','ТЕКИЛА','ДЖИН','БРЕНДИ','КАЛЬВАДОС','БУРБОН','ДИДЖЕСТИВЫ','НАСТОЙК','НАПИТКИ АЛК','САМБУКА','ВОДК','МЕДОВУХА'] },
    { key: 'BEER_CIDER',  label: 'Пиво / сидр',             color: '#8B6F47',
      patterns: ['ПИВО','СИДР'] },
    { key: 'PREMIUM_FOOD',label: 'Деликатесы / премиум',    color: '#5F259F',
      patterns: ['ИКРА','КРЕВЕТ','КРАБ','РАКООБРАЗ','ЛОСОС','СЕМГА','ФОРЕЛЬ','СЫР С БЕЛОЙ ПЛЕС','СЫР С ГОЛУБОЙ','СЫР С ПЛЕСЕНЬ','ХАМОН','ДЕЛИКАТЕС'] },
    { key: 'COLD_CUTS',   label: 'Колбасы / нарезки',       color: '#C0392B',
      patterns: ['ВЕТЧИНА','КОЛБАС','КУПАТ','САРДЕЛЬК','САЛО','СОСИСК','ПАШТЕТ','ШПИК','ЗЕЛЬЦ','ЗАЛИВНОЕ'] },
    { key: 'READY_FOOD',  label: 'Готовая еда / СП',        color: '#E87722',
      patterns: ['СУШИ','РОЛЛ','ОНИГИРИ','ГОТОВЫЕ БЛЮДА','ГОТОВЫЕ ГАРНИРЫ','БЛЮДА С','БЛЮДА БЫСТР','БЛЮДА ИЗ ЯИЦ','БЛЮДА МОЛОЧ','БЛЮДА МУЧН','БЛЮДА МЯСН','БЛЮДА НАЦИО','БЛЮДА ОВОЩ','БЛЮДА ЯЙЧ','ВТОРЫЕ БЛЮДА','ВОСТОЧНАЯ КУХНЯ','ЗАВТРАКИ','ЗАКУСК','МОНО-БЛЮДА','ПЕРВЫЕ БЛЮДА','САЛАТ','СУПЫ','ЛАПША МОМЕНТ','ПЮРЕ МОМЕНТ','ПРОЧИЕ МОМЕНТ','СУПЫ МОМЕНТ','СНЕКИ ГОРЯЧИЕ','ВЫПЕЧК','ПИРОГ','ПИЦЦА','БУРГЕР','СЭНДВИЧ','ПЕЛЬМЕН','ВАРЕНИК','НАГГЕТС','СУРИМИ','СП'] },
    { key: 'FRESH_MEAT',  label: 'Мясо / птица свежие',     color: '#C41E3A',
      patterns: ['ГОВЯДИН','СВИНИН','БАРАНИН','КУРИЦ','КУРИНОЕ','ИНДЕЙК','ФАРШ','ПТИЦ ОХЛ','МЯСО ОХЛ','МЯСО ВЕС','СУБПРОДУКТ','П/Ф НАТУРАЛ','ПРОЧАЯ ПТИЦА'] },
    { key: 'FISH',        label: 'Рыба / морепродукты',     color: '#1B5E8A',
      patterns: ['РЫБА','КИЛЬК','САЙРА','САРДИНА','САРДИНЕЛЛ','СКУМБРИ','ШПРОТ','ТУНЕЦ','ПЕЧЕНИ РЫБ','ПРЕСЕРВ','СНЭКИ ИЗ РЫБЫ','МОРЕПРОДУКТ'] },
    { key: 'FRESH_FRUIT', label: 'Фрукты',                  color: '#2E8B57',
      patterns: ['БАНАН','ЯБЛОК','ВИНОГРАД','МАНДАРИН','АПЕЛЬСИН','ГРУШ','КИВИ','АВОКАДО','АНАНАС','АРБУЗ','ВИШН','ГРАНАТ','ГРЕЙПФРУТ','ГОЛУБИК','ЧЕРНИК','ДЫНЯ','ИНЖИР','КЛУБНИК','КОКОС','ЛАЙМ','ЛИЧИ','МАЛИН','МАНГО','ПАПАЙЯ','ПЕРСИК','ПОМЕЛО','СВИТИ','СЛИВА','ФЕЙХОА','ФИНИК','ФИЗАЛИС','ХУРМА','ЧЕРЕШН','ЛИМОН','ЭКЗОТИЧ','АБРИКОС','ЯГОДЫ','ИЗЮМ','КУРАГА','ЧЕРНОСЛИВ','ЦУКАТ','СУХОФРУКТ','ОРЕХ','АРАХИС','ГРЕЦКИЙ','КЕШЬЮ','МИНДАЛЬ','ФУНДУК','ФИСТАШК','СЕМЕЧК'] },
    { key: 'FRESH_VEG',   label: 'Овощи / зелень',          color: '#88B04B',
      patterns: ['ТОМАТ','ОГУРЕЦ','ОГУРЦЫ','ПЕРЕЦ','КАБАЧ','КАПУСТ','МОРКОВ','КАРТОФ','ЛУК','ЗЕЛЕН','САЛАТ ЗЕЛ','АЙСБЕРГ','РУККОЛА','ПЕТРУШК','УКРОП','СЕЛЬДЕРЕЙ','ИМБИРЬ','ЧЕСНОК','СВЕКЛА','ТЫКВА','РЕДИС','ШАМПИНЬОН','БАКЛАЖАН','ВЕШЕНК','ГРИБЫ','ЦВЕТНАЯ','БРОККОЛИ','КОЛЬРАБИ','АССОРТИ ОВОЩ','КУКУРУЗА'] },
    { key: 'DAIRY',       label: 'Молочка / яйца',          color: '#4A90D9',
      patterns: ['МОЛОКО','СМЕТАН','ТВОРОГ','ТВОРОЖ','ЙОГУРТ','СЫР','КЕФИР','РЯЖЕНК','СЛИВК','МАСЛО СЛИВ','СПРЕД','ЯЙЦА','СЫРОК','СЫВОРОТКА','НАЦИОНАЛЬНЫЕ КИСЛОМ','МОЛОЧНЫЕ КОКТЕЙЛИ','РАСТИТЕЛЬНЫЕ ДЕСЕРТЫ','РАСТИТЕЛЬНЫЕ НАПИТКИ','ПУДИНГ','ДЕСЕРТЫ МОЛОЧ','МАРГАРИН','МАСЛО ПРОЧЕЕ','МАСЛО СМЕС','ТРАДИЦИОННЫЕ'] },
    { key: 'BAKERY',      label: 'Хлеб / выпечка',          color: '#FFB81C',
      patterns: ['ХЛЕБ','БАТОН','БАГЕТ','БУЛК','КУЛИЧ','ПАНЕТТОНЕ','КРУАССАН','ДОНАТ','ПОНЧИК','МАФФИН','СДОБ','МЕЛКОШТУЧ','ХБИ','ИЗДЕЛИЯ Х/Б','ИЗДЕЛИЯ СЛАДКИЕ ДЕФРОСТ','КОРЖИ','ТАРТАЛЕТК','СОЛОМКА','РУЛЕТ','ТЕСТО','ПОЛУФАБРИКАТЫ ИЗ ТЕСТА','СУХАРИ МУЧНЫЕ'] },
    { key: 'CONFECTIONERY',label:'Кондитерка / снеки',      color: '#9B59B6',
      patterns: ['ЧИПС','СНЕК','СНЭК','ШОКОЛАД','КОНФЕТ','БАТОНЧИК','ЖЕВАТЕЛЬ','ЖВАЧК','МАРМЕЛАД','ЗЕФИР','ХАЛВ','ПАСТИЛ','ВОСТОЧНЫЕ СЛАДОСТИ','ДРАЖЕ','КАРАМЕЛЬ','ЛЕДЕНЦ','МУЧНЫЕ СОЛЕНЫЕ СНЭКИ','ПОПКОРН','СЛАДОСТ','ДЕСЕРТ','МОРОЖЕНОЕ','ПЕЧЕНЬЕ','ТОРТ','ПИРОЖН','КЕКС','ПРЯН','ВАФЛИ','ВАФЕЛЬН','СУШКИ','СУХАРИК','КРЕКЕР','ГАЛЕТ','КОНДИТЕРСК'] },
    { key: 'BEVERAGES',   label: 'Напитки безалк.',         color: '#00897B',
      patterns: ['ВОДА','СОК','ЧАЙ','КОФЕ','ЛИМОНАД','КВАС','ГАЗИРОВ','ЭНЕРГЕТ','НАПИТ','НЕКТАР','МОРС','КОЛА','ТОНИК','СМУЗИ','ЛЕД ПИЩ','ХОЛОДНЫЕ ЧАИ'] },
    { key: 'GROCERY',     label: 'Бакалея',                 color: '#003A70',
      patterns: ['МУКА','САХАР','СОЛЬ','КРУП','РИС','ГРЕЧ','МАКАРОН','МАСЛО ПОДС','МАСЛО ОЛИВК','УКСУС','СПЕЦИИ','ПРИПРАВ','СОУС','КЕТЧУП','МАЙОНЕЗ','ГОРЧИЦ','КОНСЕРВ','ВАРЕНЬЕ','КОНФИТЮР','МЁД','МЕД','АДЖИКА','ВАНИЛИН','ДЖЕМ','ПОВИДЛО','ДРОЖЖИ','ЗЕРНОВЫЕ','КАШИ','КОНЦЕНТРАТЫ БУЛЬОН','КРАХМАЛ','КУКУРУЗН','ЛАПША','ЛЕЧО','МУЧНЫЕ СМЕСИ','МЮСЛИ','ОВСЯН','ПИЩЕВЫЕ','ПРОЧИЕ ОВОЩ','ПШЕНО','СОЛЕНЬ','ФУНКЦИОНАЛЬН','ПАСТА СЛАДКАЯ','ДОБАВКИ','ДИАБЕТИКА','ДИЕТИКА','БАДЫ'] },
    { key: 'FROZEN',      label: 'Заморозка',               color: '#7DB3E0',
      patterns: ['ЗАМОРОЖ',' ЗАМ','ЗАМ ','ОВОЩИ И СМЕСИ ЗАМ','СНЭКИ ЗАМ','СУПЫ ЗАМ','ТЕСТО ЗАМ','ГРИБЫ ЗАМ','РЫБНЫЕ П/Ф ЗАМ','КОНДИТЕРСКИЕ ИЗДЕЛИЯ ЗАМ'] },
    { key: 'BEAUTY_HEALTH',label:'Красота / здоровье',      color: '#EC407A',
      patterns: ['ПРОКЛАДК','УРОЛОГИЧ','ГИГИЕН','ДЕЗОДОРАНТ','КРЕМ ДЛЯ','ПЕНА','ГЕЛЬ','ПОМАД','ТУШЬ','ЛАК','БРИТВ','СТАНОК','СТАНКИ','КАССЕТЫ','БАЛЬЗАМ','КОНДИЦИОНЕР','ОПОЛАСКИВАТЕЛ','ГЕЛИ ДЛЯ ДУША','СОЛИ ДЛЯ ВАНН','НАБ.КОСМ/ПАРФ','КОСМ/ПАРФ','СПОНЖ','НАБОРЫ ДЕКОРАТИВНОЙ КОСМЕТИКИ','ПЛАТКИ НОСОВЫЕ БУМАЖНЫЕ','РАСЧЕСК','ПРЕЗЕРВАТИВ','ТЕСТ НА БЕРЕМ','ПЛАСТЫРИ','КАПСУЛЫ','СИЗ','МЕДИЦИНСКИЕ','ТАМПОНЫ','ДЕТСКИЕ ВЛАЖНЫЕ САЛФЕТКИ','СР-ВА Д/МЫТЬЯ ВОЛОС','СР-ВА УХОДА','СР-ВА ДЛЯ КУПАНИЯ'] },
    { key: 'HOUSEHOLD',   label: 'Бытхим / уборка',         color: '#B0BEC5',
      patterns: ['БУМАГА','ТУАЛЕТНАЯ','САЛФЕТК','ШАМПУН','МЫЛО','ЗУБН','ПАСТА ЗУБ','МОЮЩ','СТИРАЛЬН','ЧИСТЯЩ','ОСВЕЖИТ','ОТБЕЛИВ','ПОРОШ','КОНДИЦИОНЕР ДЛЯ БЕЛ','ОТ ТАРАКАНОВ','СРЕДСТВ','ГЕЛИ ДЛЯ СТИРКИ','ДЛЯ КОВРОВ','ДЛЯ МЫТЬЯ','ДЛЯ ПЛИТЫ','ДЛЯ САНТЕХНИКИ','ДЛЯ СТЕКОЛ','ДЛЯ УДАЛЕНИЯ ЗАСОРОВ','СПРЕИ','АЭРОЗОЛИ','ПЕРЧАТКИ ХОЗ'] },
    { key: 'GARDEN',      label: 'Дача / огород',           color: '#558B2F',
      patterns: ['ГРУНТ','УДОБРЕН','СЕМЕНА','САЖЕНЦЫ','ГОРШК','БАРБЕКЮ','МАНГАЛ','КОПТИЛЬН','САДОВЫЕ ИНСТР','ОБОРУДОВАНИЕ ДЛЯ ПОЛИВА','САЖЕНЦЫ ЦВЕТОВ','САЖЕНЦЫ РАСТЕНИЙ','ПРИНАДЛЕЖНОСТИ ДЛЯ БАРБЕКЮ'] },
    { key: 'SCHOOL',      label: 'Школа / канцелярия',      color: '#1976D2',
      patterns: ['ТЕТРАДИ','ДНЕВНИКИ','КАРАНДАШИ','РУЧКИ','ФЛОМАСТЕР','МАРКЕР','АЛЬБОМ','БЛОКНОТ','КИСТИ','КРАСКИ','КЛЕЙ','КОРРЕКТОР','ШТРИХ','СКОТЧ','ПАПК','КАНЦЕЛЯР','МЕЛК','РАСКРАСК','КНИГИ ДЕТСК','КНИГИ ХУДО','ПОСОБИ','КАЛЕНДАРИ'] },
    { key: 'SEASONAL_CLOTH',label:'Сезонная одежда / обувь', color: '#FF7043',
      patterns: ['НОСКИ','КОЛГОТКИ','ТАПОЧК','ОДЕЖД','БЕЛЬ','ГОЛОВНЫЕ УБОРЫ','ШАРФЫ','ПЕРЧАТКИ','ПЛАТКИ ЖЕН','ПЛАТКИ МУЖ','БРЮКИ','РУБАШК','ФУТБОЛК','ТОПЫ','РЕЗИНОВАЯ','ЗИМНЯЯ ОБУВЬ','ШЛЕПКИ','ПАНТОЛЕТЫ','СЛАНЦЫ','ЗОНТЫ','СУМК'] },
    { key: 'HOLIDAY_DECOR',label: 'Праздничный декор',      color: '#D81B60',
      patterns: ['ЕЛКИ','ЕЛОЧНЫЕ','ПАСХАЛЬН','БУКЕТ','СУВЕНИР','МИШУРА','ДОЖДИК','КОНФЕТТИ','СЕРПАНТИН','ОТКРЫТКИ','ПОДАРОЧНАЯ','НОВОГОДН','РОЖДЕСТВЕНСК','ЭЛЕКТРИЧЕСКИЕ УКРАШЕНИЯ'] },
    { key: 'KITCHENWARE', label: 'Посуда / кухня',          color: '#6D4C41',
      patterns: ['ПОСУД','ТЕКСТИЛЬ','ПОЛОТЕНЦ','БОКАЛ','ФУЖЕР','СТАКАН','КУХОННАЯ','ВЕДРА','ТАЗЫ','ГРАФИНЫ','КУВШИНЫ','БАНКИ','ДОСКИ','НОЖИ','ДУРШЛАГ','СИТО','ЕМКОСТИ','КАСТРЮЛИ','СКОВОРОД','ЧАШКИ','КРУЖКИ','ТАРЕЛК','СПИЦЫ','СТОЛОВЫЕ СЕРВИЗЫ','ФОРМА ДЛЯ ЗАПЕКАНИЯ','КРЫШКИ ДЛЯ КОНСЕРВАЦИИ','ТЕРМОПАКЕТ','КОФР','КОНТЕЙНЕР','КОРЗИНА','ПЛАСТИКОВЫЕ','СОЛОМИНК','ШПАЖК','ПАЛОЧКИ','ЗУБОЧИСТК','ПЛЕНКА','АЛЮМИНИЕВАЯ ФОЛЬГА','ГУБК','ТРЯПК','ШВАБРЫ','ВЕНИКИ','СОВКИ','МЕШКИ','ПАКЕТЫ ДЛЯ ЗАПЕКАНИЯ','ПАКЕТЫ ДЛЯ ЛЬДА'] },
    { key: 'NONFOOD',     label: 'Прочий нон-фуд',          color: '#777777',
      patterns: ['ИНСТРУМЕНТ','ЖУРНАЛ','ТОПЛИВ','СВЕЧИ','БАТАРЕЙК','ПОДУШК','ЧАСЫ','АКСЕССУАР','БИЖУТЕРИЯ','ЛАМПА','СВЕТИЛЬНИК','ОЧКИ','МОБИЛЬНЫЕ ТЕЛЕФОНЫ','НАУШНИК','МЕППИНГ','ГАЛАМАРТ','Р/У','Д/У','МОДЕЛЬ','АВТОМО','КОВРИК','ДЕКОР','ПРЕДМЕТЫ','ПРИНАДЛЕЖНОСТИ','ОБОРУДОВАНИЕ','ТОВАРЫ ЗА ПОКУПКУ','ТОВАРЫ ДЛЯ ОЧИСТКИ','МАТЕРИАЛ','ИГРУШК','КНИГИ','ПРОЧИЕ'] },
];

const TREND_OTHER = { key: 'OTHER', label: 'Прочее', color: '#9CA3AF' };

// Совпадение паттерна с НАЧАЛОМ слова: перед паттерном не должно быть кириллической/латинской буквы.
// Иначе "СОК" находится в "ПЕСОК" и "ВЫСОКОКАЛОРИЙНЫЙ", путая категории.
function _patternMatchesAtWordStart(text, pattern) {
    let from = 0;
    while (true) {
        const idx = text.indexOf(pattern, from);
        if (idx === -1) return false;
        if (idx === 0) return true;
        const prev = text[idx - 1];
        if (!/[А-ЯЁA-Z]/i.test(prev)) return true;
        from = idx + 1;
    }
}

// "X ДЛЯ Y" — существительное в X (тип товара), Y — назначение (модификатор).
// "КРЫШКИ ДЛЯ КОНСЕРВАЦИИ" → главная часть "КРЫШКИ" (нон-фуд), а не "КОНСЕРВ" в Y.
// Если категория начинается с "ДЛЯ" — это весь паттерн (например "ДЛЯ КОВРОВ" из бытхима).
function _mainPart(C) {
    if (C.startsWith('ДЛЯ ')) return C;
    const m = C.match(/^(.+?)\s+ДЛЯ\s+/);
    return m ? m[1].trim() : C;
}

function groupOfCategory(cat) {
    if (!cat) return TREND_OTHER;
    const C = cat.toUpperCase();
    const main = _mainPart(C);
    // Pass 1: ищем по существительному (главной части до " ДЛЯ ")
    if (main !== C) {
        for (const g of TREND_GROUPS) {
            for (const p of g.patterns) {
                if (_patternMatchesAtWordStart(main, p)) return g;
            }
        }
    }
    // Pass 2: fallback на полную строку
    for (const g of TREND_GROUPS) {
        for (const p of g.patterns) {
            if (_patternMatchesAtWordStart(C, p)) return g;
        }
    }
    return TREND_OTHER;
}

function trendsChannelKey() {
    return window._activeChannel === 'ecom' ? 'ECOM' : 'OFF';
}

function trendsCityLabel(c) {
    return c === 'Moscow + MO' ? 'Москва + МО' : (c === 'Spb+LO' ? 'СПб + ЛО' : 'Прочие регионы');
}

const TRENDS_MONTH_FULL = ['', 'ЯНВАРЬ', 'ФЕВРАЛЬ', 'МАРТ', 'АПРЕЛЬ', 'МАЙ', 'ИЮНЬ',
                           'ИЮЛЬ', 'АВГУСТ', 'СЕНТЯБРЬ', 'ОКТЯБРЬ', 'НОЯБРЬ', 'ДЕКАБРЬ'];

let _trendsState = { seg: 'ACTIVE_LFL', city: 'Moscow + MO', metric: null };

// Default metric per (segment, channel): 'overindex' | 'frequency'
function trendsDefaultMetric(seg, ch) {
    // У NEW/RANDOM в офлайне сильнее показатель частоты (dch), у остальных — над-индекс (dc).
    if (ch === 'OFF' && (seg === 'NEW' || seg === 'RANDOM')) return 'frequency';
    return 'overindex';
}
function trendsCurrentMetric() {
    if (_trendsState.metric) return _trendsState.metric;
    return trendsDefaultMetric(_trendsState.seg, trendsChannelKey());
}

function buildTrends() {
    if (typeof TRENDS_DATA === 'undefined') return;
    const container = document.getElementById('tab-trends');

    // Segment sub-tabs (BIG_CHECK style)
    const segTabs = TRENDS_SEGMENTS.map(seg => {
        const c = TRENDS_SEG_COLORS[seg];
        const active = seg === _trendsState.seg;
        return `<button class="seg-sub-btn ${active ? 'active' : ''}" data-seg="${seg}" style="${active ? `--accent:${c}` : ''}">
            <span class="seg-sub-dot" style="background:${c}"></span>${TRENDS_SEG_LABELS[seg]}
        </button>`;
    }).join('');

    // Region — single dropdown
    const cityOptions = TRENDS_CITIES.map(cy =>
        `<option value="${cy}"${cy === _trendsState.city ? ' selected' : ''}>${trendsCityLabel(cy)}</option>`
    ).join('');
    // Channel — single dropdown (same style)
    const currentCh = window._activeChannel === 'ecom' ? 'ecom' : 'offline';
    const channelOptions = [
        ['offline', 'Оффлайн'],
        ['ecom',    'E-commerce'],
    ].map(([k, lbl]) => `<option value="${k}"${k === currentCh ? ' selected' : ''}>${lbl}</option>`).join('');
    // Metric — what to optimize for (over-index of contacts vs frequency of purchase)
    const curMetric = trendsCurrentMetric();
    const metricOptions = [
        ['overindex', 'Над-индекс (доля покупателей)'],
        ['frequency', 'Частота покупок (чеков/клиента)'],
    ].map(([k, lbl]) => `<option value="${k}"${k === curMetric ? ' selected' : ''}>${lbl}</option>`).join('');

    container.innerHTML = `
        <div class="page-header">
            <div>
                <h1>Тренды категорий по сегментам</h1>
                <p class="subtitle">Адвент-календарь миссий: что каждый сегмент покупает чаще среднего · по месяцам, регионам и каналам</p>
            </div>
        </div>

        <div class="seg-group-controls">
            <div class="seg-sub-tabs" id="trends-seg-tabs">${segTabs}</div>
        </div>

        <div class="trends-region-filter">
            <span class="trends-region-label">РЕГИОН:</span>
            <select id="trends-city-select" class="trends-region-select">${cityOptions}</select>
            <span class="trends-region-label" style="margin-left:14px">КАНАЛ:</span>
            <select id="trends-channel-select" class="trends-region-select">${channelOptions}</select>
            <span class="trends-region-label" style="margin-left:14px">МЕТРИКА:</span>
            <select id="trends-metric-select" class="trends-region-select" style="min-width:240px">${metricOptions}</select>
        </div>

        <div id="trends-content"></div>
    `;

    container.querySelectorAll('#trends-seg-tabs .seg-sub-btn').forEach(b =>
        b.addEventListener('click', () => {
            _trendsState.seg = b.dataset.seg;
            _trendsState.metric = null; // вернуться на дефолтную метрику для нового сегмента
            buildTrends();
        }));
    document.getElementById('trends-city-select').addEventListener('change', e => {
        _trendsState.city = e.target.value;
        buildTrends();
    });
    document.getElementById('trends-channel-select').addEventListener('change', e => {
        // Сбрасываем метрику чтобы при смене канала вернуться на дефолт для нового среза
        _trendsState.metric = null;
        setActiveChannel(e.target.value);
    });
    document.getElementById('trends-metric-select').addEventListener('change', e => {
        _trendsState.metric = e.target.value;
        buildTrends();
    });

    renderTrends();
}

function trendsBucketRaw(seg, city, ym, ch) {
    const key = `${seg}|${city}|${ch}|${ym}`;
    return (TRENDS_DATA[key] || []);
}

function trendsBucket(seg, city, ym, ch) {
    const minShare = trendsMinShareForSeg(seg);
    if (minShare <= 0) return trendsBucketRaw(seg, city, ym, ch);
    return trendsBucketRaw(seg, city, ym, ch).filter(r => r.cn != null && r.cn >= minShare);
}

// Сколько категорий-маркеров есть в указанном срезе (без фильтра шума)
function trendsCoverage(seg, city, ch) {
    let total = 0;
    for (const ym of TRENDS_MONTHS) {
        total += trendsBucketRaw(seg, city, ym, ch).length;
    }
    return total;
}

// Найти комбинацию (city, channel) с максимальным покрытием для сегмента
function trendsBestCoverage(seg) {
    const opts = [];
    for (const c of TRENDS_CITIES) for (const ch of TRENDS_CHANNELS) {
        opts.push({ city: c, ch, n: trendsCoverage(seg, c, ch) });
    }
    opts.sort((a, b) => b.n - a.n);
    return opts;
}

// Минимальный порог для метрики «Частота покупок» (dch)
const TRENDS_MIN_DCH = 0.10;          // рост частоты ≥ 10% YoY

function renderTrends() {
    const { seg, city } = _trendsState;
    const ch = trendsChannelKey();
    const metric = trendsCurrentMetric(); // 'overindex' | 'frequency'
    const segLabel = TRENDS_SEG_LABELS[seg];
    const segColor = TRENDS_SEG_COLORS[seg];
    const cityLabel = trendsCityLabel(city);
    const channelLabel = ch === 'ECOM' ? 'E-commerce' : 'Оффлайн';

    // Метрика-зависимый фильтр и сортировка. Только относительные пороги — никаких cn-фильтров.
    function passesMetric(r) {
        if (metric === 'frequency') {
            return r.dch != null && r.dch >= TRENDS_MIN_DCH;
        }
        return r.dc != null && r.dc >= TRENDS_MIN_OVERINDEX;
    }
    function metricValue(r) {
        return metric === 'frequency' ? (r.dch || 0) : (r.dc || 0);
    }

    let monthsData = TRENDS_MONTHS.map(ym => {
        const rows = trendsBucket(seg, city, ym, ch);
        const top = [...rows]
            .filter(passesMetric)
            .sort((a, b) => metricValue(b) - metricValue(a));
        return { ym, label: TRENDS_LABELS[ym], rows, top };
    });
    let totalCats = monthsData.reduce((s, m) => s + m.top.length, 0);

    // Если в этом срезе вообще нет данных — показываем подсказку с лучшими альтернативами
    if (totalCats === 0) {
        const alts = trendsBestCoverage(seg).filter(o => o.n > 0).slice(0, 3);
        const altsHtml = alts.length ? `
            <div style="margin-top:18px;font-size:13px;color:#444">
                Доступные срезы для <strong>${segLabel}</strong>:
                <div style="display:flex;gap:8px;justify-content:center;flex-wrap:wrap;margin-top:10px">
                    ${alts.map(a => {
                        const cl = trendsCityLabel(a.city);
                        const chl = a.ch === 'ECOM' ? 'E-commerce' : 'Оффлайн';
                        return `<button class="trends-alt-btn" data-city="${a.city}" data-ch="${a.ch}">
                            ${cl} · ${chl} <span style="opacity:0.7;font-size:11px">(${a.n} маркеров)</span>
                        </button>`;
                    }).join('')}
                </div>
                <div style="font-size:11px;color:#888;margin-top:10px">
                    Сегмент <strong>${segLabel}</strong> в выбранном канале/регионе либо отсутствует, либо его слишком мало для статистики.
                </div>
            </div>` : `<div style="margin-top:14px;font-size:12px;color:#888">У этого сегмента нет данных ни в одном срезе.</div>`;

        document.getElementById('trends-content').innerHTML = `
            <div class="card" style="text-align:center;padding:40px 30px;color:#444">
                <div style="font-size:32px;margin-bottom:12px">🤷</div>
                <div style="font-size:15px;font-weight:600">Нет данных для <strong>${segLabel} · ${cityLabel} · ${channelLabel}</strong></div>
                ${altsHtml}
            </div>`;

        document.querySelectorAll('.trends-alt-btn').forEach(b => b.addEventListener('click', () => {
            _trendsState.city = b.dataset.city;
            // переключаем глобальный канал, если нужно
            const targetCh = b.dataset.ch === 'ECOM' ? 'ecom' : 'offline';
            if (window._activeChannel !== targetCh) {
                setActiveChannel(targetCh);
            } else {
                buildTrends();
            }
        }));
        return;
    }

    // Year-summary: aggregate all top categories across months -> group counts
    const yearAll = [];
    for (const m of monthsData) for (const r of m.top) yearAll.push(r);
    const yearGroups = aggregateGroups(yearAll, metric);
    const yearTopCats = topCategoriesAcrossYear(monthsData);

    const headlineHTML = renderTrendsHeadline(seg, ch, city, yearGroups, yearTopCats);
    const calendarHTML = renderAdventCalendar(monthsData, segColor, metric);
    const groupsHTML = renderGroupAnalysis(yearGroups, segLabel);
    const narrativeHTML = renderTrendsNarrative(seg, city, ch, yearGroups, yearTopCats);

    document.getElementById('trends-content').innerHTML = `
        <div class="trends-context-bar" style="border-left:6px solid ${segColor}">
            <div class="trends-context-title" style="color:${segColor}">${segLabel}</div>
            <div class="trends-context-meta">
                <span><strong>${cityLabel}</strong></span>
                <span class="dot">·</span>
                <span><strong>${channelLabel}</strong></span>
                <span class="dot">·</span>
                <span>15 месяцев · ${totalCats} категорий-маркеров</span>
                <span class="dot">·</span>
                <span style="color:#6b7b8d">${metric === 'frequency' ? `Δ-частота YoY ≥ ${(TRENDS_MIN_DCH*100).toFixed(0)}%` : `над-индекс ≥ x ${TRENDS_MIN_OVERINDEX.toFixed(2)}`}${trendsMinShareForSeg(seg) > 0 ? ` · доля покуп. ≥ ${(trendsMinShareForSeg(seg)*100).toFixed(1)}%` : ''}</span>
            </div>
        </div>
        ${headlineHTML}
        <div class="card" style="margin-top:16px">
            <div style="padding:14px 18px 4px;color:#6b7b8d;font-size:12px;line-height:1.55">
                ${metric === 'frequency'
                    ? `В каждой ячейке месяца — категории, которые сегмент покупает <strong>чаще обычного</strong> (рост частоты YoY ≥ ${(TRENDS_MIN_DCH*100).toFixed(0)}%), сгруппированные в миссии. Подходит для NEW/RANDOM в офлайне, где доля покупателей минимальная, но те, кто покупает — делают это часто.`
                    : `В каждой ячейке месяца — все над-индексные категории (что сегмент покупает чаще среднего), сгруппированные в миссии.`}
                Цвет полоски = доминирующая миссия месяца. Внутри: число категорий, ${metric === 'frequency' ? 'средняя/max Δ-частота' : 'средний/max над-индекс'} и примеры.
            </div>
            ${calendarHTML}
        </div>
        ${groupsHTML}
        ${narrativeHTML}
    `;
}

function renderAdventCalendar(monthsData, segColor, metric) {
    metric = metric || 'overindex';
    const isFreq = metric === 'frequency';
    let html = '<div class="advent-calendar">';
    for (const m of monthsData) {
        const monthNum = parseInt(m.ym.slice(4, 6));
        const year = m.ym.slice(0, 4);
        const monthName = TRENDS_MONTH_FULL[monthNum];
        if (m.top.length === 0) {
            html += `
                <div class="advent-cell empty">
                    <div class="advent-month">${monthName}<span class="advent-year">${year}</span></div>
                    <div class="advent-empty">Нет данных</div>
                </div>`;
            continue;
        }
        const groupsInMonth = groupsOfMonth(m.top, metric);
        const totalCats = m.top.length;
        const dominantGroup = groupsInMonth[0];
        const headStripe = dominantGroup ? dominantGroup.color : segColor;
        const hdrLabel = isFreq ? 'категорий с ростом частоты' : 'категорий с над-индексом';
        html += `
            <div class="advent-cell" style="border-top:4px solid ${headStripe}">
                <div class="advent-month">${monthName}<span class="advent-year">${year}</span></div>
                <div class="advent-month-tag">${totalCats} ${hdrLabel} · ${groupsInMonth.length} миссий${dominantGroup ? ` · преобл. <strong style="color:${dominantGroup.color}">${dominantGroup.label}</strong>` : ''}</div>
                <div class="advent-groups">
                    ${groupsInMonth.map(g => {
                        const examples = g.items.slice(0, 3).map(r => r.c);
                        const stat = isFreq
                            ? `${g.items.length} кат · ср. ${fmtPctDch(g.avgVal)} · max ${fmtPctDch(g.maxVal)}`
                            : `${g.items.length} кат · ср. x ${g.avgVal.toFixed(2)} · max x ${g.maxVal.toFixed(2)}`;
                        return `
                            <div class="advent-group-block" style="border-left:3px solid ${g.color}">
                                <div class="advent-group-head">
                                    <span class="advent-group-name" style="color:${g.color}">${g.label}</span>
                                    <span class="advent-group-stat">${stat}</span>
                                </div>
                                <div class="advent-group-examples">${examples.map(c => `<span title="${c}">${shorten(c, 28)}</span>`).join(' · ')}${g.items.length > 3 ? ` <span class="advent-group-more">+${g.items.length - 3}</span>` : ''}</div>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>`;
    }
    html += '</div>';
    return html;
}

function fmtPctDch(v) {
    if (v == null) return '—';
    return (v >= 0 ? '+' : '') + (v * 100).toFixed(0) + '%';
}

// Группируем категории месяца по analyst group, сортируем по числу категорий.
// Метрика влияет только на агрегаты (avgVal/maxVal): dc для over-index, dch для frequency.
function groupsOfMonth(items, metric) {
    const isFreq = metric === 'frequency';
    const valOf = r => isFreq ? (r.dch || 0) : (r.dc || 0);
    const map = {};
    for (const r of items) {
        const g = groupOfCategory(r.c);
        if (!map[g.key]) map[g.key] = { ...g, items: [], valSum: 0, maxVal: 0 };
        map[g.key].items.push(r);
        map[g.key].valSum += valOf(r);
        map[g.key].maxVal = Math.max(map[g.key].maxVal, valOf(r));
    }
    const arr = Object.values(map);
    arr.forEach(g => {
        g.avgVal = g.valSum / g.items.length;
        g.items.sort((a, b) => valOf(b) - valOf(a));
    });
    arr.sort((a, b) => b.items.length - a.items.length || b.avgVal - a.avgVal);
    return arr;
}

function topGroupOfList(items) {
    if (!items.length) return null;
    const counts = {};
    for (const r of items) {
        const g = groupOfCategory(r.c);
        if (!counts[g.key]) counts[g.key] = { ...g, n: 0, dcSum: 0 };
        counts[g.key].n += 1;
        counts[g.key].dcSum += (r.dc || 0);
    }
    const arr = Object.values(counts);
    arr.sort((a, b) => b.n - a.n || b.dcSum - a.dcSum);
    return arr[0];
}

function aggregateGroups(items, metric) {
    const isFreq = metric === 'frequency';
    const valOf = r => isFreq ? (r.dch || 0) : (r.dc || 0);
    // base = "обычный уровень" — для over-index это 1.0 (равно среднему сегменту), для частоты 0.0 (нет роста YoY)
    const base = isFreq ? 0 : 1;
    const map = {};
    for (const g of [...TREND_GROUPS, TREND_OTHER]) map[g.key] = { ...g, items: [], valSum: 0, occurrences: 0, impact: 0 };
    for (const r of items) {
        const g = groupOfCategory(r.c);
        const v = valOf(r);
        map[g.key].items.push(r);
        map[g.key].occurrences += 1;
        map[g.key].valSum += v;
        // impact = сумма «избытка» над базой → группы с реально яркими аномалиями всплывают наверх,
        // даже если категорий немного. Базовые ежедневные категории при dc≈1.1 дают слабый impact.
        map[g.key].impact += Math.max(0, v - base);
    }
    const res = [];
    for (const k of Object.keys(map)) {
        const m = map[k];
        if (!m.occurrences) continue;
        m.avgDc = m.valSum / m.occurrences;
        m.avgVal = m.avgDc;
        res.push(m);
    }
    // Ранжирование: основное — impact (отличительность для сегмента), вторичное — число вхождений
    res.sort((a, b) => b.impact - a.impact || b.occurrences - a.occurrences);
    return res;
}

function topCategoriesAcrossYear(monthsData) {
    // Aggregate by category name: how many months it appeared as "marker" + best dc
    const bag = {};
    for (const m of monthsData) {
        for (const r of m.top) {
            if (!bag[r.c]) bag[r.c] = { c: r.c, months: [], maxDc: 0, sumDc: 0, n: 0 };
            bag[r.c].months.push({ ym: m.ym, label: m.label, dc: r.dc, cn: r.cn });
            bag[r.c].maxDc = Math.max(bag[r.c].maxDc, r.dc);
            bag[r.c].sumDc += r.dc;
            bag[r.c].n += 1;
        }
    }
    const arr = Object.values(bag);
    arr.sort((a, b) => b.n - a.n || b.maxDc - a.maxDc);
    return arr;
}

// Audience implications — что значит, что в топе сегмента появились эти группы.
// Только описательные интерпретации, без додумывания. Каждая группа = аудитория или миссия.
const GROUP_AUDIENCE = {
    KIDS:          { audience: 'семьи с маленькими детьми (мамы с детьми, молодые родители)',
                     signal: 'life-event миссия — «магазин рядом для срочных детских покупок»',
                     niche: 'ниши: подгузники, заменители молока, детские пюре, игрушки' },
    PETS:          { audience: 'владельцы кошек / собак',
                     signal: 'регулярная нужда — корм и наполнитель туалетов берут в одном чеке',
                     niche: 'ниши: влажные/сухие корма, лакомства, наполнители' },
    WINE:          { audience: 'миссия «к столу / в подарок» — клиенты, выбирающие ДИКСИ как источник вина',
                     signal: 'часто привязано к календарным праздникам — 23 февраля, 8 марта, новогодние',
                     niche: 'ниши: страновые тихие вина (Италия, Франция), просекко, шампанское' },
    STRONG_ALC:    { audience: 'клиенты с подарочными или поводными миссиями',
                     signal: 'пики совпадают с праздниками — 23 февраля, 8 марта, новогодние, корпоративы',
                     niche: 'ниши: виски, коньяк, ром — премиальный и нишевый алкоголь' },
    BEER_CIDER:    { audience: 'импульсная летняя миссия — пиво к посиделкам, пикникам',
                     signal: 'летний пик; часто в комбо с орехами / снеками',
                     niche: '' },
    PREMIUM_FOOD:  { audience: 'премиум-потребитель — выбирает качество, не цену',
                     signal: 'устойчивая покупка деликатесов; чувствительность к цене ниже среднего',
                     niche: 'ниши: икра, креветки, лосось, сыры с плесенью, хамон' },
    COLD_CUTS:     { audience: 'ежедневная закупка к чаю / завтраку',
                     signal: 'базовый ассортимент, маркер устоявшейся покупки',
                     niche: '' },
    READY_FOOD:    { audience: 'миссия «перекус / быстрый обед» — городской ритм',
                     signal: 'часто у молодой аудитории, RTD-формат, ритуал «дорога домой»',
                     niche: 'ниши: суши/роллы, готовые блюда, СП, пицца' },
    FRESH_MEAT:    { audience: 'готовка дома',
                     signal: 'плановая закупка — мясо/птица под недельное меню',
                     niche: '' },
    FISH:          { audience: 'локальный аппетит / ЗОЖ-потребитель',
                     signal: 'в СПб особенно сильно из-за региональной кулинарной традиции',
                     niche: '' },
    FRESH_FRUIT:   { audience: 'ежедневная закупка / ЗОЖ',
                     signal: 'базовая корзина; усиливается сезонно (ягоды летом, мандарины зимой)',
                     niche: 'летние ниши: сухофрукты, орехи (дачный перекус)' },
    FRESH_VEG:     { audience: 'готовка дома / ежедневная закупка',
                     signal: 'базовая корзина; летний пик по огурцам/томатам/зелени',
                     niche: '' },
    DAIRY:         { audience: 'ежедневная закупка',
                     signal: 'базовая корзина — молочка ходит в каждом чеке',
                     niche: '' },
    BAKERY:        { audience: 'ежедневная закупка',
                     signal: 'хлеб = главный повод визита; СП-выпечка = «к столу»',
                     niche: '' },
    CONFECTIONERY: { audience: 'импульс / семьи с детьми',
                     signal: 'часто пикует у новых клиентов и сегментов с детьми',
                     niche: '' },
    BEVERAGES:     { audience: 'импульс / перекус',
                     signal: 'городской ритм; летний пик по воде/квасу/морсу',
                     niche: 'ниши: RTD-кофе у молодой аудитории «по дороге»' },
    GROCERY:       { audience: 'плановая закупка / готовка дома',
                     signal: 'базовый ассортимент — крупы, масло, специи берут редко но устойчиво',
                     niche: '' },
    FROZEN:        { audience: 'закупка впрок — характерно для большого чека и e-comm',
                     signal: 'хранится долго — берут реже, но в больших объемах',
                     niche: '' },
    BEAUTY_HEALTH: { audience: 'женская / семейная аудитория',
                     signal: 'гигиена, прокладки, средства по уходу — указывают на регулярную семейную закупку',
                     niche: '' },
    HOUSEHOLD:     { audience: 'плановая хозяйственная закупка',
                     signal: 'характерно для большого чека и e-comm — «впрок»',
                     niche: '' },
    GARDEN:        { audience: 'дачники, садоводы, любители барбекю',
                     signal: 'сезонный пик март–июнь (старт дачного сезона), отдельный пик август (заготовки)',
                     niche: 'грунт, удобрения, саженцы, мангал, садовый инвентарь' },
    SCHOOL:        { audience: 'родители школьников, студенты',
                     signal: 'сезонный пик август–сентябрь (подготовка к школе), второй пик январь (вторая четверть)',
                     niche: 'тетради, ручки, краски, дневники, рюкзаки' },
    SEASONAL_CLOTH:{ audience: 'клиенты с экстренной/сезонной потребностью в одежде/обуви',
                     signal: 'климатический фактор — носки/колготки в феврале (морозы), уход за обувью в ноябре (слякоть), шлёпки летом',
                     niche: 'часто это «забыл/прохудилось» — магазин у дома выигрывает за счёт скорости' },
    HOLIDAY_DECOR: { audience: 'миссия «подготовка к празднику»',
                     signal: 'пики совпадают с календарём: декабрь (Новый год), март–апрель (Пасха), февраль–март (8 марта)',
                     niche: 'ёлки, мишура, пасхальные сувениры, открытки, подарочная упаковка' },
    KITCHENWARE:   { audience: 'миссия «срочно нужна посуда / утварь»',
                     signal: 'часто разовая покупка — посуда, формы для запекания, контейнеры',
                     niche: 'может пикать перед праздничным застольем (декабрь, март)' },
    NONFOOD:       { audience: 'разнородные нишевые подгруппы',
                     signal: 'остаточная категория после выделения мисcий — мелочи, аксессуары, инструменты',
                     niche: '' },
    TOBACCO:       { audience: 'курильщики / клиенты с импульсной миссией',
                     signal: 'импульс, часто привязано к ежедневному маршруту',
                     niche: '' },
    OTHER:         { audience: '', signal: '', niche: '' },
};

// Авто-формирование headline — без выдумок, на основе фактических топ-групп.
function buildAutoPitch(seg, ch, yearGroups) {
    if (!yearGroups.length) return '';
    const channelLabel = ch === 'ECOM' ? 'e-commerce' : 'оффлайн';
    const top = yearGroups.slice(0, 3);
    const audiences = top.map(g => {
        const meta = GROUP_AUDIENCE[g.key];
        return meta && meta.audience ? `<strong>${g.label}</strong> (${meta.audience})` : `<strong>${g.label}</strong>`;
    });
    return `Топ-3 группы среди над-индексных категорий в ${channelLabel}: ${audiences.join('; ')}.`;
}

// Раскладка категорий-маркеров по ПРИСУТСТВИЮ во времени (без сравнения долей между сегментами).
// persistent: появляется в ≥ 6 месяцах из 15 — устойчивый маркер
// regular:    появляется в 4–5 месяцах — частый маркер
// seasonal:   появляется в 1–3 месяцах — сезонный/событийный пик
function classifyMarkers(yearTopCats) {
    const out = { persistent: [], regular: [], seasonal: [] };
    for (const c of yearTopCats) {
        if (c.n >= 6) out.persistent.push(c);
        else if (c.n >= 4) out.regular.push(c);
        else out.seasonal.push(c);
    }
    return out;
}

// Сегмент-специфический контекст: в чём суть сегмента (не миссии)
const SEGMENT_FRAME = {
    ACTIVE_LFL: {
        nature: 'постоянные лояльные клиенты с самой широкой корзиной',
        focus:  'над-индекс показывает категории, где доля покупателей сегмента выше, чем у среднего клиента',
    },
    NEW: {
        nature: 'клиенты, впервые покупающие в ДИКСИ',
        focus:  'над-индекс показывает категории, которые новые клиенты выбирают чаще, чем средний клиент — поводы первого визита',
    },
    RANDOM: {
        nature: 'редкие/случайные клиенты, заходящие за конкретным поводом',
        focus:  'над-индекс показывает категории, которые случайные выбирают чаще, чем средний клиент — триггеры визита',
    },
    BIG_CHECK: {
        nature: 'клиенты с большим средним чеком (~2× от базового)',
        focus:  'над-индекс показывает категории, доля покупателей в которых выше у этого сегмента, чем в среднем чеке',
    },
    LOW_CHECK: {
        nature: 'клиенты с маленьким средним чеком — точечный визит',
        focus:  'над-индекс показывает категории, которые этот сегмент выбирает чаще, чем средний клиент',
    },
    HIGH_PRICE: {
        nature: 'клиенты, выбирающие позиции с высокой ценой SKU',
        focus:  'над-индекс показывает категории, доля покупателей в которых выше у этого сегмента, чем в среднем ценовом сегменте',
    },
    LOW_PRICE: {
        nature: 'клиенты, выбирающие самые дешёвые позиции',
        focus:  'над-индекс показывает категории, доля покупателей в которых выше у этого сегмента, чем в среднем ценовом сегменте',
    },
};

function renderTrendsHeadline(seg, ch, city, yearGroups, yearTopCats) {
    const segColor = TRENDS_SEG_COLORS[seg];
    const segLabel = TRENDS_SEG_LABELS[seg];
    const top5Groups = yearGroups.slice(0, 5);
    if (!top5Groups.length) return '';

    const frame = SEGMENT_FRAME[seg] || { nature: '', focus: '' };

    // Уникальные категории внутри каждой миссии (за все 15 месяцев)
    const missions = top5Groups.map((g, idx) => {
        const meta = GROUP_AUDIENCE[g.key] || {};
        // dedupe by category name, keep best-value record (dc или dch в зависимости от метрики)
        const byCat = {};
        for (const r of g.items) {
            const cur = byCat[r.c];
            const v = (r.dc || 0);
            if (!cur || v > (cur.dc || 0)) byCat[r.c] = r;
        }
        const cats = Object.values(byCat).sort((a, b) => (b.dc || 0) - (a.dc || 0));
        return { idx: idx + 1, group: g, meta, cats };
    });

    // Уникальные «маркеры» сегмента (категории с топовым impact, не базовые)
    const segMarkers = yearTopCats
        .filter(c => c.maxDc >= 1.5)
        .slice(0, 8);

    return `
        <div class="trends-headline-card" style="border-left:5px solid ${segColor};margin-top:16px">
            <div class="trends-headline-title">📌 Профиль миссий: ${segLabel}</div>
            <div class="trends-segment-frame">
                <strong>Кто это:</strong> ${frame.nature}.
                <strong>Что показывает анализ:</strong> ${frame.focus}.
            </div>
            ${segMarkers.length ? `<div class="trends-segment-markers">
                <strong>Уникальные маркеры сегмента:</strong>
                ${segMarkers.map(c => `<span class="trends-mini-chip" title="${c.c}">${c.c} <em>x ${c.maxDc.toFixed(2)}</em></span>`).join('')}
            </div>` : ''}
            <div class="trends-mission-list">
                ${missions.map(({ idx, group, meta, cats }) => `
                    <div class="trends-mission" style="border-left:4px solid ${group.color}">
                        <div class="trends-mission-head">
                            <span class="trends-mission-num" style="background:${group.color}">${idx}</span>
                            <span class="trends-mission-name" style="color:${group.color}">${group.label}</span>
                            <span class="trends-mission-stat">${cats.length} ${pluralCats(cats.length)} · ${group.occurrences} попаданий за 15 мес. · ср. x ${group.avgDc.toFixed(2)} · impact ${group.impact.toFixed(1)}</span>
                        </div>
                        ${meta.audience ? `<div class="trends-mission-meta"><strong>Аудитория:</strong> ${meta.audience}</div>` : ''}
                        ${meta.signal ? `<div class="trends-mission-meta"><strong>Сигнал:</strong> ${meta.signal}</div>` : ''}
                        <div class="trends-mission-cats">
                            <strong>Что в этой миссии:</strong>
                            ${cats.slice(0, 18).map(r => `<span class="trends-mini-chip" title="${r.c}">${r.c} <em>x ${r.dc.toFixed(2)}</em></span>`).join('')}
                            ${cats.length > 18 ? `<span class="trends-mission-more">+${cats.length - 18} ещё</span>` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    `;
}

function pluralCats(n) {
    const m100 = n % 100;
    if (m100 >= 11 && m100 <= 14) return 'категорий';
    const m10 = n % 10;
    if (m10 === 1) return 'категория';
    if (m10 >= 2 && m10 <= 4) return 'категории';
    return 'категорий';
}

function renderGroupAnalysis(yearGroups, segLabel) {
    if (!yearGroups.length) return '';
    const top = yearGroups.slice(0, 12);
    const max = Math.max(...top.map(g => g.occurrences), 1);
    let html = `<div class="card" style="margin-top:16px"><div class="card-header"><h3>🎯 Аналитика по группам категорий</h3></div>`;
    html += `<div style="padding:6px 18px 0;color:#6b7b8d;font-size:12px;line-height:1.55">
        Сколько раз категории каждой группы попадали в над-индексные за все 15 месяцев. Топ-группы = ядро миссии сегмента.
    </div>`;
    html += '<div class="trends-groups-grid">';
    for (const g of top) {
        const pct = g.occurrences / max * 100;
        const top3 = [...g.items].sort((a, b) => b.dc - a.dc).slice(0, 3);
        // dedupe top3 by category
        const seen = new Set();
        const uniq = [];
        for (const r of top3) {
            if (seen.has(r.c)) continue;
            seen.add(r.c);
            uniq.push(r);
        }
        html += `
            <div class="trends-group-card">
                <div class="trends-group-head">
                    <div class="trends-group-dot" style="background:${g.color}"></div>
                    <div class="trends-group-name">${g.label}</div>
                    <div class="trends-group-idx" style="color:${g.color}">${g.occurrences}</div>
                </div>
                <div class="trends-group-bar-simple">
                    <div class="trends-group-bar-simple-fill" style="width:${pct}%;background:${g.color}"></div>
                </div>
                <div class="trends-group-meta">ср. над-индекс x ${g.avgDc.toFixed(2)}</div>
                <div class="trends-group-top">
                    ${uniq.map(r => `<div class="trends-group-top-row"><span>${shorten(r.c, 30)}</span><strong style="color:${g.color}">x ${r.dc.toFixed(2)}</strong></div>`).join('')}
                </div>
            </div>`;
    }
    html += '</div></div>';
    return html;
}

function renderTrendsNarrative(seg, city, ch, yearGroups, yearTopCats) {
    const segLabel = TRENDS_SEG_LABELS[seg];
    const segColor = TRENDS_SEG_COLORS[seg];
    const channelLabel = ch === 'ECOM' ? 'E-commerce' : 'Оффлайн';
    const cityLabel = trendsCityLabel(city);
    const top5Groups = yearGroups.slice(0, 5);
    const cls = classifyMarkers(yearTopCats);

    // Авто-генерация headline на основе фактического распределения групп
    const autoHeadline = buildAutoHeadline(top5Groups, cls);

    // Авто-генерация интерпретаций по группам (что значит, что они в топе)
    const groupInterpretations = top5Groups.map(g => {
        const meta = GROUP_AUDIENCE[g.key];
        if (!meta || !meta.audience) return null;
        const examples = [...g.items].sort((a, b) => b.dc - a.dc).slice(0, 4).map(r => r.c);
        // dedup
        const uniqEx = [...new Set(examples)];
        return { group: g, meta, examples: uniqEx };
    }).filter(x => x);

    const recs = trendsRecommendations(seg, ch, top5Groups);

    return `
        <div class="segment-insight" style="border-left-color:${segColor};margin-top:16px">
            <div class="segment-insight-header">
                <div class="icon" style="background:${segColor};color:#fff">!</div>
                <h3>Что говорят данные</h3>
            </div>
            <div style="padding:14px 20px;background:#fafafa;border-bottom:1px solid var(--border);font-size:14px;font-weight:600;color:var(--text);line-height:1.55">
                ${autoHeadline}
            </div>

            <div style="padding:16px 22px;border-bottom:1px solid var(--border)">
                <div style="font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;color:#2c3e50;margin-bottom:12px">
                    🔍 Что значат эти группы (от данных, не от гипотез)
                </div>
                ${groupInterpretations.map(({ group, meta, examples }) => `
                    <div class="trends-audience-row">
                        <div class="trends-audience-tag" style="background:${group.color}1a;color:${group.color};border:1px solid ${group.color}55">
                            ${group.label} · ${group.occurrences} мес. · ср. x ${group.avgDc.toFixed(2)}
                        </div>
                        <div class="trends-audience-body">
                            <div><strong>Аудитория:</strong> ${meta.audience}</div>
                            <div style="color:#6b7b8d;margin-top:3px"><strong>Сигнал:</strong> ${meta.signal}</div>
                            ${meta.niche ? `<div style="color:#6b7b8d;margin-top:3px"><strong>Что в топе:</strong> ${meta.niche}</div>` : ''}
                            <div style="margin-top:6px;font-size:11px;color:#888"><strong>Примеры:</strong> ${examples.slice(0, 4).map(e => `<span class="trends-mini-chip" style="font-size:10px">${e}</span>`).join('')}</div>
                        </div>
                    </div>
                `).join('')}
            </div>

            <div class="insight-grid">
                <div class="insight-block">
                    <div class="insight-block-title">
                        <span class="tag tag-positive">УСТОЙЧИВЫЕ</span>
                        Маркеры в ≥ 6 из 15 месяцев
                    </div>
                    <ul>${cls.persistent.slice(0, 6).map(c => `<li><strong>${c.c}</strong> — ${c.n} мес., max x ${c.maxDc.toFixed(2)}</li>`).join('') || '<li>Нет устойчивых маркеров — у сегмента очень разнообразный профиль покупок от месяца к месяцу.</li>'}</ul>
                </div>
                <div class="insight-block">
                    <div class="insight-block-title">
                        <span class="tag tag-growth">ЧАСТЫЕ</span>
                        Маркеры в 4–5 месяцах
                    </div>
                    <ul>${cls.regular.slice(0, 6).map(c => `<li><strong>${c.c}</strong> — ${c.n} мес., max x ${c.maxDc.toFixed(2)}</li>`).join('') || '<li>—</li>'}</ul>
                </div>
                <div class="insight-block">
                    <div class="insight-block-title">
                        <span class="tag tag-warning">СЕЗОННЫЕ</span>
                        Появляются 1–3 месяца
                    </div>
                    <ul>${cls.seasonal.slice(0, 6).map(c => {
                        const ms = c.months.slice(0,3).map(m => m.label).join(', ');
                        return `<li><strong>${c.c}</strong> — ${ms}, x ${c.maxDc.toFixed(2)}</li>`;
                    }).join('') || '<li>—</li>'}</ul>
                </div>
            </div>

            <div class="insight-rec-block">
                <div class="rec-title">Рекомендации CVM (на основе доминирующих групп)</div>
                <div class="rec-items">
                    ${recs.map(r => `<div class="rec-item"><strong>${r.title}</strong>${r.text}</div>`).join('')}
                </div>
            </div>
        </div>
    `;
}

// Headline = что показывают данные. Без выдуманных историй.
function buildAutoHeadline(top5Groups, cls) {
    const parts = [];
    parts.push(`<strong>${cls.persistent.length}</strong> устойчив${cls.persistent.length === 1 ? 'ый маркер' : 'ых маркеров'} (≥ 6 мес.), <strong>${cls.regular.length}</strong> част${cls.regular.length === 1 ? 'ый' : 'ых'} (4–5 мес.), <strong>${cls.seasonal.length}</strong> сезонн${cls.seasonal.length === 1 ? 'ый' : 'ых'} (1–3 мес.)`);
    let head = parts.join(', ') + '.';
    if (top5Groups.length) {
        const topNames = top5Groups.slice(0, 3).map(g => g.label).join(' / ');
        head += ` Доминирующие миссии: <strong>${topNames}</strong>.`;
    }
    return head;
}

function trendsRecommendations(seg, ch, topGroups) {
    const isEcom = ch === 'ECOM';
    const r = [];
    const groupKeys = (topGroups || []).map(g => g.key);
    const has = k => groupKeys.includes(k);

    // Универсальные рекомендации, основанные на фактических топ-группах
    if (has('KIDS'))         r.push({ title: 'Триггер «семьи с детьми»',  text: 'Среди топ-групп — детские товары. Целевые пуш-уведомления и подборки для родителей: подгузники по подписке, детское питание, СТМ для детей.' });
    if (has('PETS'))         r.push({ title: 'Триггер «владельцы питомцев»',text: 'Над-индекс по корму. Подписка на корм + персональные подборки лакомств. Можно делать триггер «корм заканчивается».' });
    if (has('WINE') || has('STRONG_ALC')) r.push({ title: 'Праздничные миссии', text: 'Над-индекс по алкоголю → подарочные сеты к 23 февраля / 8 марта / новогодним. Готовая выкладка «вино + книга» / «виски + закуска».' });
    if (has('READY_FOOD'))   r.push({ title: 'RTD / готовая еда',         text: 'Над-индекс по СП и готовой еде → расширение полки горячих перекусов, HORECA-механики, кофе-пойнт у входа.' });
    if (has('PREMIUM_FOOD')) r.push({ title: 'Премиум-сервис',            text: 'Над-индекс по деликатесам → ранний доступ к новинкам, персональные подборки премиум-СТМ.' });
    if (has('NONFOOD'))      r.push({ title: 'Микро-подгруппы нон-фуда',  text: 'Если в нон-фуде грунт/удобрения, канцелярия или сезонная обувь — таргетированные кампании на узкие подсегменты (дачники / родители школьников), не на весь сегмент.' });
    if (has('FROZEN') || has('HOUSEHOLD')) r.push({ title: 'Закупка впрок', text: 'Над-индекс по заморозке/бытхиму → бандлы и чек-бонусы (скидка при чеке X+).' });
    if (has('BAKERY') || has('DAIRY')) r.push({ title: 'Базовая ежедневная закупка', text: 'Хлеб, молочка — повод визита. Кросс-сел: «к молоку — творог со скидкой», «к хлебу — масло».' });

    // Фолбэк: общие рекомендации по сегменту
    if (seg === 'NEW') {
        if (isEcom) {
            r.push({ title: 'Welcome-серия 30/60/90', text: 'Welcome-купон на детские/корм + триггер «к второму заказу — скидка на молочку и фреш».' });
            r.push({ title: 'Понизить порог доставки', text: 'На 2-й заказ — снять барьер «дополнить корзину».' });
        } else {
            r.push({ title: 'Сезонные миссии повода', text: 'Готовые подарочные сеты (вино + книга) к 23 февраля / 8 марта. Выкладка у входа.' });
            r.push({ title: 'Welcome для молодых родителей', text: 'Карта ДИКСИ + сертификат при выписке из роддома или регистрации в ЗАГСе.' });
        }
        r.push({ title: 'Расширение корзины', text: 'Кросс-сел через приложение: «попробуйте к молоку — каши и творог со скидкой 20%».' });
    } else if (seg === 'ACTIVE_LFL') {
        r.push({ title: 'Удержание лояльных', text: 'Персональные подборки на основе индивидуальных «маркеров» сегмента (top-5 над-индекс категорий).' });
        r.push({ title: 'Раннее предупреждение', text: 'Triggered-кампания при YoY-падении частоты по любимой категории — миграция к конкуренту.' });
        r.push({ title: 'Развитие СТМ в маркерах', text: 'Рост СТМ в топ-группах сегмента (+2–3 п.п. доли) даст прямой рост маржи.' });
    } else if (seg === 'RANDOM') {
        r.push({ title: 'Конверсия в Active', text: 'Купон на основную категорию (молоко, мясо, фреш) при покупке любой импульсной — расширяет повод визита.' });
        r.push({ title: 'Триггер по локации', text: 'Push-уведомление при близости к магазину с купоном на «полную корзину».' });
        r.push({ title: 'Программа постоянного', text: 'Бонусы за серию из 3 чеков подряд → формирование привычки.' });
    } else if (seg === 'BIG_CHECK') {
        r.push({ title: 'Удержание VIP', text: 'Программа лояльности с накопительными бонусами на чек > 2000 ₽.' });
        r.push({ title: 'Развитие нон-фуда', text: 'Расширение полки бытхима и текстиля, сезонные подборки — драйвер чека.' });
        r.push({ title: 'Кросс-сел премиум', text: 'Персональные подборки премиум-категорий (вино, сыр, готовая еда) для роста маржи.' });
    } else if (seg === 'LOW_CHECK') {
        r.push({ title: 'Бандлы', text: '«2-й товар бесплатно» к основным категориям → расширение чека.' });
        r.push({ title: 'Купон на сопутствующее', text: '«К хлебу — масло сливочное со скидкой» — рост ширины корзины.' });
        r.push({ title: 'Чек 500+', text: 'Бонусы за чек выше 500 ₽ — стимулирует увеличение размера чека.' });
    } else if (seg === 'HIGH_PRICE') {
        r.push({ title: 'Premium-сервис', text: 'Отдельная касса, бесплатная упаковка, ранний доступ к новинкам.' });
        r.push({ title: 'Премиум-СТМ', text: 'Развитие линейки премиум-СТМ — улучшение маржи без снижения лояльности.' });
        r.push({ title: 'Эксклюзивные категории', text: 'Limited-edition позиции, сезонные коллаборации с премиум-брендами.' });
    } else if (seg === 'LOW_PRICE') {
        r.push({ title: 'Апсейл к MIDDLE', text: 'Купоны на «средние» SKU в категориях, где сегмент берет first-price.' });
        r.push({ title: 'СТМ first-price', text: 'Развитие первоценовых СТМ для удержания экономного клиента.' });
        r.push({ title: 'Бонусы за серию', text: '«5 чеков подряд = бонус» — формирование привычки и снижение оттока.' });
    }
    return r;
}

function shorten(s, n) {
    if (!s) return '';
    return s.length > n ? s.slice(0, n - 1) + '…' : s;
}


// --- Segment Group (Price / Check) ---
const PRICE_CHECK_INSIGHTS = {
    HIGH: {
        headline: 'Премиальный сегмент по цене SKU: компактная корзина из дорогих позиций, меньшая частота визитов и небольшая доля от выручки.',
        blocks: [
            { title: 'Клиентская база', tag: 'neutral', tagText: 'СПЕЦ-СЕГМЕНТ',
              points: ['Меньшая доля от базы (~19%) и от выручки (~13–14%)', 'Покупают реже остальных ценовых сегментов — count_check ниже', 'ARPU ниже, чем у MIDDLE — меньший вклад в выручку, но премиальный профиль покупки'] },
            { title: 'Корзина', tag: 'positive', tagText: 'ПРЕМИУМ',
              points: ['Средний чек обеспечивается премиум-ценой SKU (самая высокая avg cost SKU)', 'SKU/чек ниже среднего — компактная корзина из дорогих позиций'] },
            { title: 'Скидка и цена', tag: 'warning', tagText: 'ВНИМАНИЕ',
              points: ['Низкая чувствительность к скидкам — ценят сервис и качество', 'Реальная скидка ниже, чем у других сегментов'] },
        ],
        recs: [
            { title: 'Программа удержания', text: 'Персональные предложения, эксклюзивные категории, ранний доступ к новинкам.' },
            { title: 'Премиум-СТМ', text: 'Развитие линейки СТМ для премиума — способ улучшить экономику без потери лояльности.' },
            { title: 'Сервис vs скидка', text: 'Инвестиции в сервис (доставка, упаковка, отдельная касса) дают больший ROI, чем глубокие скидки.' },
        ]
    },
    MIDDLE: {
        headline: 'Массовый сегмент — ядро выручки сети. Самый высокий ARPU и самый широкий чек среди ценовых сегментов.',
        blocks: [
            { title: 'Клиентская база', tag: 'growth', tagText: 'ЯДРО ВЫРУЧКИ',
              points: ['Одна из двух крупнейших долей среди ценовых сегментов (~40%)', 'Самый высокий ARPU и средний чек среди price-сегментов', 'Высокая частота визитов — count_check выше HIGH и LOW'] },
            { title: 'Чек и корзина', tag: 'positive', tagText: 'ШИРОКАЯ',
              points: ['Самый высокий средний чек среди price-сегментов', 'Самая широкая корзина (SKU/чек) — базовые категории + регулярные премиум'] },
            { title: 'Потенциал роста', tag: 'positive', tagText: 'УПСЕЛЛ',
              points: ['Каждый процент апселла к премиум-цене SKU = существенный прирост выручки', 'Чувствительность к качественным предложениям выше, чем у LOW'] },
        ],
        recs: [
            { title: 'Апселл-кампания', text: 'Целевые промо на категории с высокой ценой SKU (нон-фуд, бытхим, готовая еда).' },
            { title: 'Купоны на размер чека', text: '«Скидка X% при чеке от Y ₽» — стимулирует увеличение чека через бандлы.' },
            { title: 'Клуб 1500+', text: 'Бонусы за чек выше определённого порога — формирует привычку к большому чеку.' },
        ]
    },
    LOW: {
        headline: 'Эконом-сегмент: выбирают позиции с самой низкой ценой SKU. Большая база с потенциалом по росту чека.',
        blocks: [
            { title: 'Клиентская база', tag: 'neutral', tagText: 'ШИРОКАЯ',
              points: ['Одна из двух крупнейших долей наряду с MIDDLE (~40%)', 'Самый низкий средний чек среди price-сегментов'] },
            { title: 'Поведение', tag: 'warning', tagText: 'ЭКОНОМНЫЙ',
              points: ['Самый низкий avg cost SKU — выбирают экономные позиции', 'Высокая чувствительность к скидкам — реальная скидка наибольшая'] },
            { title: 'Потенциал', tag: 'positive', tagText: 'РОСТ ЧЕКА',
              points: ['Главный рычаг — рост среднего чека через апсейл к MIDDLE-цене SKU', 'Расширение корзины и переход в более премиальные категории'] },
        ],
        recs: [
            { title: 'Скидка на сопутствующее', text: '«Купи X — получи Y со скидкой» расширяет корзину без снижения общей маржи.' },
            { title: 'Промо на базовые', text: 'Стимулирование частых покупок базовых категорий (молоко, хлеб, мясо).' },
            { title: 'Программа постоянного клиента', text: 'Бонусы за серию покупок (5 чеков подряд = бонус) — формирует привычку.' },
        ]
    },
    BIG_CHECK: {
        headline: 'Клиенты с большим чеком: широкая корзина с долей нон-фуд, плановый закупочный тур. Ядро маржи на одного клиента.',
        blocks: [
            { title: 'Корзина', tag: 'growth', tagText: 'ШИРОКАЯ',
              points: ['Средний чек в ~2–2.5x выше базового AVG_CHECK', 'SKU/чек значительно выше среднего — широкая корзина за визит', 'Высокая ср. цена SKU объясняется долей нон-фуд (бытхим, посуда, текстиль), а не премиум-полками'] },
            { title: 'Поведение', tag: 'positive', tagText: 'СТАБИЛЬНОСТЬ',
              points: ['Большой чек = плановый закупочный тур', 'Меньшая чувствительность к скидкам, выше — к качеству'] },
            { title: 'Удержание', tag: 'warning', tagText: 'РИСК',
              points: ['Уход 1 клиента = потеря большой выручки', 'Требуется early-warning на изменения паттерна'] },
        ],
        recs: [
            { title: 'Удержание VIP', text: 'Программа лояльности с бонусами за частоту больших чеков.' },
            { title: 'Расширение корзины', text: 'Кросс-сел премиальных категорий через персональные подборки.' },
            { title: 'Сервис', text: 'Бесплатная доставка, отдельные кассы, premium-сервис для топ-1%.' },
        ]
    },
    AVG_CHECK: {
        headline: 'Базовый клиент: средний чек, частые визиты. Доминирующий сегмент по чековости — более половины базы.',
        blocks: [
            { title: 'Клиентская база', tag: 'growth', tagText: 'ДОМИНИРУЮЩИЙ',
              points: ['Более половины базы (~52%) — самый крупный сегмент по чековости', 'Привычка покупать «как обычно»'] },
            { title: 'Корзина', tag: 'neutral', tagText: 'БАЗОВАЯ',
              points: ['Стандартный чек: основные категории + минимум премиум', 'Низкая доля СТМ премиум'] },
            { title: 'Потенциал', tag: 'positive', tagText: 'УВЕЛИЧЕНИЕ ЧЕКА',
              points: ['Целевые рекомендации могут поднять чек на 5–10%', 'Большой эффект от бандлов и комбо-предложений'] },
        ],
        recs: [
            { title: 'Бандл-промо', text: '«2-й товар в подарок» / «3-й бесплатно» — увеличение позиций в чеке.' },
            { title: 'Купон на премиум', text: 'Скидка на премиум-категорию при покупке базовой → знакомство с ассортиментом.' },
            { title: 'Контекстные подборки', text: 'Рекомендации сопутствующих товаров на чеке/в приложении.' },
        ]
    },
    LOW_CHECK: {
        headline: 'Клиенты с низким чеком: точечные покупки, низкая корзина. Цель — рост среднего чека и частоты.',
        blocks: [
            { title: 'Клиентская база', tag: 'neutral', tagText: 'ВХОДНОЙ',
              points: ['Часто новые/случайные клиенты', 'Тестовые покупки — единичные товары'] },
            { title: 'Корзина', tag: 'warning', tagText: 'УЗКАЯ',
              points: ['Узкая корзина: 2–4 SKU за визит', 'Высокая чувствительность к скидкам и промо-выкладкам'] },
            { title: 'Конверсия', tag: 'positive', tagText: 'ПОТЕНЦИАЛ',
              points: ['Цель — конверсия в AVG_CHECK через расширение корзины', 'Welcome-серии с бонусом за следующий чек'] },
        ],
        recs: [
            { title: 'Welcome-промо', text: '«Скидка X% при втором чеке» — стимулирует возврат и рост корзины.' },
            { title: 'Точечные категории', text: 'Скидки на сопутствующие товары: «купил молоко — получи скидку на хлеб».' },
            { title: 'Минимальный чек для бонуса', text: 'Бонусы начисляются от чека 500/700/1000 ₽ — формирует привычку чека.' },
        ]
    }
};

let _segGroupState = {
    'price_seg': { channel: 'offline', ym: null, sub: 'SUMMARY' },
    'check_seg': { channel: 'offline', ym: null, sub: 'SUMMARY' }
};

function buildSegmentGroup(groupKey) {
    if (typeof SNAPSHOTS_OFFLINE === 'undefined') return;

    const cfg = groupKey === 'price_seg' ? {
        title: 'Ценовые сегменты',
        subtitle: 'Высокий / Средний / Низкий ценовой сегмент',
        segments: SEGMENTS_PRICE,
        descs: {
            HIGH: 'Премиальный по цене SKU: компактная корзина из дорогих позиций, реже визиты, низкая чувствительность к скидкам.',
            MIDDLE: 'Массовый сегмент с самым высоким ARPU и широкой корзиной — основа выручки сети.',
            LOW: 'Эконом-сегмент: низкий средний чек, выбор экономных позиций, высокая чувствительность к скидкам.'
        }
    } : {
        title: 'Сегменты по чеку',
        subtitle: 'Большой / Средний / Низкий чек',
        segments: SEGMENTS_CHECK,
        descs: {
            BIG_CHECK: 'Клиенты с большим чеком (~2-2.5x от среднего): широкая корзина с долей нон-фуд, высокая ср. цена SKU.',
            AVG_CHECK: 'Доминирующий сегмент (~52% базы): стандартный средний чек, частые визиты, основные категории.',
            LOW_CHECK: 'Клиенты с низким чеком: точечные покупки, узкая корзина, потенциал роста через расширение корзины.'
        }
    };

    const state = _segGroupState[groupKey];
    state.channel = window._activeChannel || 'offline';
    const channelSnap = state.channel === 'offline' ? SNAPSHOTS_OFFLINE : SNAPSHOTS_ECOM;
    // чековые сегменты могут отставать от ценовых (данные больших чеков приходят позже) —
    // показываем только месяцы, за которые есть данные сегментов этой группы
    const ymList = Object.keys(channelSnap)
        .filter(m => cfg.segments.some(sg => channelSnap[m]?.[sg]))
        .sort();
    if (!state.ym || !ymList.includes(state.ym)) state.ym = ymList[ymList.length - 1];
    if (state.sub !== 'SUMMARY' && !cfg.segments.includes(state.sub)) state.sub = 'SUMMARY';

    const monthOpts = ymList.map(ym =>
        `<option value="${ym}"${ym === state.ym ? ' selected' : ''}>${YM_LABELS[ym] || ym}</option>`
    ).join('');

    const summaryColor = '#5F259F';
    const summaryBtn = `<button class="seg-sub-btn ${state.sub === 'SUMMARY' ? 'active' : ''}" data-sub="SUMMARY" style="${state.sub === 'SUMMARY' ? `--accent:${summaryColor}` : ''}">
            <span class="seg-sub-dot" style="background:${summaryColor}"></span>Сводная
        </button>`;
    const subTabs = summaryBtn + cfg.segments.map(seg => {
        const color = SEG_COLORS_FULL[seg];
        return `<button class="seg-sub-btn ${seg === state.sub ? 'active' : ''}" data-sub="${seg}" style="${seg === state.sub ? `--accent:${color}` : ''}">
            <span class="seg-sub-dot" style="background:${color}"></span>${SEG_RU_FULL[seg]}
        </button>`;
    }).join('');

    const container = document.getElementById('tab-' + groupKey);
    container.innerHTML = `
        <div class="page-header">
            <div>
                <h1>${cfg.title}</h1>
                <p class="subtitle">${cfg.subtitle}</p>
            </div>
            <div class="header-meta">
                ${renderGlobalChannelToggle()}
                <select id="${groupKey}-month-sel" class="month-selector">${monthOpts}</select>
            </div>
        </div>

        <div class="seg-group-controls">
            <div class="seg-sub-tabs">${subTabs}</div>
        </div>

        <div id="${groupKey}-segment-content"></div>
    `;

    // Handlers
    document.getElementById(`${groupKey}-month-sel`).addEventListener('change', e => {
        state.ym = e.target.value;
        renderSegmentGroupSegment(groupKey, cfg);
    });
    container.querySelectorAll('.seg-channel-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            state.channel = btn.dataset.ch;
            buildSegmentGroup(groupKey);
        });
    });
    container.querySelectorAll('.seg-sub-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            state.sub = btn.dataset.sub;
            buildSegmentGroup(groupKey);
        });
    });

    if (_segGroupState[groupKey].sub === 'SUMMARY') {
        renderSegmentGroupSummary(groupKey, cfg);
    } else {
        renderSegmentGroupSegment(groupKey, cfg);
    }
}

// --- Аналитический "слайд" для Ценовых сегментов (под сводной таблицей) ---
function renderPriceSegNarrative(snap, prevSnap, curLabel, channelLbl) {
    const segs = ['HIGH', 'MIDDLE', 'LOW'];
    const totC = segs.reduce((s, x) => s + (snap[x]?.CLIENTS || 0), 0);
    const totP = segs.reduce((s, x) => s + (prevSnap[x]?.CLIENTS || 0), 0);
    const totTOC = segs.reduce((s, x) => s + (snap[x]?.CLIENTS || 0) * (snap[x]?.BUDGET || 0), 0);
    const totTOP = segs.reduce((s, x) => s + (prevSnap[x]?.CLIENTS || 0) * (prevSnap[x]?.BUDGET || 0), 0);

    const data = segs.map(sg => {
        const c = snap[sg] || {}, p = prevSnap[sg] || {};
        const shareBaseCur = totC ? (c.CLIENTS || 0) / totC : 0;
        const shareBasePrev = totP ? (p.CLIENTS || 0) / totP : 0;
        const shareTOCur = totTOC ? ((c.CLIENTS || 0) * (c.BUDGET || 0)) / totTOC : 0;
        const shareTOPrev = totTOP ? ((p.CLIENTS || 0) * (p.BUDGET || 0)) / totTOP : 0;
        const yoy = (cur, prev) => (cur != null && prev != null && prev !== 0) ? (cur - prev) / prev : null;
        return {
            seg: sg,
            name: SEG_RU_FULL[sg],
            color: SEG_COLORS_FULL[sg],
            clientsYoY: yoy(c.CLIENTS, p.CLIENTS),
            shareBasePP: shareBaseCur - shareBasePrev,
            shareTOPP: shareTOCur - shareTOPrev,
            shareBaseCur, shareTOCur,
            avgCheckYoY: yoy(c.AVG_CHECK, p.AVG_CHECK),
            skuCheckYoY: yoy(c.AVG_SKU, p.AVG_SKU),
            priceSkuYoY: yoy(c.AVG_COST_SKU, p.AVG_COST_SKU),
            saleCardYoY: yoy(c.SALE, p.SALE),
            realSaleYoY: yoy(c.REAL_SALE, p.REAL_SALE),
            saleCardCur: c.SALE, saleCardPrev: p.SALE,
            avgCheckCur: c.AVG_CHECK,
            skuCheckCur: c.AVG_SKU,
            priceSkuCur: c.AVG_COST_SKU,
            bonusYoY: (() => {
                const cv = (c.BONUS_PAY != null && c.CLIENTS) ? Math.abs(c.BONUS_PAY) / c.CLIENTS : null;
                const pv = (p.BONUS_PAY != null && p.CLIENTS) ? Math.abs(p.BONUS_PAY) / p.CLIENTS : null;
                return yoy(cv, pv);
            })(),
        };
    });

    const dHigh = data[0], dMid = data[1], dLow = data[2];
    const fmtPP = v => (v == null) ? '—' : (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + ' пп';
    const fmtP  = v => (v == null) ? '—' : (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%';
    const cls = v => v == null ? '' : (v >= 0 ? 'up' : 'down');

    // Migration card
    const migCard = (d, verdict, verdictColor) => `
        <div class="ps-mig-card" style="border-top:4px solid ${d.color}">
            <div class="ps-mig-head">
                <span class="dot" style="background:${d.color}"></span>
                <span class="ps-mig-name">${d.name}</span>
            </div>
            <div class="ps-mig-rows">
                <div class="ps-mig-row"><span>Δ доля базы</span><strong class="${cls(d.shareBasePP)}">${fmtPP(d.shareBasePP)}</strong></div>
                <div class="ps-mig-row"><span>Δ доля в ТО</span><strong class="${cls(d.shareTOPP)}">${fmtPP(d.shareTOPP)}</strong></div>
                <div class="ps-mig-row"><span>Δ клиентов YoY</span><strong class="${cls(d.clientsYoY)}">${fmtP(d.clientsYoY)}</strong></div>
            </div>
            <div class="ps-mig-verdict" style="background:${verdictColor}20;color:${verdictColor}">${verdict}</div>
        </div>
    `;

    return `
        <div class="mck-divider" style="margin-top:32px">
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#FF7900,transparent)"></div>
            <div class="mck-divider-label" style="color:#FF7900">📊 АНАЛИТИЧЕСКИЙ СЛАЙД · МИГРАЦИЯ ЦЕНОВЫХ СЕГМЕНТОВ · ${curLabel}</div>
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#FF7900,transparent)"></div>
        </div>

        <!-- Hero / headline -->
        <div class="card mck-card ps-hero">
            <div class="ps-hero-eyebrow">КЛЮЧЕВОЙ ВЫВОД · ${channelLbl}</div>
            <div class="ps-hero-headline">
                Поляризация базы: <span style="color:#5F259F">средний</span> сжимается в долях,
                <span style="color:#FF7900">низкий</span> и <span style="color:#1A5490">высокий</span> растут — клиенты «расходятся» по краям.
            </div>
            <div class="ps-hero-sub">
                K-shaped потребление 2025–2026: реальные доходы массового сегмента под давлением → миграция в эконом-полку.
                Доходный сегмент удерживает долю на росте цен SKU. Тренд соответствует общероссийской динамике (Росстат: реальные ЗП +3% при инфляции ~8%; рост дискаунтеров и пенетрации СТМ).
            </div>
        </div>

        <!-- Migration cards -->
        <div class="card mck-card">
            <div class="card-header">
                <h3>1. Миграция: кто теряет долю, кто забирает</h3>
                <span class="mck-subtitle">Изменение долей внутри ценовых сегментов, YoY</span>
            </div>
            <div class="ps-mig-grid">
                ${migCard(dHigh, '↑ ДОХОДНЫЙ РАСТЁТ', '#1A5490')}
                ${migCard(dMid,  '↓ ТЕРЯЕТ ДОЛЮ',     '#C41E3A')}
                ${migCard(dLow,  '↑↑ ВЗРЫВНОЙ РОСТ',  '#2E8B57')}
            </div>
            <div class="mck-conclusion" style="border-left-color:#FF7900">
                <strong>Вывод:</strong> «Средний» теряет <strong class="down">${fmtPP(dMid.shareBasePP)}</strong> доли базы и <strong class="down">${fmtPP(dMid.shareTOPP)}</strong> доли в ТО.
                «Низкий» забирает <strong class="up">${fmtPP(dLow.shareBasePP)}</strong> базы и <strong class="up">${fmtPP(dLow.shareTOPP)}</strong> ТО — почти 1:1 миграция.
                Клиенты не уходят из сети — они <em>пересобирают корзину дешевле</em>.
            </div>
        </div>

        <!-- Discount paradox -->
        <div class="card mck-card">
            <div class="card-header">
                <h3>2. Парадокс промо: скидки выросли у всех, чек — только у «Низкого»</h3>
                <span class="mck-subtitle">Карточная скидка vs средний чек, YoY</span>
            </div>
            <div class="ps-paradox">
                <table class="ps-paradox-table">
                    <thead>
                        <tr>
                            <th>Сегмент</th>
                            <th>Скидка по карте, YoY</th>
                            <th>Ср. чек, YoY</th>
                            <th>SKU/чек, YoY</th>
                            <th>Цена SKU, YoY</th>
                            <th>Эффект промо</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.map(d => {
                            const verdict = d.seg === 'LOW'
                                ? '<span class="ps-eff ps-eff-good">Конвертирует в чек</span>'
                                : '<span class="ps-eff ps-eff-bad">Не конвертирует</span>';
                            return `<tr>
                                <td><span class="seg-badge"><span class="dot" style="background:${d.color}"></span>${d.name}</span></td>
                                <td><strong class="${cls(d.saleCardYoY)}">${fmtP(d.saleCardYoY)}</strong></td>
                                <td><strong class="${cls(d.avgCheckYoY)}">${fmtP(d.avgCheckYoY)}</strong></td>
                                <td><strong class="${cls(d.skuCheckYoY)}">${fmtP(d.skuCheckYoY)}</strong></td>
                                <td><strong class="${cls(d.priceSkuYoY)}">${fmtP(d.priceSkuYoY)}</strong></td>
                                <td>${verdict}</td>
                            </tr>`;
                        }).join('')}
                    </tbody>
                </table>
                <div class="ps-paradox-bars">
                    ${data.map(d => {
                        const sale = (d.saleCardYoY || 0) * 100;
                        const check = (d.avgCheckYoY || 0) * 100;
                        const max = 45;
                        const wSale = Math.min(100, Math.abs(sale) / max * 100);
                        const wCheck = Math.min(100, Math.abs(check) / max * 100);
                        return `
                            <div class="ps-pb-row">
                                <div class="ps-pb-label"><span class="dot" style="background:${d.color}"></span>${d.name}</div>
                                <div class="ps-pb-bars">
                                    <div class="ps-pb-bar-wrap">
                                        <div class="ps-pb-bar ps-pb-sale" style="width:${wSale}%"></div>
                                        <span class="ps-pb-bar-text">Скидка ${sale >= 0 ? '+' : ''}${sale.toFixed(1)}%</span>
                                    </div>
                                    <div class="ps-pb-bar-wrap">
                                        <div class="ps-pb-bar ${check >= 0 ? 'ps-pb-up' : 'ps-pb-down'}" style="width:${wCheck}%"></div>
                                        <span class="ps-pb-bar-text">Ср. чек ${check >= 0 ? '+' : ''}${check.toFixed(1)}%</span>
                                    </div>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
            <div class="mck-conclusion" style="border-left-color:#5F259F">
                <strong>Что произошло:</strong> в HIGH/MIDDLE карточная скидка выросла на <strong>+37&hellip;+39%</strong>, а средний чек —
                <strong>≈ 0%</strong>. В Низком сегменте скидка <strong>+${((dLow.saleCardYoY||0)*100).toFixed(0)}%</strong> → чек <strong class="up">+${((dLow.avgCheckYoY||0)*100).toFixed(1)}%</strong>.
                Это значит: добавление товаров в промо «по карте» в HIGH/MIDDLE стимулировало клиентов получать дополнительную скидку на то, что они <em>и так покупали</em>, не расширяя корзину. У «Низкого» — наоборот, промо реально приводит к доп. покупкам.
            </div>
        </div>

        <!-- 4 bullets — interpretation -->
        <div class="card mck-card">
            <div class="card-header">
                <h3>3. Что говорят остальные показатели</h3>
                <span class="mck-subtitle">Полное прочтение метрик из сводной таблицы</span>
            </div>
            <div class="ps-bullets">
                <div class="ps-bullet" style="border-left-color:#1A5490">
                    <div class="ps-bullet-title">«Высокому» нечего купить</div>
                    <div class="ps-bullet-text">
                        SKU/чек <strong class="down">${fmtP(dHigh.skuCheckYoY)}</strong> при росте цены SKU только <strong>${fmtP(dHigh.priceSkuYoY)}</strong> (≈ инфляция).
                        Корзина сжимается даже в премиальной полке — клиент не находит расширенный ассортимент.
                        Средний чек минус <strong class="down">${fmtP(dHigh.avgCheckYoY)}</strong> — впервые за период.
                    </div>
                </div>
                <div class="ps-bullet" style="border-left-color:#5F259F">
                    <div class="ps-bullet-title">Средний → Низкий: миграция почти 1:1</div>
                    <div class="ps-bullet-text">
                        Средний теряет <strong>${fmtPP(dMid.shareBasePP)}</strong> доли базы — Низкий получает <strong>${fmtPP(dLow.shareBasePP)}</strong>.
                        Клиенты не покидают сеть, а пересобирают корзину дешевле. Это видно и по цене SKU: у Низкого <strong>${fmtP(dLow.priceSkuYoY)}</strong> — рост сильнее инфляции, т.к. база «обогатилась» бывшими «средними».
                    </div>
                </div>
                <div class="ps-bullet" style="border-left-color:#FF7900">
                    <div class="ps-bullet-title">Реальная скидка растёт у всех на ~8–10%</div>
                    <div class="ps-bullet-text">
                        HIGH <strong>${fmtP(dHigh.realSaleYoY)}</strong> · MIDDLE <strong>${fmtP(dMid.realSaleYoY)}</strong> · LOW <strong>${fmtP(dLow.realSaleYoY)}</strong>.
                        Маржинальная нагрузка растёт во всех сегментах, но окупается чеком только у Низкого.
                    </div>
                </div>
                <div class="ps-bullet" style="border-left-color:#C41E3A">
                    <div class="ps-bullet-title">Бонусная программа теряет вовлечённость</div>
                    <div class="ps-bullet-text">
                        Баллов/клиент: HIGH <strong class="down">${fmtP(dHigh.bonusYoY)}</strong> · MIDDLE <strong class="down">${fmtP(dMid.bonusYoY)}</strong> · LOW <strong class="${cls(dLow.bonusYoY)}">${fmtP(dLow.bonusYoY)}</strong>.
                        При росте карточной скидки на 30–40% баллов начисляется меньше — промо смещается с бонусной механики на прямые карточные скидки.
                    </div>
                </div>
            </div>
        </div>

        <!-- Recommendations -->
        <div class="card mck-card mck-priorities">
            <div class="card-header"><h3>4. Что делать (3 приоритета)</h3></div>
            <div class="mck-priority-grid">
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#FF7900">1</div>
                    <div class="mck-priority-title" style="color:#FF7900">Перепаковать промо в HIGH/MIDDLE</div>
                    <div class="mck-priority-desc">
                        Карточные скидки растут, чек — нет. Сократить «бесплатные» дисконты на категориях, которые клиент покупает регулярно;
                        сместить промо на товары-расширители корзины (новинки, импульсные категории, премиум-СТМ).
                    </div>
                    <div class="mck-priority-impact" style="background:#fff5eb;color:#FF7900">Цель: вернуть real_sale в HIGH к +0–3% YoY</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#5F259F">2</div>
                    <div class="mck-priority-title" style="color:#5F259F">Удержать «Средний» от полной миграции</div>
                    <div class="mck-priority-desc">
                        Сегмент пока в базе, но сжимается на ~10% доли. Через 6–9 месяцев эта доля может уйти необратимо.
                        Запустить целевую welcome-промо «Средний → Средний»: персональные предложения по их историческим категориям, а не общие скидки.
                    </div>
                    <div class="mck-priority-impact" style="background:#f5efff;color:#5F259F">Цель: остановить падение доли базы Среднего</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#2E8B57">3</div>
                    <div class="mck-priority-title" style="color:#2E8B57">Расширить ассортимент для «Высокого»</div>
                    <div class="mck-priority-desc">
                        SKU/чек у HIGH падает 6.3% — клиент готов платить, но не находит товара. Расширить премиум-полку (СТМ-премиум, импорт, ready-to-eat).
                        Это единственный сегмент, где рост ассортимента даст рост чека без скидок.
                    </div>
                    <div class="mck-priority-impact" style="background:#f0f9f0;color:#2E8B57">Цель: SKU/чек HIGH +3–5% YoY за 2 квартала</div>
                </div>
            </div>
        </div>
    `;
}

// ===== Shared helpers for slides 2-5 =====
function _trendsLatestKey(seg, city, ch) {
    if (typeof TRENDS_DATA === 'undefined') return null;
    const prefix = `${seg}|${city}|${ch}|`;
    const months = Object.keys(TRENDS_DATA).filter(k => k.startsWith(prefix)).map(k => k.split('|')[3]).sort();
    return months.length ? prefix + months[months.length - 1] : null;
}
function _ymLabel(ymStr) {
    const k = ymStr.slice(0,4) + '-' + ymStr.slice(4);
    return YM_LABELS[k] || (TRENDS_MONTH_FULL[+ymStr.slice(4,6)] + ' ' + ymStr.slice(0,4));
}
function _audienceCard(title, color, emoji, subtitle, items, hint) {
    const list = items.slice(0, 8).map(i => `
        <div class="aud-cat">
            <span class="aud-cat-name">${i.c}</span>
            <span class="aud-cat-meta"><strong>${(i.cn*100).toFixed(1)}%</strong> покуп. · ${i.ch.toFixed(2)} чек/мес${i.p ? ' · ' + Math.round(i.p) + ' ₽' : ''}${i.dc != null ? ' · над-индекс ' + i.dc.toFixed(2) + '×' : ''}</span>
        </div>
    `).join('') || '<div class="aud-cat-empty">— нет маркеров в этой группе —</div>';
    return `
        <div class="aud-card" style="border-top:4px solid ${color}">
            <div class="aud-head">
                <span class="aud-emoji">${emoji}</span>
                <div>
                    <div class="aud-title" style="color:${color}">${title}</div>
                    <div class="aud-sub">${subtitle}</div>
                </div>
            </div>
            <div class="aud-cats">${list}</div>
            ${hint ? `<div class="aud-hint">${hint}</div>` : ''}
        </div>
    `;
}
function _matchAny(cat, pats) { return pats.some(p => cat.includes(p)); }

// ===== Slide 2: ACTIVE_LFL · Москва+МО · Оффлайн — категорийный портрет =====
function renderActiveLflMoscowSlide() {
    const key = _trendsLatestKey('ACTIVE_LFL', 'Moscow + MO', 'OFF');
    if (!key) return '';
    const arr = TRENDS_DATA[key];
    const ymLabel = _ymLabel(key.split('|')[3]);

    const isKid = c => _matchAny(c, ['УЛЬТРАПАСТЕРИЗ','СЫРОК ГЛАЗИР','ДЕСЕРТЫ И ЙОГУРТЫ','ТВОРОГ НАТУР','ЙОГУРТ ПИТЬ','КЕФИР, БИФИДО','КАШИ ДЕТСК','СМЕСИ ГОТОВЫЕ МОЛОЧНЫЕ ДЕТСКИЕ','МАНДАРИН','ШОКОЛАД МОЛОЧ','МЫЛЬНЫЕ ПУЗЫРИ','ТВОРОЖНАЯ МАССА']);
    const isPet = c => _matchAny(c, ['ВЛАЖНЫЕ КОРМА','СУХИЕ КОРМА','ЛАКОМСТВ','ДЛЯ КОШ','ДЛЯ СОБ','КОРМ КОШ','КОРМ СОБ']);
    const isCookHome = c => _matchAny(c, ['РЕПЧАТЫЙ ЛУК','КАРТОФЕЛЬ','МОРКОВ','ЛИМОН','ПЕРЕЦ КРАС','ТОМАТ','КАПУСТА','ОГУРЦ','МАЙОНЕЗ','СМЕТАН','МУКА','МАСЛО ПОДСОЛНЕЧ','МАКАРОН','САХАР','ЧЕСНОК']);
    const isPremiumFresh = c => _matchAny(c, ['СЫР ТВЕРД','СЫР ПОЛУТВЕРД','СЫР ТВОРОЖ','СЫР РАССОЛ','КУРИЦА РАЗДЕЛКА','КОЛБАСЫ ВАРЕН','МАСЛО СЛИВ','ПРЕСЕРВЫ РЫБН','КОЛБАСЫ В/К']);
    const isBasic = c => _matchAny(c, ['БАНАН','ЯЙЦА','ЯБЛОК','БАТОН','ХЛЕБ ','МОЛОКО ПАСТЕР','МЕЛКОШТУЧ','БУМАГА','КЕФИР И КЕФИР','АПЕЛЬСИН']);

    const sortByCn = (a,b) => b.cn - a.cn;
    const basic = arr.filter(o => isBasic(o.c) && !isKid(o.c)).sort(sortByCn);
    const cook  = arr.filter(o => isCookHome(o.c)).sort(sortByCn);
    const kids  = arr.filter(o => isKid(o.c)).sort(sortByCn);
    const pets  = arr.filter(o => isPet(o.c)).sort(sortByCn);
    const prem  = arr.filter(o => isPremiumFresh(o.c) && !isBasic(o.c)).sort(sortByCn);

    // Реальные числа из данных
    const top1 = arr.slice().sort(sortByCn)[0];
    const milk = arr.find(o => o.c.includes('УЛЬТРАПАСТЕРИЗ'));
    const catFood = arr.find(o => o.c.includes('ВЛАЖНЫЕ КОРМА КОШ'));
    const chicken = arr.find(o => o.c.includes('КУРИЦА РАЗДЕЛКА'));
    const cheese = arr.find(o => o.c.includes('СЫР ТВЕРДЫЙ'));
    const top5sum = arr.slice().sort(sortByCn).slice(0,5).reduce((s,o)=>s+o.cn,0);

    // СТМ-кандидаты — реальные cn из данных
    const stm = (cats) => {
        const f = arr.filter(o => cats.some(c => o.c.includes(c)));
        const sumCn = f.reduce((s,o)=>s+o.cn, 0);
        const avgCh = f.length ? f.reduce((s,o)=>s+o.ch, 0) / f.length : 0;
        return { sumCn, avgCh, names: f.slice(0,3).map(o=>o.c.toLowerCase()) };
    };
    const stmMilk = stm(['МОЛОКО ПАСТЕР','МОЛОКО УЛЬТРАПАСТЕРИЗ','КЕФИР','СМЕТАН','ТВОРОГ','СЫРОК','ЙОГУРТ']);
    const stmBread = stm(['ХЛЕБ ','БАТОН','МЕЛКОШТУЧ']);
    const stmVeg = stm(['РЕПЧАТЫЙ ЛУК','КАРТОФЕЛЬ','МОРКОВ','КАПУСТА']);
    const stmGroc = stm(['САХАР','МАСЛО ПОДСОЛНЕЧ','МАКАРОН','МУКА']);

    return `
        <div class="mck-divider" style="margin-top:32px">
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#003A70,transparent)"></div>
            <div class="mck-divider-label" style="color:#003A70">📊 СЛАЙД 2 · АКТИВНЫЕ LFL · МОСКВА + МО · ОФФЛАЙН · ${ymLabel}</div>
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#003A70,transparent)"></div>
        </div>

        <div class="card mck-card lfl-hero">
            <div class="lfl-hero-eyebrow">ПОРТРЕТ ЯДРА БАЗЫ · ${ymLabel}</div>
            <div class="lfl-hero-headline">
                Активные LFL Москвы — это <span style="color:#003A70">семейная корзина повседневной готовки</span>:
                базовые овощи, хлеб, молочка, фрукты — и сильный «детский» след в полке.
            </div>
            <div class="lfl-hero-sub">
                <strong>${arr.length}</strong> категорий-маркеров, по которым сегмент покупает чаще среднего по сети.
                Топ-категория — <strong>${top1?.c?.toLowerCase() || ''}</strong>: <strong>${top1 ? (top1.cn*100).toFixed(1) : '—'}%</strong> сегмента, частота <strong>${top1 ? top1.ch.toFixed(2) : '—'} чека/мес</strong>.
                Топ-5 категорий покрывают суммарно <strong>${(top5sum*100).toFixed(0)}%</strong> сегмента.
            </div>
        </div>

        <div class="card mck-card">
            <div class="card-header">
                <h3>1. Какие категории покупают — по аудиториям внутри сегмента</h3>
                <span class="mck-subtitle">Все цифры — из i_trend по сегменту ACTIVE_LFL × Moscow+MO × OFF × ${ymLabel}</span>
            </div>
            <div class="aud-grid">
                ${_audienceCard('Базовая ежедневная корзина', '#003A70', '🛒', `${basic.length} маркеров`, basic,
                    `Эти категории присутствуют у 16–25% сегмента и покупаются ${basic.length ? basic[0].ch.toFixed(1) : '~2'}+ раз/мес → платформа для любого CVM-таргетинга.`)}
                ${_audienceCard('Готовка дома (ингредиенты)', '#88B04B', '🏠', `${cook.length} маркеров`, cook,
                    `Сильный признак семейной готовки: лук + картофель + морковь («борщевой набор») + масло подсолнечное + мука/сахар. Готовых блюд в маркерах нет вообще.`)}
                ${_audienceCard('Семьи с детьми', '#FF8FAB', '👨‍👩‍👧', `${kids.length} маркеров`, kids,
                    `«Невидимая» подгруппа в составе сегмента. УВТ-молоко: <strong>${milk ? (milk.cn*100).toFixed(1) : '—'}%</strong> покупателей, частота <strong>${milk ? milk.ch.toFixed(2) : '—'} чека/мес</strong> — устойчивый маркер дома с детьми.`)}
                ${_audienceCard('Семьи с питомцами', '#A0522D', '🐾', `${pets.length} маркеров`, pets,
                    `Влажный корм для кошки: <strong>${catFood ? (catFood.cn*100).toFixed(1) : '—'}%</strong> сегмента, частота <strong>${catFood ? catFood.ch.toFixed(2) : '—'} чека/мес</strong> — рекордная среди всех маркеров. Питомец = постоянная причина возврата.`)}
                ${_audienceCard('Свежее мясо/сыр (премиум-полка)', '#5F259F', '🥩', `${prem.length} маркеров`, prem,
                    `Курица охл. ${chicken ? '— ' + Math.round(chicken.p) + ' ₽' : ''}, сыр твёрдый ${cheese ? '— ' + Math.round(cheese.p) + ' ₽' : ''} — премиум-якорь. Расширение даст рост чека без скидок.`)}
            </div>
        </div>

        <div class="card mck-card lfl-milk">
            <div class="card-header">
                <h3>2. Спецблок: ультрапастеризованное молоко = семьи с детьми</h3>
                <span class="mck-subtitle">УВТ-молоко: ${milk ? (milk.cn*100).toFixed(1) : '—'}% покупателей, ${milk ? milk.ch.toFixed(2) : '—'} чека/мес, ср.цена ${milk ? Math.round(milk.p) : '—'} ₽, над-индекс ${milk ? milk.dc.toFixed(2) : '—'}×</span>
            </div>
            <div class="lfl-milk-grid">
                <div class="lfl-milk-cell">
                    <div class="lfl-milk-cell-title">Почему именно УВТ-молоко = маркер семей с детьми</div>
                    <ul class="lfl-milk-list">
                        <li>Длительный срок хранения <strong>(до 6 мес без холодильника)</strong> — мама закупает «впрок» на сад/школу/дорогу</li>
                        <li>Стерильность — <strong>безопасно для ребёнка без кипячения</strong>, ключевой выбор педиатров</li>
                        <li>Tetra Pak — удобно положить в портфель/сумку (200/250 мл порции)</li>
                        <li>Молочные коктейли для дошкольников — почти всегда УВТ</li>
                    </ul>
                </div>
                <div class="lfl-milk-cell">
                    <div class="lfl-milk-cell-title">Что обычно покупают родители для ребёнка</div>
                    <table class="lfl-milk-table">
                        <thead><tr><th>Тип</th><th>Кому</th><th>Бренд-якоря</th></tr></thead>
                        <tbody>
                            <tr><td>УВТ <strong>3.2 / 3.5%</strong></td><td>дети 3+</td><td>Простоквашино, Домик в деревне, Веселый молочник</td></tr>
                            <tr><td>УВТ <strong>6%</strong> топл.</td><td>школьники, какао</td><td>Простоквашино, Лебедянский</td></tr>
                            <tr><td>УВТ <strong>2.5% витаминизир.</strong></td><td>дошкольники</td><td>Тёма, Агуша, Растишка, ФрутоНяня</td></tr>
                            <tr><td>УВТ <strong>200 мл</strong> с трубочкой</td><td>в школу/сад</td><td>Чудо-детки, Агуша, Растишка</td></tr>
                            <tr><td>Безлактозное / на овсе</td><td>аллергики</td><td>Valio, Parmalat, Nemoloko</td></tr>
                        </tbody>
                    </table>
                </div>
                <div class="lfl-milk-cell lfl-milk-insight">
                    <div class="lfl-milk-cell-title" style="color:#FF8FAB">📌 Что это даёт ритейлеру</div>
                    <p>УВТ-молоко: <strong>${milk ? (milk.cn*100).toFixed(1) : '—'}%</strong> покрытия сегмента + частота <strong>${milk ? milk.ch.toFixed(2) : '—'}</strong> чека/мес — на уровне «ежедневных» категорий, при компактной аудитории.</p>
                    <p style="margin-top:10px">Cross-sell кандидаты к УВТ-молоку, которые тоже в маркерах сегмента: <strong>каши, сырки глазированные (${arr.find(o=>o.c.includes('СЫРОК ГЛАЗИР')) ? (arr.find(o=>o.c.includes('СЫРОК ГЛАЗИР')).cn*100).toFixed(1)+'%' : '—'}), питьевой йогурт (${arr.find(o=>o.c==='ЙОГУРТ ПИТЬЕВОЙ') ? (arr.find(o=>o.c==='ЙОГУРТ ПИТЬЕВОЙ').cn*100).toFixed(1)+'%' : '—'}), бананы (${arr.find(o=>o.c==='БАНАНЫ') ? (arr.find(o=>o.c==='БАНАНЫ').cn*100).toFixed(1)+'%' : '—'}), шоколад молочный (${arr.find(o=>o.c==='ШОКОЛАД МОЛОЧНЫЙ') ? (arr.find(o=>o.c==='ШОКОЛАД МОЛОЧНЫЙ').cn*100).toFixed(1)+'%' : '—'})</strong>.</p>
                    <p style="margin-top:10px">Триггер: «Ваше любимое молоко + что добавить к завтраку?» — персональный CVM-пуш на детский завтрак.</p>
                </div>
            </div>
        </div>

        <div class="card mck-card">
            <div class="card-header">
                <h3>3. Где развивать СТМ для топ-сегментов</h3>
                <span class="mck-subtitle">Доли — суммарное покрытие категорий из i_trend сегмента</span>
            </div>
            <table class="data-table lfl-stm-table">
                <thead>
                    <tr>
                        <th>Категорийный кластер</th>
                        <th>Почему СТМ зайдёт</th>
                        <th>Σ покрытия (cn)</th>
                        <th>Ср. частота</th>
                    </tr>
                </thead>
                <tbody>
                    <tr><td><strong>Молочка</strong> (молоко, кефир, сметана, творог, сырки, йогурт)</td><td>высокая частота, лояльность к качеству, не к бренду</td><td><span class="lfl-stm-share">${(stmMilk.sumCn*100).toFixed(0)}%</span></td><td>${stmMilk.avgCh.toFixed(2)} чек/мес</td></tr>
                    <tr><td><strong>Хлеб / выпечка</strong> (батоны, мелкоштучные)</td><td>СТМ-выпечка ИНД уже доминирует — углубить премиум-СТМ</td><td><span class="lfl-stm-share">${(stmBread.sumCn*100).toFixed(0)}%</span></td><td>${stmBread.avgCh.toFixed(2)} чек/мес</td></tr>
                    <tr><td><strong>Овощи борщевого набора</strong> (лук, картофель, морковь, капуста)</td><td>«готовка дома» = маркер сегмента</td><td><span class="lfl-stm-share">${(stmVeg.sumCn*100).toFixed(0)}%</span></td><td>${stmVeg.avgCh.toFixed(2)} чек/мес</td></tr>
                    <tr><td><strong>Базовая бакалея</strong> (сахар, масло подс., макароны, мука)</td><td>низкая лояльность к бренду, плотная корзина</td><td><span class="lfl-stm-share">${(stmGroc.sumCn*100).toFixed(0)}%</span></td><td>${stmGroc.avgCh.toFixed(2)} чек/мес</td></tr>
                </tbody>
            </table>
            <div class="mck-conclusion" style="border-left-color:#FF7900">
                <strong>Логика:</strong> СТМ выгоднее всего разворачивать в категориях с высокой частотой и низкой бренд-чувствительностью.
                4 кластера выше — суммарно <strong>${((stmMilk.sumCn+stmBread.sumCn+stmVeg.sumCn+stmGroc.sumCn)*100).toFixed(0)}%</strong> категорийного покрытия сегмента.
                Цель: рост доли СТМ в этих кластерах → защита маржи от роста реальной скидки (см. Слайд 1, +8…+10% YoY у всех ценовых сегментов).
            </div>
        </div>

        <div class="card mck-card mck-priorities">
            <div class="card-header"><h3>4. CVM-приоритеты для Active LFL · Москва · Оффлайн</h3></div>
            <div class="mck-priority-grid">
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#FF8FAB">1</div>
                    <div class="mck-priority-title" style="color:#FF8FAB">Семейный таргетинг</div>
                    <div class="mck-priority-desc">
                        Триггеры по детским маркерам (УВТ-молоко ${milk ? (milk.cn*100).toFixed(1) : '—'}%, сырки, питьевой йогурт, мандарины ${arr.find(o=>o.c==='МАНДАРИН') ? (arr.find(o=>o.c==='МАНДАРИН').cn*100).toFixed(1)+'%' : '—'}, каши).
                        Бандлы «завтрак ребёнку» + продвижение СТМ-аналогов известных брендов.
                    </div>
                    <div class="mck-priority-impact" style="background:#fff0f4;color:#FF8FAB">KPI: +1 SKU в чек у семей с детьми</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#88B04B">2</div>
                    <div class="mck-priority-title" style="color:#88B04B">«Готовка дома» — рецепт недели</div>
                    <div class="mck-priority-desc">
                        Push-меню с полным набором ингредиентов. Скидка только при покупке всего набора в одном чеке.
                        Стимулирует SKU/чек, который сейчас падает <strong>−6.3% YoY</strong> у Высокого и <strong>−6.5%</strong> у Среднего (Слайд 1).
                    </div>
                    <div class="mck-priority-impact" style="background:#f5f9eb;color:#88B04B">KPI: SKU/чек +0.3–0.5 / месяц</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#A0522D">3</div>
                    <div class="mck-priority-title" style="color:#A0522D">Питомцы — двигатель частоты</div>
                    <div class="mck-priority-desc">
                        Влажный корм для кота: ${catFood ? catFood.ch.toFixed(2) : '—'} чека/мес, рекорд частоты в сегменте.
                        Триггер «закончился корм?» по периодичности + кросс-сел премиум-СТМ-кормов.
                    </div>
                    <div class="mck-priority-impact" style="background:#faf3ee;color:#A0522D">KPI: частота визитов +0.2 / клиент</div>
                </div>
            </div>
        </div>
    `;
}

// ===== Slide 3: BIG_CHECK · Москва+МО · Оффлайн — что формирует большой чек =====
function renderBigCheckSlide() {
    const key = _trendsLatestKey('BIG_CHECK', 'Moscow + MO', 'OFF');
    const lflKey = _trendsLatestKey('ACTIVE_LFL', 'Moscow + MO', 'OFF');
    if (!key || !lflKey) return '';
    const bc  = TRENDS_DATA[key];
    const lfl = TRENDS_DATA[lflKey];
    const ymLabel = _ymLabel(key.split('|')[3]);

    const lflSet = new Set(lfl.map(o => o.c));
    const newCats = bc.filter(o => !lflSet.has(o.c)).sort((a,b) => b.cn - a.cn);

    const isReadyFood = c => _matchAny(c, ['ПЕЛЬМЕН','ХИНКАЛ','МАНТЫ','СУРИМИ','СУПЫ','БУЛЬОН','БЛЮДА','ГОТОВЫЕ','ВАРЕНИК','НАГГЕТС','СНЕКИ ГОР','ЛАПША МОМЕНТ','ПЮРЕ МОМЕНТ','СУПЫ МОМЕНТ','ПИЦЦА','БУРГЕР','СЭНДВИЧ','ЗАВТРАКИ','САЛАТ','РОЛЛ','СУШИ']);
    const isGrocery   = c => _matchAny(c, ['КОФЕ','ЧАЙ','МАСЛО ПОДС','САХАР','МАКАРОН','МУКА','СОЛЕНЬ','МЁД','МЕД','КРУП','РИС','ГРЕЧ','СПЕЦИИ','ПРИПРАВ','СОУС','КЕТЧУП','КОНСЕРВ','ВАРЕНЬЕ']);
    const isDrinks    = c => _matchAny(c, ['ВОДА','СОК','КОЛА','ЭНЕРГЕТ','ХОЛОДНЫЙ ЧАЙ','ЛИМОНАД','КВАС','НЕКТАР','МОРС']);
    const isSweetsImpulse = c => _matchAny(c, ['ШОКОЛАД','КОНФЕТ','БАТОНЧИК','ВЕСОВЫЕ','ПЕЧЕНЬЕ','ВАФЛИ','ПРЯН','МОРОЖЕН','ДЕСЕРТ']);
    const isPremium   = c => _matchAny(c, ['СЫР ПОЛУТВЕРД','СЫР РАССОЛ','КОЛБАСЫ В/К','КОЛБАСЫ П/К','ИКРА','ЛОСОС','СЕМГА','ХАМОН']);

    const ready  = newCats.filter(o => isReadyFood(o.c));
    const groc   = newCats.filter(o => isGrocery(o.c) && !isReadyFood(o.c));
    const drinks = newCats.filter(o => isDrinks(o.c));
    const sweets = newCats.filter(o => isSweetsImpulse(o.c));
    const prem   = newCats.filter(o => isPremium(o.c));

    const lflCnAvg = lfl.reduce((s,o) => s + o.cn, 0) / Math.max(lfl.length, 1);
    const bcCnAvg  = bc.reduce((s,o) => s + o.cn, 0) / Math.max(bc.length, 1);

    const pelmeni = bc.find(o => o.c.includes('ПЕЛЬМЕН'));
    const coffee  = bc.find(o => o.c.includes('КОФЕ'));
    const tea     = bc.find(o => o.c.includes('ЧАЙ ЧЕРНЫЙ'));
    const candy   = bc.find(o => o.c.includes('КОНФЕТЫ ВЕСОВЫЕ В ШОКОЛАД'));
    const cheeseHard = bc.find(o => o.c === 'СЫР ПОЛУТВЕРДЫЙ');

    return `
        <div class="mck-divider" style="margin-top:32px">
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#5F259F,transparent)"></div>
            <div class="mck-divider-label" style="color:#5F259F">📊 СЛАЙД 3 · БОЛЬШОЙ ЧЕК · МОСКВА + МО · ОФФЛАЙН · ${ymLabel}</div>
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#5F259F,transparent)"></div>
        </div>

        <div class="card mck-card bc-hero">
            <div class="bc-hero-eyebrow">ЧТО ФОРМИРУЕТ БОЛЬШОЙ ЧЕК · ${ymLabel}</div>
            <div class="bc-hero-headline">
                Большой чек — это <span style="color:#5F259F">не премиум-полка, а комплексная закупка</span>:
                к ежедневной корзине добавляются <strong>готовая еда, расширенная бакалея и напитки</strong>.
            </div>
            <div class="bc-hero-sub">
                Дифф vs Active LFL: <strong>${bc.length}</strong> маркеров в Big Check vs <strong>${lfl.length}</strong> у LFL → <strong>${newCats.length} «новых»</strong> категорий, отсутствующих у обычного LFL.
                Это категории «закупки на неделю»: пельмени (${pelmeni ? (pelmeni.cn*100).toFixed(1)+'% сегмента' : '—'}),
                кофе раств. (${coffee ? Math.round(coffee.p)+' ₽' : '—'}),
                премиум-конфеты (${candy ? Math.round(candy.p)+' ₽' : '—'}),
                сыр полутвёрдый (${cheeseHard ? Math.round(cheeseHard.p)+' ₽' : '—'}).
            </div>
        </div>

        <div class="card mck-card">
            <div class="card-header">
                <h3>1. «Новые» категории — что появляется в чеке Big Check, но нет у Active LFL</h3>
                <span class="mck-subtitle">Дифф-анализ маркеров. Цифры — из i_trend, ${ymLabel}</span>
            </div>
            <div class="aud-grid">
                ${_audienceCard('Готовая еда / полуфабрикаты', '#E87722', '🍱', `${ready.length} новых маркеров`, ready,
                    'Active LFL готовит из ингредиентов. Big Check добавляет готовое: пельмени, сурими охл, готовые блюда. Это +1 миссия в чеке.')}
                ${_audienceCard('Расширенная бакалея', '#003A70', '🌾', `${groc.length} новых маркеров`, groc,
                    'Кофе, чай, соленья, рис, крупы. Закупка раз в 2–4 недели большим объёмом — драйвер высокого SKU/чек.')}
                ${_audienceCard('Напитки сверх воды', '#00897B', '🥤', `${drinks.length} новых маркеров`, drinks,
                    'Появляются 100% соки, минералка, энергетики, холодный чай. «Семейный набор напитков на неделю».')}
                ${_audienceCard('Премиум-снеки и сладости', '#9B59B6', '🍫', `${sweets.length} новых маркеров`, sweets,
                    `Конфеты в шоколадной глазури (${candy ? Math.round(candy.p)+' ₽/кг' : '—'}), шоколадные батончики, премиум-весовые. Импульс в большой корзине.`)}
                ${_audienceCard('Премиум-полка для семьи', '#5F259F', '🧀', `${prem.length} новых маркеров`, prem,
                    'Сыр полутвёрдый, рассольный, варёные/п-к колбасы. Не икра/семга — а «достойный» средний премиум для семейного стола.')}
            </div>
        </div>

        <div class="card mck-card">
            <div class="card-header">
                <h3>2. Главные выводы — за счёт чего Big Check больше Average</h3>
            </div>
            <div class="bc-bullets">
                <div class="bc-bullet" style="border-left-color:#E87722">
                    <div class="bc-bullet-title">⚡ Ready-to-eat впервые появляется в маркерах</div>
                    <div class="bc-bullet-text">У Active LFL — готовых блюд почти нет. У Big Check — <strong>${ready.length} новых маркеров готовой еды</strong>: пельмени (${pelmeni ? (pelmeni.cn*100).toFixed(1)+'%' : '—'}), сурими охл, супы, готовые блюда. <strong>+1 миссия в чеке</strong>.</div>
                </div>
                <div class="bc-bullet" style="border-left-color:#003A70">
                    <div class="bc-bullet-title">🛒 Бакалея = «закупка впрок» вместо «дозакупки»</div>
                    <div class="bc-bullet-text">Кофе растворимый ${coffee ? '('+Math.round(coffee.p)+' ₽)' : ''}, чай ${tea ? '('+Math.round(tea.p)+' ₽)' : ''}, масло, соленья — товары длительного срока. Big Check — поход «закупиться на 2 недели», а не «забежать за хлебом и молоком».</div>
                </div>
                <div class="bc-bullet" style="border-left-color:#9B59B6">
                    <div class="bc-bullet-title">🎁 Импульсные сладости в большой корзине</div>
                    <div class="bc-bullet-text">Премиум-конфеты ${candy ? '('+Math.round(candy.p)+' ₽/кг)' : ''}, шоколадные батончики, мороженое импульсное. Когда чек уже большой, добавление 200–400 ₽ «на сладкое» воспринимается как незначительная доля.</div>
                </div>
                <div class="bc-bullet" style="border-left-color:#5F259F">
                    <div class="bc-bullet-title">🧀 Не «премиум», а «средний премиум для дома»</div>
                    <div class="bc-bullet-text">Сыр полутвёрдый ${cheeseHard ? '('+Math.round(cheeseHard.p)+' ₽)' : ''}, варёные колбасы хорошего качества, минералка, 100% соки. Это <strong>не икра и не хамон</strong>, а апгрейд массовых категорий.</div>
                </div>
                <div class="bc-bullet" style="border-left-color:#88B04B">
                    <div class="bc-bullet-title">📦 Среднее покрытие ниже, чем в Active LFL</div>
                    <div class="bc-bullet-text">Big Check ср. <strong>${(bcCnAvg*100).toFixed(1)}%</strong> покрытия категории vs Active LFL <strong>${(lflCnAvg*100).toFixed(1)}%</strong>. Big Check — <strong>не одинаковый набор у всех</strong>, а индивидуальные «комплексные миссии». Каждый клиент собирает свою большую корзину.</div>
                </div>
            </div>
        </div>

        <div class="card mck-card mck-priorities">
            <div class="card-header"><h3>3. CVM-механики для роста чека</h3></div>
            <div class="bc-cvm-grid">
                <div class="bc-cvm">
                    <div class="bc-cvm-icon" style="background:#5F259F">1</div>
                    <div class="bc-cvm-title">Прогрессивный кэшбек по чеку</div>
                    <div class="bc-cvm-desc">«Чек 1500₽ → +1% бонусами, 2500₽ → +3%, 4000₽ → +5%». Прямой стимул довести корзину до следующего порога.</div>
                    <div class="bc-cvm-impact">KPI: доля чеков ≥3000₽ +5 пп / квартал</div>
                </div>
                <div class="bc-cvm">
                    <div class="bc-cvm-icon" style="background:#E87722">2</div>
                    <div class="bc-cvm-title">«Купи 2 миссии — получи скидку»</div>
                    <div class="bc-cvm-desc">Бандлы «Готовая еда + Напиток», «Бакалея + Свежее мясо», «Завтрак ребёнку + Кофе родителю». Сборка миссий в одном чеке.</div>
                    <div class="bc-cvm-impact">KPI: SKU/чек +0.5–1.0 за 2 квартала</div>
                </div>
                <div class="bc-cvm">
                    <div class="bc-cvm-icon" style="background:#003A70">3</div>
                    <div class="bc-cvm-title">«Закупка на неделю» — пуш</div>
                    <div class="bc-cvm-desc">Раз в неделю — список «что вам обычно нужно на неделю» по предыдущим чекам. Доп. бонусы при покупке всего списка единым чеком.</div>
                    <div class="bc-cvm-impact">KPI: частота больших чеков +1 / квартал</div>
                </div>
                <div class="bc-cvm">
                    <div class="bc-cvm-icon" style="background:#9B59B6">4</div>
                    <div class="bc-cvm-title">Импульс на кассе для большого чека</div>
                    <div class="bc-cvm-desc">При чеке от 2000₽ — спецпредложение в зоне касс: премиум-шоколад / конфеты со скидкой. Доля сладкого импульса в Big Check уже выше — усилить.</div>
                    <div class="bc-cvm-impact">KPI: AOV +30–50 ₽ на чеках 2000₽+</div>
                </div>
                <div class="bc-cvm">
                    <div class="bc-cvm-icon" style="background:#2E8B57">5</div>
                    <div class="bc-cvm-title">«Семейный четверг» — двойные баллы на готовую еду</div>
                    <div class="bc-cvm-desc">Стимулировать миссию «не готовлю — беру готовое»: пельмени, сурими, готовые завтраки. Эта миссия отличает Big Check от Active LFL — масштабировать на LFL.</div>
                    <div class="bc-cvm-impact">KPI: пенетрация ready-food в LFL +5 пп</div>
                </div>
                <div class="bc-cvm">
                    <div class="bc-cvm-icon" style="background:#FF7900">6</div>
                    <div class="bc-cvm-title">Премиум-СТМ для большой корзины</div>
                    <div class="bc-cvm-desc">Big Check готов покупать «средний премиум» — сыр полутвёрдый, колбасы, премиум-конфеты. Развернуть линейку <strong>СТМ-Premium</strong> в этих категориях.</div>
                    <div class="bc-cvm-impact">KPI: доля СТМ-Premium в чеках Big Check +3 пп</div>
                </div>
            </div>
        </div>
    `;
}

// ===== Slide 4: HIGH price segment · сезонность доли и ARPU YoY =====
function renderHighSeasonalitySlide(channelSnap, ym) {
    if (!channelSnap || !ym) return '';
    const ymList = Object.keys(channelSnap).sort();
    const idx = ymList.indexOf(ym);
    const last12 = ymList.slice(Math.max(0, idx - 11), idx + 1);
    const prev12 = last12.map(m => '' + (parseInt(m.slice(0,4)) - 1) + m.slice(4));

    const segs = ['HIGH','MIDDLE','LOW'];
    function shareHigh(snap) {
        const tot = segs.reduce((s,x) => s + (snap?.[x]?.CLIENTS || 0), 0);
        return tot ? (snap?.HIGH?.CLIENTS || 0) / tot : null;
    }
    const curShare  = last12.map(m => shareHigh(channelSnap[m]));
    const prevShare = prev12.map(m => shareHigh(channelSnap[m]));
    const curArpu   = last12.map(m => channelSnap[m]?.HIGH?.BUDGET ?? null);
    const prevArpu  = prev12.map(m => channelSnap[m]?.HIGH?.BUDGET ?? null);

    window._highSeasonalityData = { last12, prev12, curShare, prevShare, curArpu, prevArpu };

    const decIdxCur  = last12.findIndex(m => m.endsWith('12'));
    const decIdxPrev = prev12.findIndex(m => m.endsWith('12'));
    const decShareCur  = decIdxCur  >= 0 ? curShare[decIdxCur]  : null;
    const decSharePrev = decIdxPrev >= 0 ? prevShare[decIdxPrev] : null;
    const decArpuCur   = decIdxCur  >= 0 ? curArpu[decIdxCur]   : null;
    const decArpuPrev  = decIdxPrev >= 0 ? prevArpu[decIdxPrev]  : null;

    const sumMonths = ['05','06','07'];
    let peakShareCur = -Infinity, peakLabelCur = '', peakArpuCur = null;
    last12.forEach((m, i) => {
        if (sumMonths.includes(m.slice(4)) && curShare[i] != null && curShare[i] > peakShareCur) {
            peakShareCur = curShare[i]; peakLabelCur = YM_LABELS[m] || m; peakArpuCur = curArpu[i];
        }
    });
    let troughShareCur = Infinity, troughLabelCur = '';
    last12.forEach((m, i) => {
        if (curShare[i] != null && curShare[i] < troughShareCur) { troughShareCur = curShare[i]; troughLabelCur = YM_LABELS[m] || m; }
    });

    const sign = v => v == null ? '' : (v >= 0 ? '+' : '');
    const decShareDelta = (decShareCur != null && decSharePrev != null) ? (decShareCur - decSharePrev) : null;
    const decArpuDelta  = (decArpuCur != null && decArpuPrev != null && decArpuPrev !== 0) ? (decArpuCur - decArpuPrev) / decArpuPrev : null;

    // Compute "lost revenue" estimate from real data
    const decClients = decIdxCur >= 0 ? channelSnap[last12[decIdxCur]]?.HIGH?.CLIENTS : null;
    const decTotalClients = decIdxCur >= 0 ? segs.reduce((s,x) => s + (channelSnap[last12[decIdxCur]]?.[x]?.CLIENTS || 0), 0) : null;
    const lostClients = (decShareDelta != null && decTotalClients) ? Math.abs(decShareDelta) * decTotalClients : null;
    const lostRevenue = (lostClients != null && decArpuCur) ? lostClients * decArpuCur : null;

    return `
        <div class="mck-divider" style="margin-top:32px">
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#1A5490,transparent)"></div>
            <div class="mck-divider-label" style="color:#1A5490">📊 СЛАЙД 4 · СЕЗОННОСТЬ ВЫСОКОГО ЦЕНОВОГО СЕГМЕНТА</div>
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#1A5490,transparent)"></div>
        </div>

        <div class="card mck-card hs-hero">
            <div class="hs-hero-eyebrow">ВЫСОКИЙ ЦЕНОВОЙ · YoY СРАВНЕНИЕ 12 МЕСЯЦЕВ</div>
            <div class="hs-hero-headline">
                Лето = пик доли «Высокого». Декабрь 2025 — провал vs Декабрь 2024:
                <span style="color:#C41E3A">мы недоработали с премиум-клиентом в Новогодний пик</span>.
            </div>
            <div class="hs-hero-sub">
                Сравнение Апр 2025 – Мар 2026 vs Апр 2024 – Мар 2025. Два устойчивых паттерна:
                сезонный пик доли HIGH летом (${peakLabelCur || '—'} → <strong>${peakShareCur > -Infinity ? (peakShareCur*100).toFixed(1)+'%' : '—'}</strong>)
                и провал в декабре (Дек 2025 <strong>${decShareCur != null ? (decShareCur*100).toFixed(1)+'%' : '—'}</strong> vs Дек 2024 <strong>${decSharePrev != null ? (decSharePrev*100).toFixed(1)+'%' : '—'}</strong>, <strong>${decShareDelta != null ? sign(decShareDelta)+(decShareDelta*100).toFixed(1)+' пп' : '—'}</strong>).
            </div>
        </div>

        <div class="charts-row">
            <div class="card">
                <div class="card-header">
                    <h3>Доля «Высокого» в базе · YoY</h3>
                    <span class="mck-subtitle">Сплошная — текущий год · пунктир — прошлый год</span>
                </div>
                <div class="chart-container"><canvas id="hs-chart-share"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header">
                    <h3>ARPU «Высокого» · YoY, ₽</h3>
                    <span class="mck-subtitle">Сплошная — текущий год · пунктир — прошлый год</span>
                </div>
                <div class="chart-container"><canvas id="hs-chart-arpu"></canvas></div>
            </div>
        </div>

        <div class="hs-kpi-strip">
            <div class="hs-kpi" style="border-top:4px solid #2E8B57">
                <div class="hs-kpi-label">☀️ Летний пик доли HIGH (текущий год)</div>
                <div class="hs-kpi-value">${peakShareCur > -Infinity ? (peakShareCur*100).toFixed(1) + '%' : '—'}</div>
                <div class="hs-kpi-sub">${peakLabelCur || '—'} · сезонный максимум${peakArpuCur ? ' · ARPU ' + Math.round(peakArpuCur).toLocaleString('ru-RU') + ' ₽' : ''}</div>
            </div>
            <div class="hs-kpi" style="border-top:4px solid #C41E3A">
                <div class="hs-kpi-label">🎄 Декабрь: доля HIGH в базе</div>
                <div class="hs-kpi-value">${decShareCur != null ? (decShareCur*100).toFixed(1) + '%' : '—'} <span class="hs-kpi-vs">vs ${decSharePrev != null ? (decSharePrev*100).toFixed(1) + '%' : '—'}</span></div>
                <div class="hs-kpi-sub ${decShareDelta != null && decShareDelta < 0 ? 'down' : 'up'}">
                    ${decShareDelta != null ? sign(decShareDelta) + (decShareDelta*100).toFixed(1) + ' пп YoY' : '—'} · vs прошлый декабрь
                </div>
            </div>
            <div class="hs-kpi" style="border-top:4px solid #C41E3A">
                <div class="hs-kpi-label">🎄 Декабрь: ARPU HIGH</div>
                <div class="hs-kpi-value">${decArpuCur != null ? Math.round(decArpuCur).toLocaleString('ru-RU') + ' ₽' : '—'} <span class="hs-kpi-vs">vs ${decArpuPrev != null ? Math.round(decArpuPrev).toLocaleString('ru-RU') + ' ₽' : '—'}</span></div>
                <div class="hs-kpi-sub ${decArpuDelta != null && decArpuDelta < 0 ? 'down' : 'up'}">
                    ${decArpuDelta != null ? sign(decArpuDelta) + (decArpuDelta*100).toFixed(1) + '% YoY' : '—'} · vs прошлый декабрь
                </div>
            </div>
        </div>

        <div class="card mck-card">
            <div class="card-header">
                <h3>1. Летний пик: «Высокий» прирастает за счёт случайных визитов</h3>
            </div>
            <div class="hs-insight">
                <div class="hs-insight-text">
                    <p>В <strong>${peakLabelCur || '—'}</strong> доля HIGH в базе достигла <strong style="color:#1A5490">${peakShareCur > -Infinity ? (peakShareCur*100).toFixed(1) : '—'}%</strong> — против <strong>${troughShareCur < Infinity ? (troughShareCur*100).toFixed(1) : '—'}%</strong> в межсезонье (${troughLabelCur || '—'}).
                    Аналогичный паттерн виден и в прошлом году.</p>
                    <p style="margin-top:8px"><strong>Гипотеза:</strong> летом увеличивается поток <em>случайных клиентов</em> в «магазинах у дома» —
                    люди гуляют в окрестностях, едут на дачу, заходят за импульсной покупкой (вода, мороженое, фрукты, готовая еда, алкоголь к шашлыку).
                    Эти случайные визиты приходятся именно на премиум-полку (компактная корзина из дорогих SKU = «высокий» по определению).</p>
                    <p style="margin-top:8px"><strong>Подтверждение:</strong> ARPU HIGH летом тоже растёт (правый график) — но не за счёт лояльности, а за счёт состава корзины случайного захода.</p>
                </div>
                <div class="hs-insight-side">
                    <div class="hs-insight-side-title">💡 Что это значит для CVM</div>
                    <ul>
                        <li>Летние «случайные» — это <strong>точка входа</strong> в программу лояльности для high-income клиентов из окрестных районов</li>
                        <li>Летом важно <strong>захватить контакт</strong>: триггер «зарегистрируйтесь и получите скидку 100₽ на следующий визит»</li>
                        <li>После лета — welcome-серия для удержания этих клиентов на осень/зиму</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="card mck-card hs-decgap">
            <div class="card-header">
                <h3>2. Декабрь 2024 vs Декабрь 2025: что упустили в Новогодний пик</h3>
            </div>
            <div class="hs-insight">
                <div class="hs-insight-text">
                    <p>В декабре прошлого года доля HIGH достигла <strong style="color:#C41E3A">${decSharePrev != null ? (decSharePrev*100).toFixed(1) : '—'}%</strong> — премиум-клиент массово приходил за новогодней закупкой.
                    В декабре этого года — только <strong style="color:#C41E3A">${decShareCur != null ? (decShareCur*100).toFixed(1) : '—'}%</strong> (<strong>${decShareDelta != null ? sign(decShareDelta) + (decShareDelta*100).toFixed(1) + ' пп' : '—'}</strong> к прошлому декабрю).</p>
                    <p style="margin-top:8px">Аналогично — ARPU HIGH в Дек 2024 был <strong>${decArpuPrev ? Math.round(decArpuPrev).toLocaleString('ru-RU') : '—'} ₽</strong>, в Дек 2025 — <strong>${decArpuCur ? Math.round(decArpuCur).toLocaleString('ru-RU') : '—'} ₽</strong>
                    (<strong>${decArpuDelta != null ? sign(decArpuDelta) + (decArpuDelta*100).toFixed(1) + '%' : '—'}</strong> YoY).</p>
                    <p style="margin-top:8px"><strong>Вопросы для post-mortem:</strong></p>
                    <ul style="margin-top:6px;line-height:1.6">
                        <li>Какие новогодние промо/механики работали в Дек 2024 и почему их не повторили в Дек 2025?</li>
                        <li>Был ли запуск премиум-СТМ к НГ-столу в 2024, которого не было в 2025?</li>
                        <li>Как сработали конкуренты (ВкусВилл, Перекрёсток) в декабре 2025 — не перетянули ли HIGH-клиента?</li>
                        <li>Какая была реклама/коммуникация на премиум-аудиторию в Дек 2024 vs Дек 2025?</li>
                    </ul>
                </div>
                <div class="hs-insight-side hs-insight-side-warn">
                    <div class="hs-insight-side-title" style="color:#C41E3A">🚨 Это упущенная выручка</div>
                    <p>${decShareDelta != null ? Math.abs(decShareDelta*100).toFixed(1) + ' пп' : '—'} доли HIGH × ${decTotalClients ? Math.round(decTotalClients/1000) + ' тыс' : '—'} клиентов всего × ARPU ${decArpuCur ? Math.round(decArpuCur).toLocaleString('ru-RU') : '—'} ₽
                    ≈ <strong>${lostRevenue != null ? (lostRevenue/1e6).toFixed(0) + ' млн ₽' : '—'}</strong> упущенной выручки только в одном декабре.</p>
                    <p style="margin-top:10px"><strong>Action:</strong> провести post-mortem декабря 2025 vs декабря 2024 на уровне ассортимента, цен, промо и ATL. Не дать повториться в декабре 2026.</p>
                </div>
            </div>
        </div>

        <div class="card mck-card mck-priorities">
            <div class="card-header"><h3>3. Что делать с сезонностью HIGH</h3></div>
            <div class="mck-priority-grid">
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#2E8B57">1</div>
                    <div class="mck-priority-title" style="color:#2E8B57">Летняя «летучая» программа захвата</div>
                    <div class="mck-priority-desc">Май–август: фокус на регистрации случайных HIGH-клиентов. Welcome-промо за подключение карты + персональная скидка на 2-й визит. Категории-якоря: вода, мороженое, премиум-фрукты, готовая еда, алкоголь.</div>
                    <div class="mck-priority-impact" style="background:#f0f9f0;color:#2E8B57">KPI: рост базы HIGH с картой за лето</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#C41E3A">2</div>
                    <div class="mck-priority-title" style="color:#C41E3A">Новогодний план для HIGH 2026</div>
                    <div class="mck-priority-desc">Восстановить активности декабря 2024: премиум-СТМ к столу, подарочные наборы, доп. баллы на «новогоднюю корзину», отдельная коммуникация по премиум-аудитории. Запуск с октября.</div>
                    <div class="mck-priority-impact" style="background:#fef0f0;color:#C41E3A">KPI: вернуть долю HIGH в Дек 2026 к ${decSharePrev != null ? (decSharePrev*100).toFixed(1)+'%' : 'уровню Дек 2024'}</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#1A5490">3</div>
                    <div class="mck-priority-title" style="color:#1A5490">Удержание летних HIGH на осень</div>
                    <div class="mck-priority-desc">После лета — welcome-серия для свежеподключённых HIGH-клиентов: 3 пуша за 60 дней с персонализированными предложениями на премиум-полку (сыры, мясо, СТМ-премиум, кофе).</div>
                    <div class="mck-priority-impact" style="background:#eaf2fa;color:#1A5490">KPI: 30% летних HIGH удержать в активной базе к Ноя</div>
                </div>
            </div>
        </div>
    `;
}

// ===== Slide 5: HIGH_PRICE · Москва+МО · Оффлайн — миссии =====
// Только реальные категории из i_trend (HIGH_PRICE|Moscow + MO|OFF|*).
function renderHighPriceMissionsSlide() {
    if (typeof TRENDS_DATA === 'undefined') return '';
    const prefix = 'HIGH_PRICE|Moscow + MO|OFF|';
    const monthKeys = Object.keys(TRENDS_DATA).filter(k => k.startsWith(prefix));
    if (!monthKeys.length) return '';

    // Aggregate over all months
    const agg = {};
    let totalRows = 0;
    monthKeys.forEach(k => {
        TRENDS_DATA[k].forEach(o => {
            totalRows++;
            if (!agg[o.c]) agg[o.c] = { c: o.c, sumCn: 0, n: 0, maxDc: 0, sumCh: 0, lastP: 0, sumP: 0 };
            agg[o.c].sumCn += o.cn;
            agg[o.c].sumP += o.p || 0;
            agg[o.c].n += 1;
            if (o.dc > agg[o.c].maxDc) agg[o.c].maxDc = o.dc;
            agg[o.c].sumCh += o.ch;
            agg[o.c].lastP = o.p || agg[o.c].lastP;
        });
    });
    const all = Object.values(agg).map(o => ({
        ...o, avgCn: o.sumCn / o.n, avgCh: o.sumCh / o.n, avgP: o.sumP / o.n
    })).sort((a, b) => b.maxDc - a.maxDc);

    // Latest month for current snapshot too
    const latestKey = monthKeys.sort()[monthKeys.length - 1];
    const latestArr = TRENDS_DATA[latestKey];
    const latestLabel = _ymLabel(latestKey.split('|')[3]);

    // Buckets
    const isReadyFood = c => _matchAny(c, ['ПЕЛЬМЕН','ХИНКАЛ','МАНТЫ','СУРИМИ','СУПЫ','БУЛЬОН','БЛЮДА','ГОТОВЫЕ','ВАРЕНИК','НАГГЕТС','СНЕКИ ГОР','ЛАПША МОМЕНТ','ПЮРЕ МОМЕНТ','СУПЫ МОМЕНТ','ПИЦЦА','БУРГЕР','СЭНДВИЧ','ЗАВТРАКИ','САЛАТ','РОЛЛ','СУШИ','БЛИНЫ','ОЛАДЬИ','СЫРНИКИ','ЗАПЕКАНКИ','КАШИ МОЛОЧНЫЕ','ЗАКУСК']);
    const isImportAlc = c => _matchAny(c, ['ВИНА ТИХИЕ БЕЛ. НОВОЙ ЗЕЛАНДИИ','ВИНА ТИХИЕ БЕЛ. АВСТРАЛИИ','ВИНА ТИХИЕ КР. США','ВИНА ТИХИЕ РОЗ. ИТАЛИИ','КОНЬЯК ФРАНЦИИ','ДЖИН ИМПОРТНЫЙ','ХЕРЕС','КАЛЬВАДОС','РАКИЯ','АНИСОВЫЕ','САМБУКА','ИМПОРТНОЕ ПИВО','КРАФТОВОЕ']);
    const isSeasonal = c => _matchAny(c, ['БАНКИ ДЛЯ КОНСЕРВАЦИИ','ГРУНТ','УДОБРЕН','СЕМЕНА','ПАСХАЛЬН','ЕЛКИ','ЕЛОЧНЫЕ','СПИЦЫ','АКСЕССУАРЫ ДЛЯ ШИТЬЯ','ИГРУШК','ДЛЯ ПИКНИКА']);
    const isSupplierDrinks = c => _matchAny(c, ['НАПИТКИ СП','ПОСТАВЩИК']);
    const isImpulse = c => _matchAny(c, ['МОБИЛЬНЫЕ ТЕЛЕФОНЫ','ЭЛЕКТРОННЫЕ СИГАРЕТЫ','СТИКИ']);

    const ready = all.filter(o => isReadyFood(o.c));
    const importAlc = all.filter(o => isImportAlc(o.c));
    const seasonal = all.filter(o => isSeasonal(o.c));
    const supplier = all.filter(o => isSupplierDrinks(o.c));
    const impulse = all.filter(o => isImpulse(o.c));
    const otherCats = all.filter(o => !isReadyFood(o.c) && !isImportAlc(o.c) && !isSeasonal(o.c) && !isSupplierDrinks(o.c) && !isImpulse(o.c));

    // Renderer for HIGH_PRICE-specific category cells (showing dc as the meaningful metric)
    function highCard(title, color, emoji, subtitle, items, hint) {
        const list = items.slice(0, 6).map(i => `
            <div class="aud-cat">
                <span class="aud-cat-name">${i.c}</span>
                <span class="aud-cat-meta"><strong>над-индекс ${(i.maxDc).toFixed(2)}×</strong> · ${i.avgCh.toFixed(2)} чек/мес${i.avgP ? ' · ' + Math.round(i.avgP) + ' ₽' : ''} · мес: ${i.n}</span>
            </div>
        `).join('') || '<div class="aud-cat-empty">— нет маркеров в этой группе —</div>';
        return `
            <div class="aud-card" style="border-top:4px solid ${color}">
                <div class="aud-head">
                    <span class="aud-emoji">${emoji}</span>
                    <div>
                        <div class="aud-title" style="color:${color}">${title}</div>
                        <div class="aud-sub">${subtitle}</div>
                    </div>
                </div>
                <div class="aud-cats">${list}</div>
                ${hint ? `<div class="aud-hint">${hint}</div>` : ''}
            </div>
        `;
    }

    return `
        <div class="mck-divider" style="margin-top:32px">
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#722F37,transparent)"></div>
            <div class="mck-divider-label" style="color:#722F37">📊 СЛАЙД 5 · МИССИИ HIGH PRICE · МОСКВА + МО · ОФФЛАЙН</div>
            <div class="mck-divider-line" style="background:linear-gradient(90deg,transparent,#722F37,transparent)"></div>
        </div>

        <div class="card mck-card hp-hero">
            <div class="hp-hero-eyebrow">ПОЧЕМУ ДИКСИ — НЕ ЦЕЛЕВОЙ МАГАЗИН ДЛЯ HIGH PRICE</div>
            <div class="hp-hero-headline">
                Маркеры HIGH price клиента в Москве — это <span style="color:#722F37">не корзина регулярных покупок</span>:
                импортный алкоголь, готовая еда, сезонка, напитки от поставщиков, импульсные категории.
            </div>
            <div class="hp-hero-sub">
                Всего <strong>${all.length}</strong> уникальных категорий-маркеров за ${monthKeys.length} мес наблюдений
                (vs ${TRENDS_DATA[_trendsLatestKey('ACTIVE_LFL','Moscow + MO','OFF')]?.length || '—'} категорий у Active LFL за один месяц).
                Все маркеры имеют долю покупателей <strong>cn ≈ 0%</strong> — это значит,
                <strong>HIGH price клиент не приходит в Дикси за повседневной корзиной</strong>. Заходит точечно за конкретной миссией.
            </div>
        </div>

        <div class="card mck-card">
            <div class="card-header">
                <h3>1. За чем заходят: разложение маркеров по миссиям</h3>
                <span class="mck-subtitle">Все цифры — агрегат i_trend по HIGH_PRICE × Moscow+MO × OFF за ${monthKeys.length} мес. Метрика — над-индекс (dc) и частота визитов (ch)</span>
            </div>
            <div class="aud-grid">
                ${highCard('🍱 Готовая еда / СП', '#E87722', '🍱', `${ready.length} маркеров`, ready,
                    'Блины, оладьи, сырники, готовые завтраки, каши молочные, салаты СП. Зашёл случайно — взял что-то к чаю / на завтрак.')}
                ${highCard('🍷 Импортный алкоголь', '#722F37', '🍷', `${importAlc.length} маркеров`, importAlc,
                    'Австралийское/новозеландское вино, импортный джин, коньяк Франции. Цены 900–4000 ₽ — премиум-категория, которой нет в обычном дискаунтере. Заходит за «бутылкой к ужину».')}
                ${highCard('🎄 Сезонка / нон-фуд', '#88B04B', '🎄', `${seasonal.length} маркеров`, seasonal,
                    'Банки для консервации, грунт/удобрения, пасхальные наборы, аксессуары для шитья. Сезонные триггеры на разовый заход.')}
                ${highCard('🥤 Напитки от поставщиков (СП)', '#00897B', '🥤', `${supplier.length} маркеров`, supplier,
                    'Уникальные напитки от локальных поставщиков — то, чего нет в больших сетях. Точечный mission-driven заход.')}
                ${highCard('📱 Импульсные / нишевые', '#9B59B6', '📱', `${impulse.length} маркеров`, impulse,
                    'Мобильные телефоны, электронные сигареты, стики. Категории, за которыми заходят разово.')}
                ${highCard('🛒 Прочее', '#777', '🛒', `${otherCats.length} маркеров`, otherCats,
                    'Остальные нишевые категории, не вошедшие в основные миссии.')}
            </div>
        </div>

        <div class="card mck-card">
            <div class="card-header">
                <h3>2. Почему это происходит — анализ</h3>
            </div>
            <div class="hp-bullets">
                <div class="hp-bullet" style="border-left-color:#722F37">
                    <div class="hp-bullet-title">🎯 Дикси — не destination store для HIGH price клиента</div>
                    <div class="hp-bullet-text">
                        Регулярные покупки HIGH price аудитория делает в <strong>ВкусВилл, Перекрёсток, Азбука Вкуса, Глобус Гурмэ</strong> — там и широкий ассортимент, и премиум-полка, и сервис.
                        В Дикси они заходят <strong>convenience-визитами</strong> — мимо проходил, надо что-то быстро купить.
                    </div>
                </div>
                <div class="hp-bullet" style="border-left-color:#E87722">
                    <div class="hp-bullet-title">⏱ Convenience-миссии: «надо быстро здесь и сейчас»</div>
                    <div class="hp-bullet-text">
                        Готовая еда (блины, сырники, салаты СП) — <strong>${ready.length}</strong> маркеров. Это «зашёл на обед / завтрак / перекус», без планирования.
                        Не идёт за этим в магазин специально — попадает по пути.
                    </div>
                </div>
                <div class="hp-bullet" style="border-left-color:#722F37">
                    <div class="hp-bullet-title">🍷 Импортный алкоголь — единственный «премиум-якорь»</div>
                    <div class="hp-bullet-text">
                        <strong>${importAlc.length}</strong> маркеров импортного алкоголя — основной премиум-якорь, который удерживает HIGH в Дикси.
                        Уникальный SKU (новозеландское вино за 1100 ₽, джин импортный 987 ₽), которого может не быть рядом → заход именно за этим.
                    </div>
                </div>
                <div class="hp-bullet" style="border-left-color:#88B04B">
                    <div class="hp-bullet-title">🎄 Сезонные триггеры = разовые заходы</div>
                    <div class="hp-bullet-text">
                        Банки для консервации (август–сентябрь), пасхальные наборы (апрель), грунт/удобрения (весна) — <strong>${seasonal.length}</strong> маркеров.
                        Чисто сезонные миссии, не образуют лояльности.
                    </div>
                </div>
                <div class="hp-bullet" style="border-left-color:#9B59B6">
                    <div class="hp-bullet-title">📊 cn ≈ 0% подтверждает гипотезу</div>
                    <div class="hp-bullet-text">
                        Доля покупателей в каждом маркере крайне низкая (cn → 0%): это значит, что среди всех HIGH-клиентов <strong>нет ни одной категории, которую покупало бы хотя бы 1% сегмента стабильно</strong>.
                        Каждый клиент — со своей уникальной микро-миссией.
                    </div>
                </div>
            </div>
        </div>

        <div class="card mck-card mck-priorities">
            <div class="card-header"><h3>3. Что делать — стратегия для convenience-аудитории</h3></div>
            <div class="mck-priority-grid">
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#E87722">1</div>
                    <div class="mck-priority-title" style="color:#E87722">Усилить convenience-миссии</div>
                    <div class="mck-priority-desc">
                        Расширить готовую еду и СП-полку (блины, сырники, салаты, готовые завтраки) — <strong>основной триггер</strong> заходов HIGH.
                        Добавить горячую витрину / coffee-to-go в Москве.
                    </div>
                    <div class="mck-priority-impact" style="background:#fff5eb;color:#E87722">KPI: рост частоты HIGH в Москве</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#722F37">2</div>
                    <div class="mck-priority-title" style="color:#722F37">Премиум-алкоголь как якорь</div>
                    <div class="mck-priority-desc">
                        Удерживать и расширять линейку импортных вин и крепкого алкоголя — <strong>${importAlc.length} категорий</strong> уже работают как «якорь».
                        Добавить wine consultant / онлайн-каталог с информацией о редких SKU.
                    </div>
                    <div class="mck-priority-impact" style="background:#f5ecee;color:#722F37">KPI: SKU импортного вина / магазин Москва</div>
                </div>
                <div class="mck-priority">
                    <div class="mck-priority-num" style="background:#88B04B">3</div>
                    <div class="mck-priority-title" style="color:#88B04B">Сезонные кампании на захват</div>
                    <div class="mck-priority-desc">
                        Использовать сезонные триггеры (НГ, Пасха, дача, консервация) для регистрации HIGH в программе лояльности —
                        специальные «премиум-наборы к сезону» с подключением карты.
                    </div>
                    <div class="mck-priority-impact" style="background:#f5f9eb;color:#88B04B">KPI: новые HIGH-карты в сезонные пики</div>
                </div>
            </div>
        </div>
    `;
}

function renderSegmentGroupSummary(groupKey, cfg) {
    const state = _segGroupState[groupKey];
    const channelSnap = state.channel === 'offline' ? SNAPSHOTS_OFFLINE : SNAPSHOTS_ECOM;
    const channelLbl = state.channel === 'offline' ? 'Оффлайн' : 'E-commerce';
    const ym = state.ym;
    const ymList = Object.keys(channelSnap).sort();
    const idx = ymList.indexOf(ym);
    const last12 = ymList.slice(Math.max(0, idx - 11), idx + 1);
    const xLabels = last12.map(y => YM_LABELS[y] || y);

    const snap = channelSnap[ym] || {};
    const prevYm = '' + (parseInt(ym.slice(0,4)) - 1) + ym.slice(4);
    const prevSnap = channelSnap[prevYm] || {};
    const curLabel = YM_LABELS[ym] || ym;

    const segs = cfg.segments;
    const totalClients = segs.reduce((s, sg) => s + (snap[sg]?.CLIENTS || 0), 0);
    const prevTotalClients = segs.reduce((s, sg) => s + (prevSnap[sg]?.CLIENTS || 0), 0);
    const totalChange = prevTotalClients ? (totalClients - prevTotalClients) / prevTotalClients : null;

    function weightedAvg(s, metric) {
        let sumNum = 0, sumC = 0;
        segs.forEach(sg => {
            const c = s[sg]?.CLIENTS || 0;
            const v = s[sg]?.[metric];
            if (v != null && c) { sumNum += c * v; sumC += c; }
        });
        return sumC ? sumNum / sumC : null;
    }

    const curArpu = weightedAvg(snap, 'BUDGET');
    const prevArpu = weightedAvg(prevSnap, 'BUDGET');
    const arpuChange = (curArpu != null && prevArpu != null && prevArpu !== 0) ? (curArpu - prevArpu) / prevArpu : null;

    const curCheck = weightedAvg(snap, 'AVG_CHECK');
    const prevCheck = weightedAvg(prevSnap, 'AVG_CHECK');
    const checkChange = (curCheck != null && prevCheck != null && prevCheck !== 0) ? (curCheck - prevCheck) / prevCheck : null;

    // Top segment by revenue
    const segRev = segs.map(sg => ({
        seg: sg,
        rev: (snap[sg]?.CLIENTS || 0) * (snap[sg]?.BUDGET || 0)
    })).sort((a, b) => b.rev - a.rev);
    const totalRev = segRev.reduce((s, r) => s + r.rev, 0);
    const topSeg = segRev[0];
    const topSegShare = totalRev ? topSeg.rev / totalRev : 0;

    // Compute share of TO per segment (within the segs group)
    function segShareTO(s, sg) {
        const tot = segs.reduce((sum, x) => sum + (s[x]?.CLIENTS || 0) * (s[x]?.BUDGET || 0), 0);
        const segTO = (s[sg]?.CLIENTS || 0) * (s[sg]?.BUDGET || 0);
        return tot ? segTO / tot : null;
    }

    // YoY for table cells
    function yoyBadgeCell(sg, col) {
        const cur = snap[sg]?.[col];
        const prev = prevSnap[sg]?.[col];
        if (cur == null || prev == null || prev === 0) return '';
        const ch = (cur - prev) / prev;
        const cls = ch >= 0 ? 'up' : 'down';
        const sign = ch >= 0 ? '+' : '';
        return `<span class="tbl-yoy ${cls}">${sign}${(ch*100).toFixed(1)}%</span>`;
    }
    function compYoyBadgeCell(cur, prev) {
        if (cur == null || prev == null || prev === 0) return '';
        const ch = (cur - prev) / prev;
        const cls = ch >= 0 ? 'up' : 'down';
        const sign = ch >= 0 ? '+' : '';
        return `<span class="tbl-yoy ${cls}">${sign}${(ch*100).toFixed(1)}%</span>`;
    }

    const cols = [
        ['SHARE_CLIENTS', 'Доля базы', 'pct', null],
        ['SHARE_TO', 'Доля в ТО', 'pct', 'computed'],
        ['CLIENTS', 'Клиентов', 'int', null],
        ['BUDGET', 'ARPU', 'rub', null],
        ['COUNT_CHECK', 'Чеков/клиента', 'dec', 'arpu'],
        ['AVG_CHECK', 'Ср. чек', 'rub', 'arpu'],
        ['AVG_SKU', 'SKU/чек', 'dec', 'check'],
        ['AVG_COST_SKU', 'Ср. цена SKU', 'rub', 'check'],
        ['PRICE_INDEX', 'Цен. индекс', 'dec3', null],
        ['SALE', 'Скидка карта', 'pct', null],
        ['REAL_SALE', 'Реал. скидка', 'pct', null],
    ];

    const computedCols = [
        {
            label: 'Баллов/кл.',
            getValue: sg => {
                const bp = snap[sg]?.BONUS_PAY;
                const cl = snap[sg]?.CLIENTS;
                return (bp != null && cl) ? Math.abs(bp) / cl : null;
            },
            getPrev: sg => {
                const bp = prevSnap[sg]?.BONUS_PAY;
                const cl = prevSnap[sg]?.CLIENTS;
                return (bp != null && cl) ? Math.abs(bp) / cl : null;
            },
            fmt: v => v == null ? '—' : Math.round(v).toLocaleString('ru-RU')
        },
        {
            label: 'Redemption',
            getValue: sg => snap[sg]?.Redemption ?? null,
            getPrev: sg => prevSnap[sg]?.Redemption ?? null,
            fmt: v => v == null ? '—' : (v * 100).toFixed(1) + '%'
        }
    ];

    // Build two-row header
    const groupRow = [];
    const colRow = [];
    let i = 0;
    groupRow.push('<th rowspan="2" class="th-seg">Сегмент</th>');
    while (i < cols.length) {
        const [, lbl, , grp] = cols[i];
        if (grp === 'arpu') {
            groupRow.push('<th colspan="2" class="th-group th-group-arpu">ARPU = Чеки × Ср. чек</th>');
            colRow.push(`<th class="th-child th-child-arpu">${cols[i][1]}</th>`);
            colRow.push(`<th class="th-child th-child-arpu">${cols[i+1][1]}</th>`);
            i += 2;
        } else if (grp === 'check') {
            groupRow.push('<th colspan="2" class="th-group th-group-check">Ср. чек = SKU × Цена</th>');
            colRow.push(`<th class="th-child th-child-check">${cols[i][1]}</th>`);
            colRow.push(`<th class="th-child th-child-check">${cols[i+1][1]}</th>`);
            i += 2;
        } else {
            groupRow.push(`<th rowspan="2">${lbl}</th>`);
            i++;
        }
    }
    computedCols.forEach(c => groupRow.push(`<th rowspan="2">${c.label}</th>`));

    function cellClass(grp) {
        if (grp === 'arpu') return ' class="td-arpu"';
        if (grp === 'check') return ' class="td-check"';
        return '';
    }

    const tableTitle = `Сводная таблица по сегментам (${curLabel} · ${channelLbl})`;
    const groupTitle = groupKey === 'price_seg' ? 'Ценовые сегменты' : 'Сегменты по чеку';

    document.getElementById(`${groupKey}-segment-content`).innerHTML = `
        <div class="page-header" style="margin-top:8px">
            <div>
                <h1>${groupTitle} · Сводная</h1>
                <p class="subtitle">${curLabel} · ${channelLbl}</p>
            </div>
        </div>

        <div class="total-row">
            <div class="total-kpi-card" id="${groupKey}-total-kpi">
                <div class="kpi-label">Всего клиентов в сегментах</div>
                <div class="kpi-value">${fmt.int(totalClients)}</div>
                ${totalChange != null ? `<div class="kpi-change ${totalChange >= 0 ? 'up' : 'down'}">${totalChange >= 0 ? '+' : ''}${(totalChange*100).toFixed(1)}% YoY</div>` : ''}
                <div class="kpi-sub">${curLabel}</div>
            </div>
            <div class="total-pie-card">
                <div class="pie-title">Доля сегментов по клиентам</div>
                <canvas id="${groupKey}-pie" height="280"></canvas>
            </div>
        </div>

        <div class="kpi-grid">
            <div class="kpi-card" style="border-top-color:${SEG_COLORS_FULL[topSeg.seg] || '#FF7900'}">
                <div class="kpi-label">Лидер по выручке</div>
                <div class="kpi-value">${SEG_RU_FULL[topSeg.seg] || topSeg.seg}</div>
                <div class="kpi-change up">${(topSegShare*100).toFixed(1)}% от ТО</div>
                <div class="kpi-sub">${curLabel}</div>
            </div>
            <div class="kpi-card" style="border-top-color:#FF7900">
                <div class="kpi-label">ARPU (ср. взв.)</div>
                <div class="kpi-value">${fmt.rub(curArpu)}</div>
                ${arpuChange != null ? `<div class="kpi-change ${arpuChange >= 0 ? 'up' : 'down'}">${arpuChange >= 0 ? '+' : ''}${(arpuChange*100).toFixed(1)}% YoY</div>` : ''}
                <div class="kpi-sub">${curLabel}</div>
            </div>
            <div class="kpi-card" style="border-top-color:#5F259F">
                <div class="kpi-label">Ср. чек (ср. взв.)</div>
                <div class="kpi-value">${fmt.rub(curCheck)}</div>
                ${checkChange != null ? `<div class="kpi-change ${checkChange >= 0 ? 'up' : 'down'}">${checkChange >= 0 ? '+' : ''}${(checkChange*100).toFixed(1)}% YoY</div>` : ''}
                <div class="kpi-sub">${curLabel}</div>
            </div>
            <div class="kpi-card" style="border-top-color:#2E8B57">
                <div class="kpi-label">Сегментов в группе</div>
                <div class="kpi-value">${segs.length}</div>
                <div class="kpi-sub">${curLabel}</div>
            </div>
        </div>

        <div class="card">
            <div class="card-header">
                <h3>${tableTitle}</h3>
            </div>
            <div class="table-wrap">
                <table class="data-table" id="${groupKey}-summary-table">
                    <thead>
                        <tr>${groupRow.join('')}</tr>
                        <tr>${colRow.join('')}</tr>
                    </thead>
                    <tbody>
                        ${segs.map(sg => `<tr>
                            <td><span class="seg-badge"><span class="dot" style="background:${SEG_COLORS_FULL[sg]}"></span>${SEG_RU_FULL[sg]}</span></td>
                            ${cols.map(([col, , type, grp]) => {
                                if (col === 'SHARE_TO') {
                                    const cur = segShareTO(snap, sg);
                                    const prev = segShareTO(prevSnap, sg);
                                    return `<td><div class="tbl-cell">${fmtVal(cur, type)}${compYoyBadgeCell(cur, prev)}</div></td>`;
                                }
                                return `<td${cellClass(grp)}><div class="tbl-cell">${fmtVal(snap[sg]?.[col], type)}${yoyBadgeCell(sg, col)}</div></td>`;
                            }).join('')}
                            ${computedCols.map(cc => {
                                const cur = cc.getValue(sg);
                                const prev = cc.getPrev(sg);
                                return `<td><div class="tbl-cell">${cc.fmt(cur)}${compYoyBadgeCell(cur, prev)}</div></td>`;
                            }).join('')}
                        </tr>`).join('')}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="charts-row" style="margin-top:16px">
            <div class="card">
                <div class="card-header"><h3>Структура клиентской базы (${YM_LABELS[last12[0]]} — ${YM_LABELS[last12[last12.length-1]]})</h3></div>
                <div class="chart-container"><canvas id="${groupKey}-sum-structure"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>ARPU по сегментам (${YM_LABELS[last12[0]]} — ${YM_LABELS[last12[last12.length-1]]}), руб.</h3></div>
                <div class="chart-container"><canvas id="${groupKey}-sum-arpu"></canvas></div>
            </div>
        </div>
        <div class="charts-row">
            <div class="card">
                <div class="card-header"><h3>Средний чек по сегментам (${YM_LABELS[last12[0]]} — ${YM_LABELS[last12[last12.length-1]]}), руб.</h3></div>
                <div class="chart-container"><canvas id="${groupKey}-sum-check"></canvas></div>
            </div>
            <div class="card">
                <div class="card-header"><h3>Чеков/клиента по сегментам</h3></div>
                <div class="chart-container"><canvas id="${groupKey}-sum-freq"></canvas></div>
            </div>
        </div>

        ${groupKey === 'price_seg' ? renderPriceSegNarrative(snap, prevSnap, curLabel, channelLbl) : ''}
        ${groupKey === 'price_seg' ? renderActiveLflMoscowSlide() : ''}
        ${groupKey === 'price_seg' ? renderBigCheckSlide() : ''}
        ${groupKey === 'price_seg' ? renderHighSeasonalitySlide(channelSnap, ym) : ''}
        ${groupKey === 'price_seg' ? renderHighPriceMissionsSlide() : ''}
    `;

    // Render charts
    setTimeout(() => {
        // Pie
        destroyChart(`${groupKey}-pie`);
        const pieData = segs.map(sg => snap[sg]?.CLIENTS || 0);
        const pieColors = segs.map(sg => SEG_COLORS_FULL[sg]);
        const pieLabels = segs.map(sg => SEG_RU_FULL[sg]);
        const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text').trim() || '#333';
        const cardBg = getComputedStyle(document.documentElement).getPropertyValue('--card-bg').trim() || '#fff';

        const outlabelPlugin = {
            id: 'pieOutlabels' + groupKey,
            afterDraw(chart) {
                const { ctx } = chart;
                const meta = chart.getDatasetMeta(0);
                const ds = chart.data.datasets[0];
                const total = ds.data.reduce((a, b) => a + b, 0);
                if (!total) return;
                const labelHeight = 28;
                const leftLabels = [], rightLabels = [];
                meta.data.forEach((arc, i) => {
                    const val = ds.data[i];
                    if (!val) return;
                    const pct = ((val / total) * 100).toFixed(1);
                    const lbl = chart.data.labels[i];
                    const midAngle = (arc.startAngle + arc.endAngle) / 2;
                    const outerR = arc.outerRadius;
                    const cx = arc.x, cy = arc.y;
                    const edgeX = cx + Math.cos(midAngle) * outerR;
                    const edgeY = cy + Math.sin(midAngle) * outerR;
                    const isRight = Math.cos(midAngle) >= 0;
                    const elbowLen = 16;
                    const elbowX = cx + Math.cos(midAngle) * (outerR + elbowLen);
                    const elbowY = cy + Math.sin(midAngle) * (outerR + elbowLen);
                    const item = { i, lbl, pct, edgeX, edgeY, elbowX, elbowY, isRight, color: ds.backgroundColor[i], y: elbowY };
                    (isRight ? rightLabels : leftLabels).push(item);
                });
                function resolve(labels) {
                    labels.sort((a, b) => a.y - b.y);
                    for (let p = 0; p < 5; p++) {
                        for (let j = 1; j < labels.length; j++) {
                            const gap = labels[j].y - labels[j-1].y;
                            if (gap < labelHeight) {
                                const shift = (labelHeight - gap) / 2;
                                labels[j-1].y -= shift;
                                labels[j].y += shift;
                            }
                        }
                    }
                }
                resolve(leftLabels); resolve(rightLabels);
                [...leftLabels, ...rightLabels].forEach(item => {
                    const lineLen = 22;
                    const endX = item.elbowX + (item.isRight ? lineLen : -lineLen);
                    const endY = item.y;
                    ctx.save();
                    ctx.strokeStyle = item.color;
                    ctx.lineWidth = 1.5;
                    ctx.beginPath();
                    ctx.moveTo(item.edgeX, item.edgeY);
                    ctx.lineTo(item.elbowX, item.elbowY);
                    ctx.lineTo(endX, endY);
                    ctx.stroke();
                    ctx.fillStyle = item.color;
                    ctx.beginPath();
                    ctx.arc(item.edgeX, item.edgeY, 2.5, 0, Math.PI * 2);
                    ctx.fill();
                    const textX = endX + (item.isRight ? 4 : -4);
                    ctx.textAlign = item.isRight ? 'left' : 'right';
                    ctx.textBaseline = 'middle';
                    ctx.font = '600 11px Inter, sans-serif';
                    ctx.fillStyle = textColor;
                    ctx.fillText(item.lbl, textX, endY - 6);
                    ctx.font = '700 11px Inter, sans-serif';
                    ctx.fillStyle = item.color;
                    ctx.fillText(item.pct + '%', textX, endY + 7);
                    ctx.restore();
                });
            }
        };

        chartInstances[`${groupKey}-pie`] = new Chart(document.getElementById(`${groupKey}-pie`), {
            type: 'doughnut',
            data: {
                labels: pieLabels,
                datasets: [{ data: pieData, backgroundColor: pieColors, borderWidth: 2, borderColor: cardBg }]
            },
            plugins: [outlabelPlugin],
            options: {
                responsive: true, maintainAspectRatio: false, cutout: '45%',
                layout: { padding: { top: 50, bottom: 40, left: 100, right: 100 } },
                plugins: {
                    legend: { display: false },
                    datalabels: { display: false },
                    tooltip: {
                        callbacks: {
                            label: ctx => {
                                const v = ctx.parsed;
                                const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                                const pct = total ? ((v / total) * 100).toFixed(1) : 0;
                                return ` ${ctx.label}: ${fmt.int(v)} (${pct}%)`;
                            }
                        }
                    }
                }
            }
        });

        // Helper to build series for a metric
        function metricSeries(sg, metric) {
            return last12.map(y => channelSnap[y]?.[sg]?.[metric] ?? null);
        }

        // Structure stacked-bar (SHARE_CLIENTS within group)
        destroyChart(`${groupKey}-sum-structure`);
        const structDatasets = segs.map(sg => {
            const series = last12.map(y => {
                const tot = segs.reduce((a, x) => a + (channelSnap[y]?.[x]?.CLIENTS || 0), 0);
                return tot ? (channelSnap[y]?.[sg]?.CLIENTS || 0) / tot : null;
            });
            return {
                label: SEG_RU_FULL[sg],
                data: series,
                backgroundColor: SEG_COLORS_FULL[sg],
                borderRadius: 2,
                stack: 'share'
            };
        });
        chartInstances[`${groupKey}-sum-structure`] = new Chart(document.getElementById(`${groupKey}-sum-structure`), {
            type: 'bar',
            data: { labels: xLabels, datasets: structDatasets },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: {
                    x: { stacked: true, grid: { display: false } },
                    y: { stacked: true, ticks: { callback: v => (v*100).toFixed(0) + '%' }, grid: { color: '#f0f2f5' } }
                },
                plugins: {
                    legend: { position: 'top', align: 'start' },
                    tooltip: { callbacks: { label: ctx => ` ${ctx.dataset.label}: ${(ctx.parsed.y*100).toFixed(1)}%` } }
                }
            }
        });

        // ARPU line
        destroyChart(`${groupKey}-sum-arpu`);
        chartInstances[`${groupKey}-sum-arpu`] = new Chart(document.getElementById(`${groupKey}-sum-arpu`), {
            type: 'line',
            data: {
                labels: xLabels,
                datasets: segs.map(sg => ({
                    label: SEG_RU_FULL[sg],
                    data: metricSeries(sg, 'BUDGET'),
                    borderColor: SEG_COLORS_FULL[sg],
                    borderWidth: 2.5, fill: false
                }))
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: {
                    x: { grid: { display: false } },
                    y: { ticks: { callback: v => v.toLocaleString('ru-RU') }, grid: { color: '#f0f2f5' } }
                },
                plugins: { legend: { position: 'top', align: 'start' } }
            }
        });

        // AVG_CHECK line
        destroyChart(`${groupKey}-sum-check`);
        chartInstances[`${groupKey}-sum-check`] = new Chart(document.getElementById(`${groupKey}-sum-check`), {
            type: 'line',
            data: {
                labels: xLabels,
                datasets: segs.map(sg => ({
                    label: SEG_RU_FULL[sg],
                    data: metricSeries(sg, 'AVG_CHECK'),
                    borderColor: SEG_COLORS_FULL[sg],
                    borderWidth: 2.5, fill: false
                }))
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: {
                    x: { grid: { display: false } },
                    y: { ticks: { callback: v => v.toLocaleString('ru-RU') }, grid: { color: '#f0f2f5' } }
                },
                plugins: { legend: { position: 'top', align: 'start' } }
            }
        });

        // COUNT_CHECK line
        destroyChart(`${groupKey}-sum-freq`);
        chartInstances[`${groupKey}-sum-freq`] = new Chart(document.getElementById(`${groupKey}-sum-freq`), {
            type: 'line',
            data: {
                labels: xLabels,
                datasets: segs.map(sg => ({
                    label: SEG_RU_FULL[sg],
                    data: metricSeries(sg, 'COUNT_CHECK'),
                    borderColor: SEG_COLORS_FULL[sg],
                    borderWidth: 2.5, fill: false
                }))
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                scales: {
                    x: { grid: { display: false } },
                    y: { ticks: { callback: v => v.toFixed(2) }, grid: { color: '#f0f2f5' } }
                },
                plugins: { legend: { position: 'top', align: 'start' } }
            }
        });

        // Slide 4 — HIGH seasonality YoY charts
        if (groupKey === 'price_seg' && window._highSeasonalityData) {
            const hs = window._highSeasonalityData;
            const monthLbls = hs.last12.map(y => YM_LABELS[y] || y);
            const prevLbls  = hs.prev12.map(y => YM_LABELS[y] || y);
            const periodCur  = `${YM_LABELS[hs.last12[0]] || hs.last12[0]}–${YM_LABELS[hs.last12[hs.last12.length-1]] || hs.last12[hs.last12.length-1]}`;
            const periodPrev = `${YM_LABELS[hs.prev12[0]] || hs.prev12[0]}–${YM_LABELS[hs.prev12[hs.prev12.length-1]] || hs.prev12[hs.prev12.length-1]}`;

            const shareCanvas = document.getElementById('hs-chart-share');
            if (shareCanvas) {
                destroyChart('hs-chart-share');
                chartInstances['hs-chart-share'] = new Chart(shareCanvas, {
                    type: 'line',
                    data: {
                        labels: monthLbls,
                        datasets: [
                            {
                                label: periodPrev,
                                data: hs.prevShare.map(v => v != null ? v * 100 : null),
                                borderColor: '#94a3b8', borderWidth: 2, borderDash: [6, 4],
                                pointBackgroundColor: '#94a3b8', pointRadius: 3, fill: false, tension: 0.35,
                            },
                            {
                                label: periodCur,
                                data: hs.curShare.map(v => v != null ? v * 100 : null),
                                borderColor: '#1A5490', borderWidth: 3,
                                pointBackgroundColor: '#1A5490', pointRadius: 4, fill: false, tension: 0.35,
                            },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: v => v.toFixed(1) + '%' }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: {
                            legend: { position: 'top', align: 'start' },
                            tooltip: { callbacks: { label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)}%` } }
                        }
                    }
                });
            }

            const arpuCanvas = document.getElementById('hs-chart-arpu');
            if (arpuCanvas) {
                destroyChart('hs-chart-arpu');
                chartInstances['hs-chart-arpu'] = new Chart(arpuCanvas, {
                    type: 'line',
                    data: {
                        labels: monthLbls,
                        datasets: [
                            {
                                label: periodPrev,
                                data: hs.prevArpu,
                                borderColor: '#94a3b8', borderWidth: 2, borderDash: [6, 4],
                                pointBackgroundColor: '#94a3b8', pointRadius: 3, fill: false, tension: 0.35,
                            },
                            {
                                label: periodCur,
                                data: hs.curArpu,
                                borderColor: '#1A5490', borderWidth: 3,
                                pointBackgroundColor: '#1A5490', pointRadius: 4, fill: false, tension: 0.35,
                            },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: v => Math.round(v).toLocaleString('ru-RU') }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: {
                            legend: { position: 'top', align: 'start' },
                            tooltip: { callbacks: { label: ctx => ` ${ctx.dataset.label}: ${Math.round(ctx.parsed.y).toLocaleString('ru-RU')} ₽` } }
                        }
                    }
                });
            }
        }
    }, 0);
}

function renderSegmentGroupSegment(groupKey, cfg) {
    const state = _segGroupState[groupKey];
    const seg = state.sub;
    const channelSnap = state.channel === 'offline' ? SNAPSHOTS_OFFLINE : SNAPSHOTS_ECOM;
    const channelLbl = state.channel === 'offline' ? 'Оффлайн' : 'E-commerce';
    const color = SEG_COLORS_FULL[seg];
    const name = SEG_RU_FULL[seg];

    const ym = state.ym;
    const ymList = Object.keys(channelSnap).sort();
    const idx = ymList.indexOf(ym);
    const last12 = ymList.slice(Math.max(0, idx - 11), idx + 1);
    const xLabels = last12.map(y => YM_LABELS[y] || y);

    // Previous-year months for comparison
    const prevLast12 = last12.map(m => {
        const y = parseInt(m.slice(0, 4)) - 1;
        return '' + y + m.slice(4);
    });
    const firstLbl = YM_LABELS[last12[0]] || '';
    const lastLbl = YM_LABELS[last12[last12.length - 1]] || '';
    const prevFirstLbl = YM_LABELS[prevLast12[0]] || '';
    const prevLastLbl = YM_LABELS[prevLast12[prevLast12.length - 1]] || '';
    const curPeriod = firstLbl + '–' + lastLbl;
    const prevPeriod = prevFirstLbl + '–' + prevLastLbl;

    const snap = channelSnap[ym] || {};
    const prevYm = '' + (parseInt(ym.slice(0,4)) - 1) + ym.slice(4);
    const prevSnap = channelSnap[prevYm] || {};
    const curLabel = YM_LABELS[ym] || ym;

    const segData = snap[seg] || {};

    // KPI metrics
    const kpiMetrics = [
        ['SHARE_CLIENTS', 'Доля от базы', 'pct'],
        ['CLIENTS', 'Клиентов', 'int'],
        ['BUDGET', 'ARPU', 'rub'],
        ['AVG_CHECK', 'Ср. чек', 'rub'],
        ['COUNT_CHECK', 'Чеков/клиента', 'dec'],
        ['AVG_SKU', 'SKU/чек', 'dec'],
        ['SALE', 'Скидка по карте', 'pct'],
        ['REAL_SALE', 'Реал. скидка', 'pct'],
    ];

    const chartConfigs = [
        { id: `pcs-${seg}-share`, title: 'Доля от базы', metric: 'SHARE_CLIENTS', yFmt: v => (v*100).toFixed(1)+'%', type: 'line' },
        { id: `pcs-${seg}-clients`, title: 'Количество клиентов', metric: 'CLIENTS', yFmt: v => Math.round(v).toLocaleString('ru-RU'), type: 'bar' },
        { id: `pcs-${seg}-arpu`, title: 'ARPU, руб.', metric: 'BUDGET', yFmt: v => Math.round(v).toLocaleString('ru-RU'), type: 'line' },
        { id: `pcs-${seg}-check`, title: 'Средний чек, руб.', metric: 'AVG_CHECK', yFmt: v => Math.round(v).toLocaleString('ru-RU'), type: 'line' },
        { id: `pcs-${seg}-freq`, title: 'Чеков/клиента', metric: 'COUNT_CHECK', yFmt: v => v.toFixed(2), type: 'line' },
        { id: `pcs-${seg}-sku`, title: 'SKU / чек', metric: 'AVG_SKU', yFmt: v => v.toFixed(2), type: 'line' },
        { id: `pcs-${seg}-discount`, title: 'Скидки', metric: null, type: 'multi-discount' },
        { id: `pcs-${seg}-price`, title: 'Ценовой индекс', metric: 'PRICE_INDEX', yFmt: v => v.toFixed(3), type: 'line' },
    ];

    // Insight HTML
    const ins = PRICE_CHECK_INSIGHTS[seg];
    const insightHTML = ins ? `
        <div class="segment-insight" style="border-left-color:${color}">
            <div class="segment-insight-header">
                <div class="icon" style="background:${color};color:#fff">!</div>
                <h3>Выводы и рекомендации · ${channelLbl}</h3>
            </div>
            <div style="padding:14px 20px;background:#fafafa;border-bottom:1px solid var(--border);font-size:14px;font-weight:600;color:var(--text);line-height:1.5">
                ${ins.headline}
            </div>
            <div class="insight-grid">
                ${ins.blocks.map(b => `
                    <div class="insight-block">
                        <div class="insight-block-title">
                            <span class="tag tag-${b.tag}">${b.tagText}</span>
                            ${b.title}
                        </div>
                        <ul>${b.points.map(p => `<li>${p}</li>`).join('')}</ul>
                    </div>
                `).join('')}
            </div>
            <div class="insight-rec-block">
                <div class="rec-title">Рекомендации</div>
                <div class="rec-items">
                    ${ins.recs.map(r => `
                        <div class="rec-item">
                            <strong>${r.title}</strong>
                            ${r.text}
                        </div>
                    `).join('')}
                </div>
            </div>
        </div>
    ` : '';

    // YoY change helper
    function snapChangeLocal(metric) {
        const cur = segData?.[metric];
        const prev = prevSnap[seg]?.[metric];
        if (cur == null || prev == null || prev === 0) return null;
        return (cur - prev) / prev;
    }

    document.getElementById(`${groupKey}-segment-content`).innerHTML = `
        <div class="page-header" style="margin-top:8px">
            <div>
                <div class="segment-header">
                    <div class="segment-dot" style="background:${color}"></div>
                    <h1>${name}</h1>
                    <span style="font-size:12px;color:#6b7b8d;margin-left:12px;padding:4px 10px;background:#f3f4f6;border-radius:12px">${channelLbl}</span>
                </div>
                <p class="segment-desc">${cfg.descs[seg]}</p>
            </div>
        </div>

        <div class="segment-kpi-grid">
            ${kpiMetrics.map(([col, label, type]) => {
                const val = segData?.[col];
                const change = snapChangeLocal(col);
                return `<div class="segment-kpi" style="border-top: 3px solid ${color}">
                    <div class="kpi-label">${label}</div>
                    <div class="kpi-value">${fmtVal(val, type)}</div>
                    ${change != null ? `<div class="kpi-change ${change >= 0 ? 'up' : 'down'}">${change >= 0 ? '+' : ''}${(change*100).toFixed(1)}% YoY</div>` : ''}
                    <div class="kpi-sub">${curLabel}</div>
                </div>`;
            }).join('')}
        </div>

        ${[0,2,4,6].map(i => `
            <div class="charts-row">
                ${chartConfigs.slice(i, i+2).map(c => `
                    <div class="card">
                        <div class="card-header"><h3>${c.title}</h3></div>
                        <div class="chart-container"><canvas id="${c.id}"></canvas></div>
                    </div>
                `).join('')}
            </div>
        `).join('')}

        ${insightHTML}
    `;

    // Render charts
    setTimeout(() => {
        const curData = metric => last12.map(y => channelSnap[y]?.[seg]?.[metric] ?? null);
        const prevData = metric => prevLast12.map(y => channelSnap[y]?.[seg]?.[metric] ?? null);

        chartConfigs.forEach(cfgC => {
            const canvas = document.getElementById(cfgC.id);
            if (!canvas) return;
            destroyChart(cfgC.id);

            if (cfgC.type === 'multi-discount') {
                chartInstances[cfgC.id] = new Chart(canvas, {
                    type: 'line',
                    data: {
                        labels: xLabels,
                        datasets: [
                            { label: 'Карта (пред.)', data: prevData('SALE'), borderColor: '#aaa', borderDash: [6,3], borderWidth: 2, fill: false, pointRadius: 2 },
                            { label: 'Карта (тек.)', data: curData('SALE'), borderColor: '#FF7900', borderWidth: 2.5, fill: false },
                            { label: 'Реал. (пред.)', data: prevData('REAL_SALE'), borderColor: '#9B59B6', borderDash: [6,3], borderWidth: 2, fill: false, pointRadius: 2 },
                            { label: 'Реал. (тек.)', data: curData('REAL_SALE'), borderColor: '#C41E3A', borderWidth: 2.5, fill: false },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: v => (v*100).toFixed(1)+'%' }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: { legend: { position: 'top', align: 'start' } }
                    }
                });
                return;
            }

            const dCur = curData(cfgC.metric);
            const dPrev = prevData(cfgC.metric);

            if (cfgC.type === 'bar') {
                chartInstances[cfgC.id] = new Chart(canvas, {
                    type: 'bar',
                    data: {
                        labels: xLabels,
                        datasets: [
                            { label: prevPeriod, data: dPrev, backgroundColor: '#e0e0e0', borderRadius: 4 },
                            { label: curPeriod, data: dCur, backgroundColor: color, borderRadius: 4 },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: cfgC.yFmt }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: { legend: { position: 'top', align: 'start' } }
                    }
                });
            } else {
                chartInstances[cfgC.id] = new Chart(canvas, {
                    type: 'line',
                    data: {
                        labels: xLabels,
                        datasets: [
                            { label: prevPeriod, data: dPrev, borderColor: '#aaa', borderDash: [6,3], borderWidth: 2, fill: false, pointRadius: 2 },
                            { label: curPeriod, data: dCur, borderColor: color, borderWidth: 2.5, fill: false },
                        ]
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        scales: {
                            x: { grid: { display: false } },
                            y: { ticks: { callback: cfgC.yFmt }, grid: { color: '#f0f2f5' } }
                        },
                        plugins: { legend: { position: 'top', align: 'start' } }
                    }
                });
            }
        });
    }, 50);
}

// CSS for sub-tabs and channel buttons
(function() {
    const style = document.createElement('style');
    style.textContent = `
        .seg-group-controls { display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; gap:12px; flex-wrap:wrap; }
        .seg-sub-tabs { display:flex; gap:0; background:#f3f4f6; border-radius:8px; padding:4px; }
        .seg-sub-btn {
            padding:9px 18px; border:none; background:transparent; cursor:pointer;
            font-size:13px; font-weight:600; color:#6b7b8d; border-radius:6px;
            transition:all 0.2s; display:inline-flex; align-items:center; gap:8px;
            font-family:inherit;
        }
        .seg-sub-btn.active {
            background:#fff; color:#1a1a1a; box-shadow:0 1px 3px rgba(0,0,0,0.1);
        }
        .seg-sub-dot { width:10px; height:10px; border-radius:50%; display:inline-block; }
        .seg-channel-toggle { display:flex; background:#f3f4f6; border-radius:8px; padding:3px; }
        .seg-channel-btn {
            padding:8px 16px; border:none; background:transparent; cursor:pointer;
            font-size:13px; font-weight:600; color:#6b7b8d; border-radius:6px;
            transition:all 0.2s; font-family:inherit;
        }
        .seg-channel-btn.active {
            background:#fff; color:#003A70; box-shadow:0 1px 3px rgba(0,0,0,0.1);
        }
        .seg-channel-btn:hover:not(.active) { color:#333; }
    `;
    document.head.appendChild(style);
})();

// --- Init ---
initMonthSelector();
buildSummary();
buildDefinitions();
buildInsights();

/* ==================================================================
   MISSIONS CATALOGUE — McKinsey-style mission grid + drawer + heatmap
   ================================================================== */
(function initMissionsTab() {
    if (typeof MISSIONS_DATA === 'undefined') return;

    const rankBand = r => {
        if (r <= 10) return 'rank-band-1';   // календарные праздники
        if (r <= 20) return 'rank-band-2';   // сезонные
        if (r <= 30) return 'rank-band-3';   // поведенческие
        if (r <= 40) return 'rank-band-4';   // контекст еды
        if (r <= 50) return 'rank-band-5';   // идентичность
        if (r <= 60) return 'rank-band-6';   // алкоголь-табак
        if (r <= 70) return 'rank-band-7';   // спец
        if (r <= 80) return 'rank-band-8';   // перекус-сладкое
        if (r <= 90) return 'rank-band-9';   // напитки
        if (r <= 100) return 'rank-band-10'; // daily
        return '';
    };

    const fmtTime = w => {
        if (!w || w === 'always') return null;
        const months = ['','Янв','Фев','Мар','Апр','Май','Июн','Июл','Авг','Сен','Окт','Ноя','Дек'];
        return w.split(',').map(m => months[parseInt(m)]).filter(Boolean).join('·');
    };

    const renderCard = (m, matchInfo) => {
        const band = rankBand(m.MISSION_RANK);
        const tw = fmtTime(m.TIME_WINDOW);
        const badges = [];
        if (tw) badges.push(`<span class="mc-badge is-time">${tw}</span>`);
        if (matchInfo && matchInfo.count > 0) {
            badges.push(`<span class="mc-badge is-match" title="${matchInfo.example.replace(/"/g, '&quot;')}">${matchInfo.count} совпадений</span>`);
        }

        return `
            <div class="mission-card ${band}" data-mission="${m.ID_MISSION}">
                <div class="mission-card-head">
                    <div class="mission-card-icon"><img src="${m.ICON_URL}" alt=""></div>
                    <div class="mission-card-title-block">
                        <div class="mission-card-title">${m.NAME}</div>
                        <div class="mission-card-group">${m.GROUP}</div>
                    </div>
                </div>
                <div class="mission-card-badges">${badges.join('')}</div>
                <div class="mission-card-desc">${m.DESCRIPTION || ''}</div>
                <div class="mission-card-metrics">
                    <div class="mc-metric">
                        <div class="mc-metric-value is-accent">${m.N_PRODUCTS}</div>
                        <div class="mc-metric-label">продуктов</div>
                    </div>
                    <div class="mc-metric">
                        <div class="mc-metric-value">${m.N_CATEGORIES}</div>
                        <div class="mc-metric-label">категорий</div>
                    </div>
                </div>
            </div>`;
    };

    const renderHeatmap = () => {
        const visible = MISSIONS_DATA.filter(m => m.ID_MISSION !== 999)
            .sort((a, b) => a.MISSION_RANK - b.MISSION_RANK);
        const colorFor = pct => {
            if (pct >= 80) return '#b91c1c';
            if (pct >= 60) return '#FF7900';
            if (pct >= 40) return '#f59e0b';
            if (pct >= 20) return '#fed7aa';
            if (pct >= 5)  return '#fef3c7';
            if (pct > 0)   return '#fafbfd';
            return '#ffffff';
        };
        const head = '<tr><th class="row-header">A \\ B</th>' +
            visible.map(m => `<th class="col-header"><span>${m.NAME}</span></th>`).join('') + '</tr>';
        const rows = visible.map(a => {
            const cells = visible.map(b => {
                const v = (MISSIONS_OVERLAP[a.ID_MISSION] || {})[b.ID_MISSION] || 0;
                const isSelf = a.ID_MISSION === b.ID_MISSION;
                const css = `background:${colorFor(v)}`;
                return `<td class="heatmap-cell ${isSelf ? 'is-self' : ''}" style="${css}" title="${a.NAME} → ${b.NAME}: ${v}%">${isSelf ? '—' : v}</td>`;
            }).join('');
            return `<tr><th class="row-header">${a.NAME}</th>${cells}</tr>`;
        }).join('');
        return `
            <div class="heatmap-legend">
                <span>% продуктов миссии A, которые также входят в миссию B</span>
                <span class="heatmap-legend-gradient"></span>
                <span>0 → 100%</span>
            </div>
            <table class="heatmap-table">${head}${rows}</table>`;
    };

    const openDrawer = (mid) => {
        const m = MISSIONS_DATA.find(x => x.ID_MISSION === mid);
        if (!m) return;
        const products = MISSIONS_PRODUCTS[String(mid)] || [];

        // Group products by category
        const byCat = {};
        products.forEach(p => {
            const key = `${p.cid}__${p.c}`;
            if (!byCat[key]) byCat[key] = { name: p.c || '(без категории)', items: [], chk: 0 };
            byCat[key].items.push(p);
            byCat[key].chk += p.chk;
        });
        const cats = Object.values(byCat).sort((a, b) => b.chk - a.chk);

        // Top-N overlapping missions
        const overlapMap = MISSIONS_OVERLAP[String(mid)] || {};
        const overlaps = Object.entries(overlapMap)
            .filter(([k, v]) => parseInt(k) !== mid && v > 0)
            .sort(([, a], [, b]) => b - a)
            .slice(0, 12)
            .map(([k, v]) => ({
                id: parseInt(k),
                name: (MISSIONS_DATA.find(x => x.ID_MISSION === parseInt(k)) || {}).NAME || k,
                pct: v
            }));

        const tw = fmtTime(m.TIME_WINDOW);
        const excl = (m.EXCLUSION_CATS || '').split(',').filter(s => s.length).length;

        const drawer = document.getElementById('mission-drawer');
        const body = drawer.querySelector('.mission-drawer-body');
        drawer.querySelector('.mission-drawer-icon img').src = m.ICON_URL;
        drawer.querySelector('.mission-drawer-title').textContent = m.NAME;
        drawer.querySelector('.mission-drawer-group').textContent = `${m.GROUP} · приоритет #${m.MISSION_RANK}`;

        body.innerHTML = `
            <div class="mdrawer-section">
                <div class="mdrawer-section-title">Описание</div>
                <div class="mdrawer-desc">${m.DESCRIPTION || ''}</div>
            </div>

            <div class="mdrawer-section">
                <div class="mdrawer-section-title">Параметры идентификации</div>
                <div class="mdrawer-params">
                    <div class="mdrawer-param">
                        <div class="mdrawer-param-label">Продуктов</div>
                        <div class="mdrawer-param-value">${m.N_PRODUCTS}</div>
                    </div>
                    <div class="mdrawer-param">
                        <div class="mdrawer-param-label">Категорий</div>
                        <div class="mdrawer-param-value">${m.N_CATEGORIES}</div>
                    </div>
                    <div class="mdrawer-param">
                        <div class="mdrawer-param-label">Период</div>
                        <div class="mdrawer-param-value" style="font-size:13px">${tw || 'круглый год'}</div>
                    </div>
                </div>
            </div>

            ${overlaps.length ? `
            <div class="mdrawer-section">
                <div class="mdrawer-section-title">Пересечения с другими миссиями (% продуктов A в B)</div>
                ${overlaps.map(o => `
                    <div class="mdrawer-overlap-bar">
                        <div class="mdrawer-overlap-name">${o.name}</div>
                        <div class="mdrawer-overlap-track"><div class="mdrawer-overlap-fill" style="width:${Math.min(o.pct, 100)}%"></div></div>
                        <div class="mdrawer-overlap-pct">${o.pct}%</div>
                    </div>
                `).join('')}
            </div>` : ''}

            <div class="mdrawer-section">
                <div class="mdrawer-section-title">Продукты миссии (${products.length})</div>
                <div class="mdrawer-products-search">
                    <input type="search" id="mdrawer-prod-search" placeholder="поиск по продукту или категории…">
                </div>
                <div id="mdrawer-cats">
                    ${cats.map((c, i) => `
                        <div class="mdrawer-cat-block" data-cat-name="${c.name.toLowerCase()}">
                            <div class="mdrawer-cat-head">
                                <span class="mdrawer-cat-name">${c.name}</span>
                                <span class="mdrawer-cat-count">${c.items.length} прод</span>
                            </div>
                            <ul class="mdrawer-prods-list">
                                ${c.items.slice(0, 50).map(p => `
                                    <li data-prod-name="${(p.p || '').toLowerCase()}">
                                        <span class="mdrawer-prod-name">${p.p}</span>
                                    </li>
                                `).join('')}
                                ${c.items.length > 50 ? `<li><span class="mdrawer-prod-name" style="color:#94a3b8">…ещё ${c.items.length - 50}</span></li>` : ''}
                            </ul>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;

        const search = document.getElementById('mdrawer-prod-search');
        if (search) {
            search.addEventListener('input', e => {
                const q = e.target.value.toLowerCase();
                body.querySelectorAll('.mdrawer-cat-block').forEach(block => {
                    const catName = block.dataset.catName;
                    const catMatch = catName.includes(q);
                    let visible = 0;
                    block.querySelectorAll('li[data-prod-name]').forEach(li => {
                        const match = catMatch || li.dataset.prodName.includes(q);
                        li.style.display = match ? '' : 'none';
                        if (match) visible++;
                    });
                    block.style.display = (q === '' || visible > 0 || catMatch) ? '' : 'none';
                });
            });
        }

        document.getElementById('mission-drawer-backdrop').classList.add('is-open');
        drawer.classList.add('is-open');
    };

    const closeDrawer = () => {
        document.getElementById('mission-drawer-backdrop').classList.remove('is-open');
        document.getElementById('mission-drawer').classList.remove('is-open');
    };

    const buildDrawerSkeleton = () => {
        if (document.getElementById('mission-drawer')) return;
        const html = `
            <div class="mission-drawer-backdrop" id="mission-drawer-backdrop"></div>
            <aside class="mission-drawer" id="mission-drawer" aria-label="Детали миссии">
                <header class="mission-drawer-header">
                    <div class="mission-drawer-icon"><img src="" alt=""></div>
                    <div class="mission-drawer-title-block">
                        <h2 class="mission-drawer-title"></h2>
                        <div class="mission-drawer-group"></div>
                    </div>
                    <button class="mission-drawer-close" id="mission-drawer-close" aria-label="Закрыть">
                        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 6l12 12M18 6L6 18"/></svg>
                    </button>
                </header>
                <div class="mission-drawer-body"></div>
            </aside>`;
        document.body.insertAdjacentHTML('beforeend', html);
        document.getElementById('mission-drawer-backdrop').addEventListener('click', closeDrawer);
        document.getElementById('mission-drawer-close').addEventListener('click', closeDrawer);
        document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDrawer(); });
    };

    const populateGroupFilter = () => {
        const sel = document.getElementById('mission-filter-group');
        const groups = Array.from(new Set(MISSIONS_DATA.map(m => m.GROUP))).filter(Boolean);
        groups.forEach(g => {
            const opt = document.createElement('option');
            opt.value = g; opt.textContent = g;
            sel.appendChild(opt);
        });
    };

    // Память о свёрнутом состоянии групп (session)
    if (!window._missionsGroupExpanded) window._missionsGroupExpanded = {};

    const renderMissions = () => {
        const sortBy = document.getElementById('mission-filter-sort').value;
        const grp    = document.getElementById('mission-filter-group').value;
        const q      = (document.getElementById('mission-filter-search').value || '').toLowerCase();

        let list = MISSIONS_DATA.filter(m => m.ID_MISSION !== 999);
        if (grp) list = list.filter(m => m.GROUP === grp);
        if (q) {
            list = list.filter(m => {
                if (m.NAME.toLowerCase().includes(q)) return true;
                if ((m.DESCRIPTION || '').toLowerCase().includes(q)) return true;
                if ((m.GROUP || '').toLowerCase().includes(q)) return true;
                const prods = MISSIONS_PRODUCTS[String(m.ID_MISSION)] || [];
                return prods.some(p => (p.p || '').toLowerCase().includes(q) || (p.c || '').toLowerCase().includes(q));
            });
        }
        if (sortBy === 'products') list.sort((a, b) => b.N_PRODUCTS - a.N_PRODUCTS);
        else if (sortBy === 'name') list.sort((a, b) => a.NAME.localeCompare(b.NAME, 'ru'));
        else list.sort((a, b) => a.MISSION_RANK - b.MISSION_RANK);

        // Подсчёт совпадений по продуктам если поиск активен
        const matches = {};
        if (q) {
            list.forEach(m => {
                const prods = MISSIONS_PRODUCTS[String(m.ID_MISSION)] || [];
                const matched = prods.filter(p =>
                    (p.p || '').toLowerCase().includes(q) || (p.c || '').toLowerCase().includes(q));
                if (matched.length) {
                    matches[m.ID_MISSION] = {
                        count: matched.length,
                        example: matched[0].p + (matched[0].c ? ` (${matched[0].c})` : '')
                    };
                }
            });
        }

        // Группируем
        const groupsMap = new Map();
        list.forEach(m => {
            const g = m.GROUP || '(без группы)';
            if (!groupsMap.has(g)) groupsMap.set(g, []);
            groupsMap.get(g).push(m);
        });

        // Порядок групп — по минимальному рангу первой миссии (после применения sort)
        // Для sortBy='rank' это даст естественный порядок; иначе — по min rank в группе.
        const groupsArr = Array.from(groupsMap.entries()).map(([name, missions]) => {
            const minRank = Math.min(...missions.map(m => m.MISSION_RANK));
            const totalProds = missions.reduce((s, m) => s + (m.N_PRODUCTS || 0), 0);
            return { name, missions, minRank, totalProds };
        });
        if (sortBy === 'products')      groupsArr.sort((a, b) => b.totalProds - a.totalProds);
        else if (sortBy === 'name')     groupsArr.sort((a, b) => a.name.localeCompare(b.name, 'ru'));
        else                             groupsArr.sort((a, b) => a.minRank - b.minRank);

        // Автораскрытие группы если есть search или выбрана конкретная группа
        const forceOpen = !!(q || grp);

        const html = groupsArr.map(g => {
            const expanded = forceOpen || !!window._missionsGroupExpanded[g.name];
            const iconStrip = g.missions.slice(0, 6).map(m =>
                `<img class="mgroup-icon" src="${m.ICON_URL}" alt="" loading="lazy">`
            ).join('');
            const extra = g.missions.length > 6 ? `<span class="mgroup-icon-more">+${g.missions.length - 6}</span>` : '';
            return `
                <section class="missions-group ${expanded ? 'is-open' : ''}" data-group="${g.name}">
                    <header class="missions-group-head" tabindex="0" role="button" aria-expanded="${expanded}">
                        <div class="mgroup-icons">${iconStrip}${extra}</div>
                        <div class="mgroup-title-block">
                            <h3 class="mgroup-title">${g.name}</h3>
                            <div class="mgroup-meta">${g.missions.length} миссии · ${g.totalProds.toLocaleString('ru-RU')} продуктов</div>
                        </div>
                        <svg class="mgroup-chevron" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 9l6 6 6-6"/></svg>
                    </header>
                    <div class="missions-group-body">
                        <div class="missions-cards-grid">${g.missions.map(m => renderCard(m, matches[m.ID_MISSION])).join('')}</div>
                    </div>
                </section>`;
        }).join('');

        document.getElementById('missions-cards').innerHTML = html;
        document.getElementById('missions-count-visible').textContent =
            `${list.length} ${list.length === 1 ? 'миссия' : (list.length > 4 ? 'миссий' : 'миссии')} · ${groupsArr.length} групп`;
    };

    const initOnce = () => {
        if (window._missionsInited) return;
        window._missionsInited = true;
        buildDrawerSkeleton();
        populateGroupFilter();

        document.getElementById('mission-filter-group').addEventListener('change', renderMissions);
        document.getElementById('mission-filter-sort').addEventListener('change', renderMissions);
        document.getElementById('mission-filter-search').addEventListener('input', renderMissions);

        document.getElementById('missions-cards').addEventListener('click', e => {
            const head = e.target.closest('.missions-group-head');
            if (head) {
                const section = head.closest('.missions-group');
                const name = section.dataset.group;
                const willOpen = !section.classList.contains('is-open');
                section.classList.toggle('is-open', willOpen);
                head.setAttribute('aria-expanded', willOpen);
                window._missionsGroupExpanded[name] = willOpen;
                return;
            }
            const card = e.target.closest('.mission-card');
            if (card) openDrawer(parseInt(card.dataset.mission));
        });
        document.getElementById('missions-cards').addEventListener('keydown', e => {
            if ((e.key === 'Enter' || e.key === ' ') && e.target.classList.contains('missions-group-head')) {
                e.preventDefault();
                e.target.click();
            }
        });

        const toggle = document.getElementById('missions-view-toggle');
        toggle.addEventListener('click', () => {
            const showHeatmap = toggle.dataset.view === 'cards';
            toggle.dataset.view = showHeatmap ? 'heatmap' : 'cards';
            toggle.classList.toggle('is-active', showHeatmap);
            document.getElementById('missions-cards').classList.toggle('is-hidden', showHeatmap);
            document.querySelector('.missions-filters').classList.toggle('is-hidden', showHeatmap);
            const hm = document.getElementById('missions-heatmap');
            hm.classList.toggle('is-hidden', !showHeatmap);
            if (showHeatmap && !hm.dataset.rendered) {
                hm.innerHTML = renderHeatmap();
                hm.dataset.rendered = '1';
            }
            toggle.lastChild.textContent = showHeatmap ? ' Карточки миссий' : ' Матрица пересечений';
        });

        renderMissions();
    };

    // Init when the missions tab becomes visible
    document.addEventListener('click', e => {
        const nav = e.target.closest('[data-tab="missions"]');
        if (nav) setTimeout(initOnce, 50);
    });
    // Auto-init if missions tab is already active on load
    if (document.querySelector('#tab-missions.active')) initOnce();
})();


/* =========================================================================
   COFFEE MISSION DASHBOARD (mission_id=55 «Готовый кофе»)
   Источник: данные/Готовый кофе.xlsx → coffee_data.js (COFFEE_DATA)
   ========================================================================= */
(function() {
    const COFFEE_BROWN   = '#6F4E37';
    const COFFEE_ORANGE  = '#FF7900';
    const COFFEE_PURPLE  = '#5F259F';
    const COFFEE_GRAY    = '#94A3B8';
    const COFFEE_BG_SOFT = '#F8F4EF';
    const COFFEE_TXT     = '#1F2A37';

    const YM_LABEL = (ym) => {
        const y = Math.floor(ym / 100);
        const m = ym % 100;
        const MM = ['янв','фев','мар','апр','май','июн','июл','авг','сен','окт','ноя','дек'];
        return MM[m-1] + " '" + String(y).slice(-2);
    };
    const fmtPct  = (x, d=1) => (x == null) ? '—' : (x*100).toFixed(d) + '%';
    const fmtPctS = (x, d=1) => (x == null) ? '—' : (x >= 0 ? '+' : '') + (x*100).toFixed(d) + '%';
    // LIFT = ratio (×N), а не delta (+N%). На вход — relative delta (X_TO_ALL),
    // lift = 1 + delta. Например, delta = +0.20 → lift = ×1.20; delta = -0.50 → ×0.50.
    const fmtLift  = (x, d=2) => (x == null) ? '—' : '×' + (1 + x).toFixed(d);
    const fmtInt  = (x) => (x == null) ? '—' : Math.round(x).toLocaleString('ru-RU').replace(/,/g,' ');
    const fmtMln  = (x) => (x == null) ? '—' : (x/1e6).toFixed(1) + ' млн ₽';
    const fmtMlrd = (x) => (x == null) ? '—' : (x/1e9).toFixed(2) + ' млрд ₽';

    function buildCoffee() {
        const container = document.getElementById('tab-coffee');
        if (!container) return;
        if (typeof COFFEE_DATA === 'undefined') {
            container.innerHTML = '<div class="page-header"><h1>Готовый кофе</h1><p class="subtitle">Данные не загружены (coffee_data.js)</p></div>';
            return;
        }

        const d  = COFFEE_DATA;
        const dyn = d.dynamics;
        const last = dyn[dyn.length - 1];
        const first = dyn[0];
        const y2024Apr = dyn.find(r => r.YEAR_MONTH === 202404);
        const y2025Apr = dyn.find(r => r.YEAR_MONTH === 202504);

        // YoY apr'26 vs apr'25
        const shareYoY  = (last.SHARE_CHECKS - y2025Apr.SHARE_CHECKS) / y2025Apr.SHARE_CHECKS;
        const contactsYoY = (last.N_CONTACTS_COFFEE - y2025Apr.N_CONTACTS_COFFEE) / y2025Apr.N_CONTACTS_COFFEE;
        const toYoY     = (last.TO_COFFEE - y2025Apr.TO_COFFEE) / y2025Apr.TO_COFFEE;
        const xGrowth   = last.SHARE_CHECKS / first.SHARE_CHECKS;

        // Aggregated for last 3 months (фев-апр 2026) — для tooltip-стабильности
        const last3 = dyn.slice(-3);
        const checksL3   = last3.reduce((s,r)=>s+r.N_CHECKS_COFFEE, 0);
        const toL3       = last3.reduce((s,r)=>s+r.TO_COFFEE, 0);
        const avgCheckL3 = toL3 / checksL3;

        container.innerHTML = `
            <div class="page-header">
                <div>
                    <h1>Готовый кофе</h1>
                    <p class="subtitle">Витрина с кофе (капучино / латте / американо и т.д.) · 53 миссии → одна, удвоившая долю чеков за год · период янв 2024 — апр 2026</p>
                </div>
                <div class="header-meta">
                    <div class="coffee-period-badge">срез affinity: апр 2026</div>
                </div>
            </div>

            <!-- ACTION TITLE -->
            <div class="coffee-headline">
                Доля чеков с готовым кофе: <b>×${xGrowth.toFixed(0)}</b> за 24 месяца (янв 2024 → апр 2026)
            </div>

            <!-- KPI HERO STRIP -->
            <div class="coffee-kpi-grid">
                <div class="coffee-kpi-card" style="--accent:${COFFEE_ORANGE}">
                    <div class="coffee-kpi-label">Доля чеков с кофе · апр'26</div>
                    <div class="coffee-kpi-value">${fmtPct(last.SHARE_CHECKS, 2)}</div>
                    <div class="coffee-kpi-delta up">${fmtPctS(shareYoY)} YoY · ×${xGrowth.toFixed(1)} к янв'24</div>
                </div>
                <div class="coffee-kpi-card" style="--accent:${COFFEE_BROWN}">
                    <div class="coffee-kpi-label">Контактов покупает кофе</div>
                    <div class="coffee-kpi-value">${fmtInt(last.N_CONTACTS_COFFEE)}</div>
                    <div class="coffee-kpi-delta up">${fmtPctS(contactsYoY)} YoY · ${fmtPct(last.SHARE_CONTACTS, 2)} базы</div>
                </div>
                <div class="coffee-kpi-card" style="--accent:${COFFEE_PURPLE}">
                    <div class="coffee-kpi-label">ТО миссии · апр'26</div>
                    <div class="coffee-kpi-value">${fmtMln(last.TO_COFFEE)}</div>
                    <div class="coffee-kpi-delta up">${fmtPctS(toYoY)} YoY · ${fmtPct(last.SHARE_TO,2)} всего ТО</div>
                </div>
                <div class="coffee-kpi-card" style="--accent:${COFFEE_GRAY}">
                    <div class="coffee-kpi-label">Средний чек визита за кофе</div>
                    <div class="coffee-kpi-value">${Math.round(last.AVG_CHECK_COFFEE)} ₽</div>
                    <div class="coffee-kpi-delta down">${Math.round((1 + (last.AVG_CHECK_TO_ALL || 0)) * 100)}% от среднего чека базы (${Math.round(last.AVG_CHECK_ALL)} ₽)</div>
                </div>
            </div>

            <!-- BLOCK 1: DYNAMICS -->
            <div class="coffee-card">
                <div class="coffee-card-head">
                    <div class="coffee-action-title">Динамика миссии «Готовый кофе»: <span>доля чеков и месячный товарооборот</span></div>
                    <div class="coffee-card-sub">28 месяцев (янв 2024 — апр 2026); чеки с картой лояльности, оффлайн, вся сеть</div>
                </div>
                <div class="coffee-chart-wrap" style="height:320px"><canvas id="coffee-dyn"></canvas></div>
                <div class="coffee-card-footer">
                    Источник: программа лояльности · оффлайн · 28 месяцев (янв 2024 — апр 2026) · вся сеть
                </div>
                <div class="coffee-ai-conclusion">
                    <span class="coffee-ai-conclusion-label">Вывод (ИИ)</span>
                    Кофе перешёл из нишевой миссии в массовую: с августа 2025 темп роста доли чеков ускорился в 3–4 раза.
                </div>
            </div>

            <!-- BLOCK 1.5: SHOP EXPANSION (перенесён сразу после DYNAMICS) -->
            <div class="coffee-card">
                <div class="coffee-card-head">
                    <div class="coffee-action-title">Декомпозиция роста доли чеков: <span>сеть магазинов с продажами кофе × продажи кофе на магазин</span></div>
                    <div class="coffee-card-sub">Рост доли чеков с кофе ×25 = (×5.6) сеть магазинов с продажами кофе × (×4.5) продажи кофе на магазин</div>
                </div>
                <div class="coffee-chart-wrap" style="height:300px"><canvas id="coffee-shops"></canvas></div>
                <div id="coffee-shop-decomp" class="coffee-decomp-grid"></div>
                <div class="coffee-ai-conclusion">
                    <span class="coffee-ai-conclusion-label">Вывод (ИИ)</span>
                    Рост ×25 = расширение сети магазинов с продажами кофе (×5.6) × рост продаж кофе на магазин (×4.5).
                </div>
            </div>

            <!-- BLOCK 2: MISSION AFFINITY -->
            <div class="coffee-card">
                <div class="coffee-card-head">
                    <div class="coffee-action-title">Какие миссии встречаются с кофе чаще/реже среднего (апр'26)</div>
                    <div class="coffee-card-sub">×N = доля чеков с миссией среди кофе-чеков ÷ доля в общей выборке (во сколько раз чаще). ×&gt;1 = миссия встречается с кофе чаще среднего, ×&lt;1 = реже</div>
                </div>
                <div class="coffee-2col">
                    <div>
                        <div class="coffee-bar-title coffee-bar-title-pos">↑ Покупают вместе чаще среднего (×&gt;1)</div>
                        <div id="coffee-mission-aff-up" class="coffee-bars"></div>
                    </div>
                    <div>
                        <div class="coffee-bar-title coffee-bar-title-neg">↓ Покупают вместе реже среднего (×&lt;0.3)</div>
                        <div id="coffee-mission-aff-dn" class="coffee-bars"></div>
                    </div>
                </div>
                <div class="coffee-ai-conclusion">
                    <span class="coffee-ai-conclusion-label">Вывод (ИИ)</span>
                    Кофе чаще всего покупают с готовой едой (×4.9) и завтраком (×4.2).
                </div>
            </div>

            <!-- BLOCK 3: PRODUCT AFFINITY (grouped by category, collapsible) -->
            <div class="coffee-card coffee-collapsible" id="coffee-prod-card">
                <div class="coffee-card-head coffee-collapsible-head" id="coffee-prod-toggle">
                    <div class="coffee-collapsible-titlebox">
                        <div class="coffee-action-title">Что покупают с кофе — <span>группы товаров по категориям</span></div>
                        <div class="coffee-card-sub">Полный каталог из 200 позиций, которые встречаются в чеках с кофе чаще среднего по всем чекам апр'26 (×&gt;1 = во сколько раз чаще), сгруппирован по группе категории. В каждой группе — ВСЕ продукты, отсортированы по кратности ×N</div>
                    </div>
                    <button class="coffee-collapsible-chevron" type="button" aria-label="Развернуть/свернуть">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
                    </button>
                </div>
                <div id="coffee-prod-groups" class="coffee-prod-groups coffee-collapsible-body"></div>
            </div>

            <!-- BLOCK 4: CONTACT PROFILE -->
            <div class="coffee-card">
                <div class="coffee-card-head">
                    <div class="coffee-action-title">Профиль клиентов миссии «Готовый кофе» vs все клиенты лояльной базы (апр'26)</div>
                    <div class="coffee-card-sub">94 332 клиента, купивших кофе, vs 5.45 млн клиентов базы. Ценовой индекс (ЦИ) и лояльность — среднее по чекам клиента в апр'26 (offline), агрегировано по клиенту</div>
                </div>
                <div id="coffee-profile" class="coffee-profile-grid"></div>
                <div class="coffee-ai-conclusion">
                    <span class="coffee-ai-conclusion-label">Вывод (ИИ)</span>
                    Клиент с кофе чуть дороже по корзине (ЦИ +12 % к базе), но с импульсными мини-визитами (лояльность −14 %).
                </div>
            </div>

            <!-- BLOCK 5: TIMING -->
            <div class="coffee-card">
                <div class="coffee-card-head">
                    <div class="coffee-action-title">Распределение чеков с кофе по дням недели и типу дня (апр'26)</div>
                    <div class="coffee-card-sub">×N = во сколько раз доля кофе-чеков в данный день/тип дня выше/ниже доли всех чеков в этот же день. ×&gt;1 = кофе покупают чаще среднего</div>
                </div>
                <div class="coffee-2col">
                    <div>
                        <div class="coffee-bar-title">По дням недели · во сколько раз чаще среднего (×N)</div>
                        <div class="coffee-chart-wrap" style="height:240px"><canvas id="coffee-dayweek"></canvas></div>
                    </div>
                    <div>
                        <div class="coffee-bar-title">Будни vs выходные · во сколько раз чаще среднего (×N)</div>
                        <div class="coffee-chart-wrap" style="height:240px"><canvas id="coffee-daytype"></canvas></div>
                    </div>
                </div>
                <div class="coffee-ai-conclusion">
                    <span class="coffee-ai-conclusion-label">Вывод (ИИ)</span>
                    Кофе — будний паттерн «по дороге»: пик во вторник-четверг, провал в субботу-воскресенье.
                </div>
            </div>

            <!-- BLOCK 6: FREQUENCY -->
            <div class="coffee-card">
                <div class="coffee-card-head">
                    <div class="coffee-action-title">Распределение клиентов миссии по частоте покупки и доли чеков миссии (апр'26)</div>
                    <div class="coffee-card-sub">94 332 клиента сгруппированы по числу покупок кофе за месяц</div>
                </div>
                <div class="coffee-2col">
                    <div class="coffee-chart-wrap" style="height:280px"><canvas id="coffee-freq-contacts"></canvas></div>
                    <div class="coffee-chart-wrap" style="height:280px"><canvas id="coffee-freq-checks"></canvas></div>
                </div>
                <div class="coffee-ai-conclusion">
                    <span class="coffee-ai-conclusion-label">Вывод (ИИ)</span>
                    3 % клиентов («почти каждый день») приносят 27 % всех чеков миссии; 59 % купили кофе только один раз за месяц.
                </div>
            </div>

            <!-- BLOCK 6.6: КОФЕ-ПОКУПАТЕЛИ × СЕГМЕНТ (окно 6 нед) -->
            <div class="coffee-card coffee-collapsible" id="coffee-firstseg-card">
                <div class="coffee-card-head coffee-collapsible-head" id="coffee-firstseg-toggle">
                    <div class="coffee-collapsible-titlebox">
                        <div class="coffee-action-title">Кофе-покупатели × сегмент клиента на конец месяца (окно 6 недель)</div>
                        <div class="coffee-card-sub">Когорта = клиенты с покупкой готового кофе в последние 6 недель до конца месяца. Лифт = доля сегмента среди кофе-покупателей ÷ доля сегмента среди <b>активной части базы</b> (Новые + Активные) — Отток исключён, т.к. по построению не может попасть в кофе-когорту</div>
                    </div>
                    <button class="coffee-collapsible-chevron" type="button" aria-label="Развернуть/свернуть">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
                    </button>
                </div>
                <div class="coffee-collapsible-body">
                    <div class="coffee-bar-title">Помесячная динамика лифта по сегментам (×N к доле сегмента в общей базе)</div>
                    <div class="coffee-chart-wrap" style="height:300px"><canvas id="coffee-firstseg-lift-lines"></canvas></div>

                    <div class="coffee-bar-title" style="margin-top:18px">Число кофе-покупателей помесячно с разбиением по сегменту</div>
                    <div class="coffee-chart-wrap" style="height:280px"><canvas id="coffee-firstseg-counts"></canvas></div>
                </div>
            </div>

            <!-- BLOCK 6.65: РЕГИОНЫ И КАНАЛЫ -->
            <div class="coffee-card coffee-collapsible" id="coffee-regionchan-card">
                <div class="coffee-card-head coffee-collapsible-head" id="coffee-regionchan-toggle">
                    <div class="coffee-collapsible-titlebox">
                        <div class="coffee-action-title">Проникновение готового кофе: по регионам и по профилю клиента</div>
                        <div class="coffee-card-sub">Доля чеков с кофе = % всех оффлайн-чеков региона / профиля, содержащих готовый кофе. Профиль клиента — где клиент обычно покупает (только оффлайн / омни)</div>
                    </div>
                    <button class="coffee-collapsible-chevron" type="button" aria-label="Развернуть/свернуть">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
                    </button>
                </div>
                <div class="coffee-collapsible-body">
                    <div class="coffee-2col">
                        <div>
                            <div class="coffee-bar-title">Доля чеков с кофе по регионам</div>
                            <div class="coffee-chart-wrap" style="height:260px"><canvas id="coffee-region-share"></canvas></div>
                        </div>
                        <div>
                            <div class="coffee-bar-title">ТО готового кофе по регионам, млн ₽/мес</div>
                            <div class="coffee-chart-wrap" style="height:260px"><canvas id="coffee-region-to"></canvas></div>
                        </div>
                    </div>
                    <div id="coffee-region-kpi" class="coffee-decomp-grid" style="margin-top:14px"></div>

                    <div class="coffee-bar-title" style="margin-top:22px">Доля чеков с кофе по профилю клиента (только оффлайн vs омни)</div>
                    <div class="coffee-card-sub" style="margin-bottom:8px">Показывает, клиенты какого профиля чаще берут готовый кофе в оффлайн-точке</div>
                    <div class="coffee-chart-wrap" style="height:240px"><canvas id="coffee-channel-share"></canvas></div>
                    <div id="coffee-channel-kpi" class="coffee-decomp-grid" style="margin-top:14px"></div>
                </div>
            </div>

            <!-- BLOCK 6.7: BASKET COMPOSITION (свёрнут по умолчанию) -->
            <div class="coffee-card coffee-collapsible is-collapsed" id="coffee-basket-card">
                <div class="coffee-card-head coffee-collapsible-head" id="coffee-basket-toggle">
                    <div class="coffee-collapsible-titlebox">
                        <div class="coffee-action-title">Состав чека с кофе: размер корзины и количество позиций кофе (апр'26)</div>
                        <div class="coffee-card-sub">Распределение чеков с кофе по числу SKU в чеке и по числу кофейных позиций</div>
                    </div>
                    <button class="coffee-collapsible-chevron" type="button" aria-label="Развернуть/свернуть">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>
                    </button>
                </div>
                <div class="coffee-collapsible-body">
                    <div class="coffee-2col">
                        <div>
                            <div class="coffee-bar-title">Размер корзины (всего SKU в чеке с кофе)</div>
                            <div id="coffee-basket-size" class="coffee-bars"></div>
                        </div>
                        <div>
                            <div class="coffee-bar-title">Сколько кофе в одном чеке</div>
                            <div id="coffee-cup-per-check" class="coffee-bars"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- BLOCK 6.5: COHORT UPSELL (по сумме чека) -->
            <div class="coffee-card">
                <div class="coffee-card-head">
                    <div class="coffee-action-title">Сравнение когорт кофе-чеков по сумме: <span>≤500 ₽ (мини) vs 501–800 ₽ (средний по сети)</span> — во сколько раз чаще встречаются категории и продукты</div>
                    <div class="coffee-card-sub">Доля = % чеков когорты, содержащих категорию/продукт. Дельта = доля в средней − доля в мини. ×N = доля в средней ÷ доля в мини (во сколько раз чаще в среднем чеке)</div>
                </div>
                <div id="coffee-cohort-strip"></div>
                <div class="coffee-3col">
                    <div>
                        <div class="coffee-bar-title">Топ-10 миссий · во сколько раз чаще (мини → ~средний)</div>
                        <div id="coffee-upsell-mission" class="coffee-bars"></div>
                    </div>
                    <div>
                        <div class="coffee-bar-title">Топ-10 категорий · во сколько раз чаще (мини → ~средний)</div>
                        <div id="coffee-upsell-cat" class="coffee-bars"></div>
                    </div>
                    <div>
                        <div class="coffee-bar-title">Топ-10 продуктов · во сколько раз чаще (мини → ~средний)</div>
                        <div id="coffee-upsell-prod" class="coffee-bars"></div>
                    </div>
                </div>
                <div class="coffee-ai-conclusion">
                    <span class="coffee-ai-conclusion-label">Вывод (ИИ)</span>
                    Сильнее всего при переходе от мини- к средней когорте растут доли миссий «Ежедневное» (+42 п.п.), «К чаю» и «Сладкий перекус». Но это <strong>не сигнал «кофе ↔ ежедневная закупка»</strong> — это другой паттерн: <strong>кофе добавляется к обычному походу в магазин</strong>, а не наоборот. Мини-чек (84% кофе-визитов) = чистый импульсный кофе на вынос. Средний и большой = «я зашёл за продуктами, заодно взял кофе». Бандлы «кофе + молочка / хлеб» работать не будут — клиент в этих чеках всё равно идёт за полноценной закупкой, а кофе для него попутный.
                </div>
            </div>

            <!-- BLOCK 7: SEGMENT GOALS & PROMOTIONS (driven by methodology) -->
            <div class="coffee-card coffee-implications">
                <div class="coffee-card-head">
                    <div class="coffee-action-title coffee-implications-title">Цели и предлагаемые акции по сегментам миссии</div>
                </div>
                <div id="coffee-goals-grid"></div>
            </div>
        `;

        renderCoffeeDynamics(dyn);
        renderCoffeeShops(dyn);
        renderCoffeeMissionAffinity(d.mission_affinity);
        renderCoffeeProductAffinity(d.product_affinity);
        renderCoffeeProfile(d.contact_profile, last);
        renderCoffeeDayweek(d.dayweek);
        renderCoffeeDaytype(d.daytype);
        renderCoffeeFrequency(d.frequency);
        renderCoffeeFirstSegment(d);
        renderCoffeeRegionChannel(d);
        renderCoffeeBasketComposition(d);
        renderCoffeeCohorts(d);
        renderCoffeeGoals(d);

        // Toggle для свёртываемых блоков
        ['coffee-prod', 'coffee-basket', 'coffee-firstseg', 'coffee-regionchan'].forEach(id => {
            const toggle = document.getElementById(id + '-toggle');
            const card   = document.getElementById(id + '-card');
            if (toggle && card) {
                toggle.addEventListener('click', () => card.classList.toggle('is-collapsed'));
            }
        });
    }
    window.buildCoffee = buildCoffee;

    /* ----------------- BLOCK 1: DYNAMICS (line + bar) ----------------- */
    function renderCoffeeDynamics(dyn) {
        const ctx = document.getElementById('coffee-dyn');
        if (!ctx) return;
        destroyChart('coffee-dyn');
        const labels = dyn.map(r => YM_LABEL(r.YEAR_MONTH));
        const shareData = dyn.map(r => +(r.SHARE_CHECKS * 100).toFixed(3)); // %
        const toData    = dyn.map(r => +(r.TO_COFFEE / 1e6).toFixed(1));   // млн руб

        chartInstances['coffee-dyn'] = new Chart(ctx, {
            plugins: [ChartDataLabels],
            data: {
                labels,
                datasets: [
                    {
                        type: 'bar',
                        label: 'ТО миссии, млн ₽',
                        data: toData,
                        backgroundColor: COFFEE_BG_SOFT,
                        borderColor: 'rgba(111,78,55,0.35)',
                        borderWidth: 1,
                        borderRadius: 3,
                        yAxisID: 'y2',
                        order: 2,
                        datalabels: {
                            display: true,
                            anchor: 'end',
                            align: 'end',
                            clip: false,
                            color: '#6F4E37',
                            font: { size: 9, weight: 700 },
                            formatter: (v, ctx) => {
                                const n = ctx.dataset.data.length, last = n - 1;
                                const step = Math.max(1, Math.round(n / 6));
                                return (ctx.dataIndex % step === 0 || ctx.dataIndex === last) && v ? v.toLocaleString('ru-RU') : '';
                            },
                        },
                    },
                    {
                        type: 'line',
                        label: 'Доля чеков с кофе, %',
                        data: shareData,
                        borderColor: COFFEE_ORANGE,
                        backgroundColor: COFFEE_ORANGE,
                        borderWidth: 2.5,
                        pointRadius: 3,
                        pointHoverRadius: 5,
                        tension: 0.35,
                        yAxisID: 'y1',
                        order: 1,
                        datalabels: {
                            display: true,
                            // линию-подписи уводим ПОД точку, чтобы не наезжали на подписи ТО-баров сверху
                            align: 'bottom',
                            offset: 6,
                            color: COFFEE_ORANGE,
                            font: { size: 9, weight: 700 },
                            formatter: (v, ctx) => {
                                const n = ctx.dataset.data.length, last = n - 1;
                                const step = Math.max(1, Math.round(n / 6));
                                return (ctx.dataIndex % step === 0 || ctx.dataIndex === last) ? v.toFixed(2) + '%' : '';
                            }
                        }
                    }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode:'index', intersect:false },
                layout: { padding: { top: 28, bottom: 18 } },
                plugins: {
                    legend: { position:'top', align:'end', labels:{ usePointStyle:true } },
                    tooltip: {
                        callbacks: {
                            label: (c) => {
                                if (c.dataset.yAxisID === 'y1') return ` Доля чеков: ${c.parsed.y.toFixed(3)}%`;
                                return ` ТО: ${c.parsed.y.toLocaleString('ru-RU')} млн ₽`;
                            }
                        }
                    }
                },
                scales: {
                    x: { ticks:{ maxRotation:0, autoSkip:true, font:{size:10} }, grid:{display:false}, border:{display:false} },
                    y1: { position:'left', display: false },
                    y2: { position:'right', display: false },
                }
            }
        });
    }

    /* ----------------- BLOCK 2: MISSION AFFINITY (h-bars) ----------------- */
    function renderCoffeeMissionAffinity(ma) {
        const filtered = ma.filter(m => m.N_IN_COFFEE >= 200);
        // top up (lift > 1) — 8 шт
        const up = filtered.filter(m => m.AFFINITY_LIFT >= 1).sort((a,b)=>b.AFFINITY_LIFT-a.AFFINITY_LIFT).slice(0,8);
        // bottom (lift < 0.3) — 8 шт самых низких среди достаточно частых
        const dn = filtered.filter(m => m.AFFINITY_LIFT < 0.3).sort((a,b)=>a.AFFINITY_LIFT-b.AFFINITY_LIFT).slice(0,8);

        const maxUp = Math.max(...up.map(m => m.AFFINITY_LIFT));
        const elUp = document.getElementById('coffee-mission-aff-up');
        const elDn = document.getElementById('coffee-mission-aff-dn');
        if (!elUp || !elDn) return;

        elUp.innerHTML = up.map(m => {
            const w = (m.AFFINITY_LIFT / maxUp) * 100;
            return `
                <div class="coffee-bar-row">
                    <div class="coffee-bar-name">${m.MISSION}</div>
                    <div class="coffee-bar-track">
                        <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${COFFEE_ORANGE}"></div>
                    </div>
                    <div class="coffee-bar-val coffee-bar-val-pos">×${m.AFFINITY_LIFT.toFixed(2)}</div>
                </div>
            `;
        }).join('');

        const maxDn = 1.0;
        elDn.innerHTML = dn.map(m => {
            const w = (m.AFFINITY_LIFT / maxDn) * 100;
            return `
                <div class="coffee-bar-row">
                    <div class="coffee-bar-name">${m.MISSION}</div>
                    <div class="coffee-bar-track">
                        <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${COFFEE_GRAY}"></div>
                    </div>
                    <div class="coffee-bar-val coffee-bar-val-neg">×${m.AFFINITY_LIFT.toFixed(2)}</div>
                </div>
            `;
        }).join('');
    }

    /* ----------------- BLOCK 3: PRODUCT AFFINITY (grouped) ----------------- */
    // Маппинг продукт → бизнес-группа. Используем I_CATEGORY_5 (точнее),
    // а I_CATEGORY_4 — только для бакетов, где cat5 не разводит подгруппы.
    // ВАЖНО: ПЕРЕКУСЫ (cat4) содержит и шоколадные батончики, и сэндвичи —
    // поэтому cat4-маппинг для ПЕРЕКУСЫ нельзя, нужен cat5.
    const COFFEE_PROD_GROUPS = [
        { key: 'COFFEE',         label: 'Сам кофе (горячие напитки)', color: COFFEE_BROWN,
          cat5: ['ГОРЯЧИЕ НАПИТКИ'] },
        { key: 'BAKERY_SWEET',   label: 'Сладкая выпечка и пончики',  color: '#E07A5F',
          cat5: ['ИЗДЕЛИЯ СЛАДКИЕ СП','ИЗДЕЛИЯ СЛАДКИЕ ДЕФРОСТ',
                 'МЕЛКОШТУЧНЫЕ ИЗДЕЛИЯ Х/Б ИНД','ИЗДЕЛИЯ МЕЛКОШТУЧНЫЕ Х/Б СП',
                 'ПИРОЖНЫЕ ИНД','ДОНАТЫ, ПОНЧИКИ, МАФФИНЫ ДЕФРОСТ',
                 'ХБИ ДЕФРОСТ БЕЗ УПАК'] },
        { key: 'SAVOURY_BAKERY', label: 'Сытная выпечка и сэндвичи',  color: '#3D6F8F',
          cat5: ['ИЗДЕЛИЯ СЫТНЫЕ СП','СЭНДВИЧ РОЛЛ,БУРГЕР,ШАУРМА',
                 'СЭНДВИЧИ','БУТЕРБРОДЫ, СЭНДВИЧИ, РОЛЛЫ СП',
                 'ПИЦЦА, ПИРОГИ, ХАЧАПУРИ СП','ХЛЕБ СП'] },
        { key: 'READY_MEAL',     label: 'Готовые блюда, салаты, суши',color: '#2E8B57',
          cat5: ['ОСНОВНЫЕ БЛЮДА','ОСНОВНЫЕ  БЛЮДА','САЛАТЫ ОВОЩНЫЕ',
                 'СУШИ,РОЛЛЫ,ОНИГИРИ','МОНО-БЛЮДА','БЛЮДА С ПТИЦЕЙ',
                 'БЛЮДА ТВОРОЖНЫЕ','БЛЮДА ЯЙЧНЫЕ','КАШИ',
                 'САЛАТЫ, ЗАКУСКИ','САЛАТЫ, СОЛЕНЬЯ СП',
                 'ГОТОВЫЕ БЛЮДА И КУЛИНАРИЯ СП','ГОТОВЫЕ БЛЮДА И КУЛИНАРИЯ ИНД'] },
        { key: 'CHOC',           label: 'Шоколад и шоколадные батончики', color: '#5F259F',
          cat5: ['ШОКОЛАДНЫЕ БАТОНЧИКИ (П)','ШОКОЛАД (П)','ШОКОЛАД'] },
        { key: 'DIET',           label: 'Диетические батончики',      color: '#00897B',
          cat5: ['БАТОНЧИКИ ДИЕТИЧЕСКИЕ','ДИЕТИКА'] },
        { key: 'BREAKFAST',      label: 'Завтрак / готовые завтраки', color: '#FFB74D',
          cat4: ['ЗАВТРАК','ГОТОВЫЕ ЗАВТРАКИ СП'] },
        { key: 'TOBACCO',        label: 'Сигареты',                   color: '#6B7B8D',
          cat5: ['СИГАРЕТЫ'] },
        { key: 'OTHER',          label: 'Прочее',                     color: '#94A3B8',
          cat4: ['ПОСУДА ОДНОРАЗОВАЯ','ВОДА СТОЛОВАЯ'],
          cat5: ['СТОЛОВЫЕ ПРИБОРЫ ОДНОРАЗОВАЯ ПОСУДА'] },
    ];

    function groupKeyForProd(p) {
        const cat4 = (p.I_CATEGORY_4 || '').trim();
        const cat5 = (p.I_CATEGORY_5 || '').trim();
        // 1) сначала пытаемся по cat5 (точнее)
        for (const g of COFFEE_PROD_GROUPS) {
            if (g.cat5 && g.cat5.includes(cat5)) return g.key;
        }
        // 2) потом по cat4 (для групп где cat5-сплита нет)
        for (const g of COFFEE_PROD_GROUPS) {
            if (g.cat4 && g.cat4.includes(cat4)) return g.key;
        }
        return 'OTHER';
    }

    function renderCoffeeProductAffinity(pa) {
        const el = document.getElementById('coffee-prod-groups');
        if (!el) return;

        // Группируем все продукты с lift > 1 по нашим бизнес-группам.
        // Пакеты и SKU лояльности — исключаем правилом проекта.
        const grouped = {};
        const groupTotals = {};
        for (const p of pa) {
            if (!p.AFFINITY_LIFT || p.AFFINITY_LIFT < 1) continue;
            if (isBagOrLoyaltySku(p)) continue;
            const k = groupKeyForProd(p);
            (grouped[k] = grouped[k] || []).push(p);
            groupTotals[k] = (groupTotals[k] || 0) + (p.N_IN_COFFEE || 0);
        }

        // Сортируем группы по суммарному N в чеках с кофе (от больших к меньшим)
        const orderedGroups = COFFEE_PROD_GROUPS
            .filter(g => grouped[g.key] && grouped[g.key].length > 0)
            .sort((a, b) => (groupTotals[b.key] || 0) - (groupTotals[a.key] || 0));

        el.innerHTML = orderedGroups.map(g => {
            const items = grouped[g.key]
                .sort((a, b) => b.AFFINITY_LIFT - a.AFFINITY_LIFT);   // ВСЕ продукты группы (без лимита)
            const maxLift = Math.max(...items.map(p => p.AFFINITY_LIFT));
            const groupShareInCoffee = items.reduce((s,p) => s + (p.SHARE_IN_COFFEE || 0), 0);
            // LIFT по группе — средневзвешенный по N_IN_COFFEE
            // (= ∑ LIFT × N_IN_COFFEE / ∑ N_IN_COFFEE)
            const totalN = items.reduce((s,p) => s + (p.N_IN_COFFEE || 0), 0);
            const groupLift = totalN > 0
                ? items.reduce((s,p) => s + (p.AFFINITY_LIFT || 0) * (p.N_IN_COFFEE || 0), 0) / totalN
                : null;

            return `
                <div class="coffee-prod-group" style="--gcolor:${g.color}">
                    <div class="coffee-prod-group-head">
                        <span class="coffee-prod-group-dot"></span>
                        <span class="coffee-prod-group-label">${g.label}</span>
                        <span class="coffee-prod-group-lift" title="во сколько раз чаще встречается в чеках с кофе, чем в среднем по сети">×${groupLift != null ? groupLift.toFixed(1) : '—'} чаще</span>
                        <span class="coffee-prod-group-meta">${items.length} SKU · доля в чеках кофе: ${(groupShareInCoffee*100).toFixed(1)}%</span>
                    </div>
                    <div class="coffee-bars">
                        ${items.map(p => {
                            const w = (p.AFFINITY_LIFT / maxLift) * 100;
                            return `
                                <div class="coffee-bar-row">
                                    <div class="coffee-bar-name" title="${cleanProdName(p.PRODUCT_NAME)}">${cleanProdName(p.PRODUCT_NAME)}</div>
                                    <div class="coffee-bar-track">
                                        <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${g.color}"></div>
                                    </div>
                                    <div class="coffee-bar-val coffee-bar-val-pos">×${p.AFFINITY_LIFT.toFixed(1)}</div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            `;
        }).join('');
    }
    function cleanProdName(name) {
        return (name || '').replace(/\s+/g,' ').trim();
    }

    // Фильтр пакетов / SKU лояльности / служебных категорий и миссий (правило проекта).
    // Применяется ко всем спискам перед рендером в дашборде (см. memory:
    // feedback_exclude_bags_loyalty).
    const BAG_LOYALTY_PATTERNS = /ПАКЕТ|МАЙКА|ФАСОВ|ПОЛИЭТИЛЕН|АКЦИ|ПОДАРОК|БОНУС|КУПОН|ПРИЗ|СПИСАН|НАЧИСЛЕН|ТОВАР.*ЗА ПОКУПКУ|СЛУЖЕБ|N\/?A|МЕППИНГ/i;
    function isBagOrLoyaltySku(p) {
        if (!p) return false;
        const fields = [
            p.PRODUCT_NAME, p.I_CATEGORY_5, p.CATEGORY,
            p.MISSION, p.NAME, p.LABEL, p.NAME_RU
        ];
        for (const f of fields) {
            if (f && BAG_LOYALTY_PATTERNS.test(String(f).toUpperCase())) return true;
        }
        return false;
    }

    /* ----------------- BLOCK 4: CONTACT PROFILE ----------------- */
    function renderCoffeeProfile(cp, last) {
        const el = document.getElementById('coffee-profile');
        if (!el) return;
        const items = [
            {
                label: 'Покупатели кофе · апр\'26',
                value: fmtInt(cp.N_CONTACTS_COFFEE),
                sub:   `${fmtPct(cp.SHARE_CONTACTS_COFFEE, 2)} всей клиентской базы (${fmtInt(cp.N_CONTACTS_ALL)} контактов)`,
                accent: COFFEE_ORANGE,
            },
            {
                label: 'Ценовой индекс кофе-клиента vs база · апр\'26',
                value: fmtLift(cp.PRICE_INDEX_TO_ALL),
                sub:   `×N = во сколько раз ценовой индекс кофе-клиента выше/ниже базы. ${(cp.AVG_PRICE_INDEX_COFFEE*100).toFixed(1)}% vs ${(cp.AVG_PRICE_INDEX_ALL*100).toFixed(1)}% базы (${fmtPctS(cp.PRICE_INDEX_TO_ALL)}). Ценовой индекс клиента = средний ценовой индекс всех его чеков апр'26 (offline). Корзина кофе-клиента чуть «дороже», но это не премиум-сегмент`,
                accent: COFFEE_PURPLE,
            },
            {
                label: 'Лояльность кофе-клиента vs база · апр\'26',
                value: fmtLift(cp.PENETRATION_CHECK_TO_ALL),
                sub:   `×N = во сколько раз лояльность кофе-клиента выше/ниже базы. ${(cp.AVG_PENETRATION_CHECK_COFFEE*100).toFixed(2)}% vs ${(cp.AVG_PENETRATION_CHECK_ALL*100).toFixed(2)}% базы (${fmtPctS(cp.PENETRATION_CHECK_TO_ALL)}). Лояльность ниже из-за импульсных кофе-визитов (узкая корзина), а не из-за «худшего» клиента`,
                accent: COFFEE_BROWN,
            },
            {
                label: 'Средний чек кофе-визита · апр\'26',
                value: `${Math.round(last.AVG_CHECK_COFFEE)} ₽`,
                sub:   `${Math.round((1 + (last.AVG_CHECK_TO_ALL || 0)) * 100)}% от среднего чека базы (${Math.round(last.AVG_CHECK_ALL)} ₽). Кофе-чек = короткий impulse-визит, не закупка`,
                accent: COFFEE_GRAY,
            },
        ];
        el.innerHTML = items.map(it => `
            <div class="coffee-profile-card" style="--accent:${it.accent}">
                <div class="coffee-profile-label">${it.label}</div>
                <div class="coffee-profile-value">${it.value}</div>
                <div class="coffee-profile-sub">${it.sub}</div>
            </div>
        `).join('');
    }

    /* ----------------- BLOCK 3.5: SHOP EXPANSION ----------------- */
    function renderCoffeeShops(dyn) {
        const ctx = document.getElementById('coffee-shops');
        if (!ctx) return;
        destroyChart('coffee-shops');

        const labels = dyn.map(r => YM_LABEL(r.YEAR_MONTH));
        const sharesShops  = dyn.map(r => +((r.SHARE_SHOPS_COFFEE || 0) * 100).toFixed(1));   // % сети с витриной
        const checksPerShop = dyn.map(r => r.N_SHOPS_COFFEE ? (r.N_CHECKS_COFFEE / r.N_SHOPS_COFFEE) : 0);
        const cpsRounded   = checksPerShop.map(v => Math.round(v));

        chartInstances['coffee-shops'] = new Chart(ctx, {
            plugins: [ChartDataLabels],
            data: {
                labels,
                datasets: [
                    {
                        type: 'bar',
                        label: 'Магазины с продажами кофе, % сети',
                        data: sharesShops,
                        backgroundColor: 'rgba(255,121,0,0.20)',
                        borderColor: 'rgba(255,121,0,0.55)',
                        borderWidth: 1,
                        borderRadius: 3,
                        yAxisID: 'y1',
                        order: 2,
                        datalabels: {
                            display: true,
                            anchor: 'end',
                            align: 'end',
                            clip: false,
                            color: COFFEE_ORANGE,
                            font: { size: 9, weight: 700 },
                            formatter: (v, ctx) => {
                                const n = ctx.dataset.data.length, last = n - 1;
                                const step = Math.max(1, Math.round(n / 6));
                                return (ctx.dataIndex % step === 0 || ctx.dataIndex === last) ? v.toFixed(0) + '%' : '';
                            }
                        }
                    },
                    {
                        type: 'line',
                        label: 'Продажи кофе на магазин, чеков/мес',
                        data: cpsRounded,
                        borderColor: COFFEE_BROWN,
                        backgroundColor: COFFEE_BROWN,
                        borderWidth: 2.5,
                        pointRadius: 3,
                        pointHoverRadius: 5,
                        tension: 0.35,
                        yAxisID: 'y2',
                        order: 1,
                        datalabels: {
                            display: true,
                            // линию-подписи уводим ПОД точку, чтобы не наезжали на «%» баров сверху
                            align: 'bottom',
                            offset: 6,
                            color: COFFEE_BROWN,
                            font: { size: 9, weight: 700 },
                            formatter: (v, ctx) => {
                                const n = ctx.dataset.data.length, last = n - 1;
                                const step = Math.max(1, Math.round(n / 6));
                                return (ctx.dataIndex % step === 0 || ctx.dataIndex === last) ? fmtInt(v) : '';
                            }
                        }
                    }
                ]
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode:'index', intersect:false },
                layout: { padding: { top: 28, bottom: 18 } },
                plugins: {
                    legend: { position:'top', align:'end' },
                    tooltip: {
                        callbacks: {
                            label: (c) => {
                                if (c.dataset.yAxisID === 'y1') return ` Магазинов с витриной: ${c.parsed.y.toFixed(1)}%`;
                                return ` Чеков на магазин: ${c.parsed.y.toLocaleString('ru-RU')}`;
                            }
                        }
                    }
                },
                scales: {
                    x: { ticks:{ maxRotation:0, autoSkip:true, autoSkipPadding:12, font:{size:10} }, grid:{display:false}, border:{display:false} },
                    y1: { position:'left', display: false, grace: '12%' },
                    y2: { position:'right', display: false, grace: '15%' },
                }
            }
        });

        // Decomposition cards below chart
        const first = dyn[0];
        const last  = dyn[dyn.length - 1];
        const shopsGrowth   = last.SHARE_SHOPS_COFFEE / first.SHARE_SHOPS_COFFEE;
        const intensityFirst = first.N_CHECKS_COFFEE / first.N_SHOPS_COFFEE;
        const intensityLast  = last.N_CHECKS_COFFEE  / last.N_SHOPS_COFFEE;
        const intensityGrowth = intensityLast / intensityFirst;
        const checkShareGrowth = last.SHARE_CHECKS / first.SHARE_CHECKS;

        const el = document.getElementById('coffee-shop-decomp');
        if (!el) return;
        const items = [
            {
                label: 'Магазины с продажами кофе, % сети',
                v_from: (first.SHARE_SHOPS_COFFEE*100).toFixed(1)+'%',
                v_to:   (last.SHARE_SHOPS_COFFEE*100).toFixed(1)+'%',
                growth: '×'+shopsGrowth.toFixed(1),
                sub:    `${fmtInt(first.N_SHOPS_COFFEE)} → ${fmtInt(last.N_SHOPS_COFFEE)} магазинов`,
                accent: COFFEE_ORANGE,
            },
            {
                label: 'Продажи кофе на магазин, чеков/мес',
                v_from: fmtInt(intensityFirst),
                v_to:   fmtInt(intensityLast),
                growth: '×'+intensityGrowth.toFixed(1),
                sub:    'число чеков с кофе в среднем на 1 магазин с продажами кофе',
                accent: COFFEE_BROWN,
            },
            {
                label: 'Доля чеков с кофе',
                v_from: (first.SHARE_CHECKS*100).toFixed(3)+'%',
                v_to:   (last.SHARE_CHECKS*100).toFixed(2)+'%',
                growth: '×'+checkShareGrowth.toFixed(0),
                sub:    `сеть магазинов с продажами кофе × продажи кофе на магазин = ${shopsGrowth.toFixed(1)} × ${intensityGrowth.toFixed(1)}`,
                accent: COFFEE_PURPLE,
            },
        ];
        el.innerHTML = items.map(it => `
            <div class="coffee-decomp-card" style="--accent:${it.accent}">
                <div class="coffee-decomp-label">${it.label}</div>
                <div class="coffee-decomp-row">
                    <span class="coffee-decomp-from">${it.v_from}</span>
                    <span class="coffee-decomp-arrow">→</span>
                    <span class="coffee-decomp-to">${it.v_to}</span>
                    <span class="coffee-decomp-growth">${it.growth}</span>
                </div>
                <div class="coffee-decomp-sub">${it.sub}</div>
            </div>
        `).join('');
    }

    /* ----------------- BLOCK 5: TIMING ----------------- */
    function renderCoffeeDayweek(dweek) {
        const ctx = document.getElementById('coffee-dayweek');
        if (!ctx) return;
        destroyChart('coffee-dayweek');
        const labels = dweek.map(r => r.DAY_OF_WEEK.replace(/^\d+ /,''));
        const lifts  = dweek.map(r => r.LIFT);
        const colors = lifts.map(v => v >= 1 ? COFFEE_ORANGE : COFFEE_GRAY);

        chartInstances['coffee-dayweek'] = new Chart(ctx, {
            type: 'bar',
            plugins: [ChartDataLabels],
            data: { labels, datasets: [{ label:'lift', data:lifts, backgroundColor:colors, borderRadius:3, barPercentage:0.7 }] },
            options: {
                responsive:true, maintainAspectRatio:false,
                plugins: {
                    legend:{ display:false },
                    datalabels: {
                        display:true, anchor:'end', align:'end',
                        formatter:(v)=> '×'+v.toFixed(2), font:{ size:10, weight:600 }, color: COFFEE_TXT
                    },
                    tooltip:{ callbacks:{ label:(c)=> ` lift = ×${c.parsed.y.toFixed(3)}` } }
                },
                scales: {
                    x: { grid:{display:false}, border:{display:false}, ticks:{ font:{size:11, weight:600} } },
                    y: { display:false, suggestedMin:0.8, suggestedMax: Math.max(...lifts)*1.15 }
                }
            }
        });
    }
    function renderCoffeeDaytype(dtype) {
        const ctx = document.getElementById('coffee-daytype');
        if (!ctx) return;
        destroyChart('coffee-daytype');
        const valid = dtype.filter(r => r.LIFT && r.LIFT > 0);
        const labels = valid.map(r => r.DAY_TYPE);
        const lifts  = valid.map(r => r.LIFT);
        const colors = lifts.map(v => v >= 1 ? COFFEE_ORANGE : COFFEE_GRAY);

        chartInstances['coffee-daytype'] = new Chart(ctx, {
            type: 'bar',
            plugins: [ChartDataLabels],
            data: { labels, datasets:[{ data: lifts, backgroundColor: colors, borderRadius:3, barPercentage:0.6 }] },
            options: {
                responsive:true, maintainAspectRatio:false,
                plugins: {
                    legend:{ display:false },
                    datalabels: {
                        display:true, anchor:'end', align:'end',
                        formatter:(v)=> '×'+v.toFixed(2), font:{ size:11, weight:600 }, color: COFFEE_TXT
                    },
                    tooltip:{ callbacks:{ label:(c)=> ` lift = ×${c.parsed.y.toFixed(3)}` } }
                },
                scales: {
                    x: { grid:{display:false}, border:{display:false}, ticks:{ font:{size:11, weight:600} } },
                    y: { display:false, suggestedMin:0.8, suggestedMax: Math.max(...lifts)*1.15 }
                }
            }
        });
    }

    /* ----------------- BLOCK 6: FREQUENCY ----------------- */
    function renderCoffeeFrequency(freq) {
        // Canonical order
        const order = ['1 — разовая','2-3 — пару раз','4-7 — почти еженедельно','8-15 — часто','16+ — почти каждый день'];
        const sorted = order.map(k => freq.find(f => f.FREQ_BUCKET === k)).filter(Boolean);
        const labels = sorted.map(f => f.FREQ_BUCKET);
        const shareC = sorted.map(f => +(f.SHARE_CONTACTS * 100).toFixed(1));
        const shareCk = sorted.map(f => +(f.SHARE_OF_CHECKS * 100).toFixed(1));

        const drawBar = (id, data, title, color) => {
            const ctx = document.getElementById(id);
            if (!ctx) return;
            destroyChart(id);
            chartInstances[id] = new Chart(ctx, {
                type: 'bar',
                plugins: [ChartDataLabels],
                data: { labels, datasets:[{ label:title, data, backgroundColor: data.map(()=>color), borderRadius:3, barPercentage:0.7 }] },
                options: {
                    indexAxis:'y',
                    responsive:true, maintainAspectRatio:false,
                    plugins: {
                        legend:{ display:false },
                        title: { display:true, text:title, color:COFFEE_TXT, font:{ size:12, weight:600 }, padding:{ bottom:10 } },
                        datalabels: {
                            display:true, anchor:'end', align:'end', clip:false,
                            formatter:(v)=> v.toFixed(1)+'%', font:{ size:11, weight:600 }, color: COFFEE_TXT
                        },
                        tooltip:{ callbacks:{ label:(c)=> ` ${c.parsed.x.toFixed(2)}%` } }
                    },
                    scales: {
                        x: { display:false, suggestedMax: Math.max(...data)*1.18 },
                        y: { grid:{display:false}, border:{display:false}, ticks:{ font:{size:11} } }
                    },
                    layout:{ padding:{ right:40 } }
                }
            });
        };
        drawBar('coffee-freq-contacts', shareC, 'Доля покупателей кофе в каждой группе', COFFEE_BROWN);
        drawBar('coffee-freq-checks',  shareCk, 'Доля чеков миссии, которые делает каждая группа', COFFEE_ORANGE);
    }

    /* ----------------- BLOCK 7: GOALS & PROMOTIONS (methodology-driven) -----
       Источник правил: данные/методика_сегменты_цели_акции.md (Manzana, 5 архетипов).
       Никакой самодеятельности — архетипы / цели / акции берутся 1:1 из каталога.
       --------------------------------------------------------------------- */

    // ── Каталог методики (раздел 5 + 6 = цель + разрешённые акции).
    // Каждая акция размечена:
    //   category_bound — привязана ли к конкретной категории/миссии
    //                    (true = подходит для mission-контекста)
    //   timing — на какой день недели работает (weekday / weekend / any)
    // Для mission-контекста (например, кофе) фильтруем по:
    //   1) category_bound = true (общие basket/cashback-механики НЕ подходят)
    //   2) timing совпадает с фактическим тайминг-паттерном миссии
    //      (если кофе покупают в будни — weekend-акции отсекаются)
    const METHODOLOGY_CATALOG = {
        'ТРАФФИК': {
            color: '#FF7900',
            goal: 'визиты на клиента → +15 % → 3 мес',
            strategy: 'Увеличить частоту визитов через категорийные акции в нужные дни недели',
            promos: [
                { id: 'traf_bonus_cat_week',     название: 'Повышенные баллы за покупку категории на неделю', механика: 'бонус',           category_bound: true,  timing: 'weekday' },
                { id: 'traf_weekend_cat',        название: 'Скидки/баллы на категории трафика в выходные',    механика: 'скидка / бонус',  category_bound: true,  timing: 'weekend' },
                { id: 'traf_gift_points_ndays',  название: 'Дарим X баллов на N дней (общая)',                механика: 'бонус',           category_bound: false, timing: 'any' },
                { id: 'traf_delayed_disc',       название: 'Отложенная скидка на категорию после покупки',    механика: 'скидка',          category_bound: true,  timing: 'any' },
                { id: 'traf_monthly_threshold',  название: 'При покупке категории на X ₽ за месяц — возврат 10 % баллами', механика: 'кэшбэк', category_bound: true,  timing: 'any' },
            ],
        },
        'ЧЕК': {
            color: '#3D6F8F',
            goal: 'средний чек → +10 % → 3 мес',
            strategy: 'Расширить корзину за счёт смежных категорий и порогов покупки',
            promos: [
                { id: 'check_3plus1',            название: 'Купи 3, получи 1 бесплатно в категории X',        механика: 'подарок',         category_bound: true,  timing: 'any' },
                { id: 'check_bonus_cat',         название: 'Дарим X баллов за покупку в категории',           механика: 'бонус',           category_bound: true,  timing: 'any' },
                { id: 'check_disc_cat',          название: 'Скидка за покупку в рекомендуемой категории X',   механика: 'скидка',          category_bound: true,  timing: 'any' },
                { id: 'check_every_x',           название: 'Каждый X-й товар в чеке со скидкой (общая)',      механика: 'скидка',          category_bound: false, timing: 'any' },
                { id: 'check_basket_threshold',  название: 'При покупке от X ₽ — скидка 10 % / X баллов (общая)', механика: 'скидка / бонус', category_bound: false, timing: 'any' },
            ],
        },
        'МАРЖА': {
            color: '#5F259F',
            goal: 'доля high-margin SKU → +5 п.п. → 6 мес',
            strategy: 'Сдвиг внутри категории к более дорогим SKU через эксклюзивные предложения',
            promos: [
                { id: 'margin_excl_offers',      название: 'Эксклюзивные предложения на high-margin SKU категории', механика: 'скидка',     category_bound: true,  timing: 'any' },
                { id: 'margin_premium_cats',     название: 'Продвижение более дорогих позиций категории, не покупаемых клиентом', механика: 'контент / скидка', category_bound: true, timing: 'any' },
                { id: 'margin_closed_sale',      название: 'Закрытые распродажи дорогих продуктов категории',  механика: 'скидка',         category_bound: true,  timing: 'any' },
            ],
        },
        'УДЕРЖАНИЕ': {
            color: '#6F4E37',
            goal: 'churn → −20 % → 6 мес',
            strategy: 'Защита ядра через работу с любимой категорией клиента и персональные привилегии',
            promos: [
                { id: 'ret_next_purchase_20',    название: '20 % на следующую покупку в категории в течение X дней', механика: 'скидка',   category_bound: true,  timing: 'any' },
                { id: 'ret_fav_cat_engage',      название: 'Вовлечение в механику с любимой категорией и товаром',   механика: 'бонус / подарок', category_bound: true, timing: 'any' },
                { id: 'ret_survey',              название: 'Опрос с бонусами за участие (общая)',             механика: 'бонус',           category_bound: false, timing: 'any' },
            ],
        },
        'РЕАКТИВАЦИЯ': {
            color: '#C41E3A',
            goal: '% вернувшихся за 60 дней → +8 п.п. → 2 мес',
            strategy: 'Win-back через скидку или баллы на любимую категорию с ограниченным сроком',
            promos: [
                { id: 'react_disc_fav_cat',      название: 'Скидка на любимую категорию',                     механика: 'скидка',          category_bound: true,  timing: 'any' },
                { id: 'react_gift_points',       название: 'Дарим X баллов на N дней (общая)',                механика: 'бонус',           category_bound: false, timing: 'any' },
                { id: 'react_disc_check_20',     название: 'Скидка 20 % на чек (общая)',                      механика: 'скидка',          category_bound: false, timing: 'any' },
            ],
        },
    };

    // Определяет тайминг-паттерн миссии по D2.DAYTYPE
    // (lift Будни vs Выходные) для пост-фильтра акций по timing.
    function inferMissionTiming(daytype) {
        if (!daytype || !daytype.length) return 'any';
        const wd = daytype.find(d => d.DAY_TYPE === 'Будни');
        const we = daytype.find(d => d.DAY_TYPE === 'Выходные');
        if (!wd || !we || !wd.LIFT || !we.LIFT) return 'any';
        if (wd.LIFT >= 1.02 && we.LIFT < 0.98) return 'weekday';
        if (we.LIFT >= 1.02 && wd.LIFT < 0.98) return 'weekend';
        return 'any';
    }

    // Фильтр акций для mission-контекста: только category-bound +
    // timing совпадает с тайминг-паттерном миссии (или 'any').
    function filterPromosForMission(promos, missionTiming) {
        return promos.filter(p => {
            if (!p.category_bound) return false;
            if (p.timing === 'any') return true;
            return p.timing === missionTiming;
        });
    }

    // ── Классификация frequency-сегмента кофе по правилам методики раздела 4 ──
    // Контекст: миссия «Готовый кофе», апр'26. Все контакты сегмента — активные в апр'26
    // (по построению), значит ни один не попадает в РЕАКТИВАЦИЮ. УДЕРЖАНИЕ применяем
    // к ядру (16+/мес) как защиту от ухода — recency-метрики per-contact в данных
    // нет, поэтому это best-effort. ЧЕК для кофе-миссии менее применим (чек миссии
    // = 1-2 SKU по построению, основной апсайд — частота).
    function classifySegment(seg) {
        const bucket = seg.FREQ_BUCKET || '';
        if (bucket.startsWith('1 —'))     return { archetype: 'ТРАФФИК',    rationale: 'разовая покупка миссии — клиент попробовал, растим до повторной' };
        if (bucket.startsWith('2-3'))     return { archetype: 'ТРАФФИК',    rationale: 'низкая частота миссии, клиент активен — растим до еженедельной' };
        if (bucket.startsWith('4-7'))     return { archetype: 'ТРАФФИК',    rationale: 'еженедельный паттерн — растим до 2+ раз/нед' };
        if (bucket.startsWith('8-15'))    return { archetype: 'МАРЖА',      rationale: 'высокая частота — апсейл премиум-позиций (раф, латте 300мл)' };
        if (bucket.startsWith('16+'))     return { archetype: 'УДЕРЖАНИЕ',  rationale: 'ядро («каждый день») — защита от ухода критична для миссии' };
        return { archetype: null, rationale: null };
    }

    /* ----------------- BLOCK 6.5: COHORT UPSELL ----------------- */
    /* ----------------- BLOCK 6.6: FIRST COFFEE × SEGMENT ----------------- */
    function renderCoffeeFirstSegment(d) {
        const ctxLift   = document.getElementById('coffee-firstseg-lift-lines');
        const ctxCounts = document.getElementById('coffee-firstseg-counts');
        if (!ctxLift || !ctxCounts) return;

        // Цвета сегментов (мапппинг: 1=Новые, 2=Активные, 3=Отток; окно 6 нед)
        // 'Активные LFL' оставлен для обратной совместимости со старыми выгрузками.
        const SEG_COLORS = {
            'Новые':        '#2E8B57',
            'Активные':     '#E87722',
            'Активные LFL': '#003A70',
            'Отток':        '#C41E3A',
        };
        const SEG_ORDER = ['Новые', 'Активные', 'Активные LFL', 'Отток'];

        const rows = (d.coffee_buyers_by_segment || [])
            .filter(r => r.SEGMENT_NAME && !r.SEGMENT_NAME.includes('не определ'));

        // --- Chart 1: Multi-line LIFT per segment over months ---
        // ⚠ Корректировка методологии: исходный LIFT_TO_BASE считает SHARE_BASE
        // от ВСЕЙ базы, включая «Отток», который по построению не может попасть
        // в кофе-когорту (Отток = нет покупок 6 нед, кофе-покупатель = есть
        // покупка → пересечение пусто). Из-за этого SHARE_BASE искусственно
        // занижен у всех активных сегментов, и лифт у всех получается > 1.
        // Правильная база = только активные сегменты (Новые + Активные).
        // Нормируем SHARE_BASE к сумме SHARE_BASE по строкам snapshot.
        const liftRowsRaw = (d.coffee_buyers_lift || [])
            .filter(r => r.SEGMENT_NAME
                && !r.SEGMENT_NAME.includes('не определ')
                && r.SHARE_BASE != null && r.SHARE_COFFEE != null);
        // sum SHARE_BASE per YEAR_MONTH (Отток в этой таблице отсутствует —
        // сумма даёт долю активной части базы)
        const baseActiveSum = {};
        for (const r of liftRowsRaw) {
            baseActiveSum[r.YEAR_MONTH] = (baseActiveSum[r.YEAR_MONTH] || 0) + (+r.SHARE_BASE || 0);
        }
        // Перерасчёт lift против активной базы
        const liftRowsAll = liftRowsRaw.map(r => {
            const activeSum = baseActiveSum[r.YEAR_MONTH] || 1;
            const shareBaseActive = (+r.SHARE_BASE) / activeSum;
            const liftAdj = shareBaseActive > 0 ? (+r.SHARE_COFFEE) / shareBaseActive : null;
            return Object.assign({}, r, {
                SHARE_BASE_ACTIVE: shareBaseActive,
                LIFT_TO_ACTIVE: liftAdj,
                DELTA_TO_ACTIVE: (+r.SHARE_COFFEE) - shareBaseActive,
            });
        }).filter(r => r.LIFT_TO_ACTIVE != null && !isNaN(r.LIFT_TO_ACTIVE));
        const liftPivot = {};
        const liftMonthsSet = new Set();
        for (const r of liftRowsAll) {
            const ym = r.YEAR_MONTH, seg = r.SEGMENT_NAME;
            liftMonthsSet.add(ym);
            (liftPivot[seg] = liftPivot[seg] || {})[ym] = +r.LIFT_TO_ACTIVE;
        }
        const liftMonths = Array.from(liftMonthsSet).sort((a,b)=>a-b);
        const liftSegs = SEG_ORDER.filter(s => liftPivot[s]);

        // диапазон оси Y: обе линии в одних координатах, линия ×1.0 в кадре
        const allLiftVals = [];
        for (const seg of liftSegs) for (const m of liftMonths) {
            const v = liftPivot[seg]?.[m];
            if (v != null && !isNaN(v)) allLiftVals.push(v);
        }
        const liftMin = Math.min(1, ...allLiftVals);
        const liftMax = Math.max(1, ...allLiftVals);
        const liftPad = (liftMax - liftMin) * 0.15 || 0.2;

        // кастомный плагин: пунктирная опорная линия на ×1.0 («как в активной базе»)
        const refLineOne = {
            id: 'coffeeRefLineOne',
            afterDatasetsDraw(chart) {
                const yScale = chart.scales.y;
                if (!yScale) return;
                const y = yScale.getPixelForValue(1.0);
                const { left, right } = chart.chartArea;
                const c = chart.ctx;
                c.save();
                c.beginPath();
                c.setLineDash([5, 4]);
                c.strokeStyle = 'rgba(31,42,55,0.40)';
                c.lineWidth = 1.2;
                c.moveTo(left, y); c.lineTo(right, y); c.stroke();
                c.setLineDash([]);
                c.fillStyle = 'rgba(31,42,55,0.65)';
                c.font = '700 10px Inter, sans-serif';
                c.textAlign = 'left'; c.textBaseline = 'bottom';
                c.fillText('×1,0 — доля как в активной базе', left + 6, y - 3);
                c.restore();
            }
        };

        destroyChart('coffee-firstseg-lift-lines');
        chartInstances['coffee-firstseg-lift-lines'] = new Chart(ctxLift, {
            type: 'line',
            plugins: [ChartDataLabels, refLineOne],
            data: {
                labels: liftMonths.map(YM_LABEL),
                datasets: liftSegs.map(seg => ({
                    label: seg,
                    data: liftMonths.map(m => liftPivot[seg]?.[m] != null ? +liftPivot[seg][m].toFixed(2) : null),
                    borderColor: SEG_COLORS[seg] || COFFEE_GRAY,
                    backgroundColor: SEG_COLORS[seg] || COFFEE_GRAY,
                    borderWidth: 2.5,
                    pointRadius: 3,
                    pointHoverRadius: 5,
                    tension: 0.3,
                    spanGaps: true,
                    datalabels: {
                        display: true,
                        align: (ctx) => (+ctx.dataset.data[ctx.dataIndex] >= 1 ? 'top' : 'bottom'),
                        offset: 4,
                        color: SEG_COLORS[seg] || COFFEE_GRAY,
                        font: { size: 9, weight: 700 },
                        formatter: (v, ctx) => {
                            if (v == null) return '';
                            // показываем подписи на первой, последней, max и min точке серии
                            const data = ctx.dataset.data;
                            const last = data.length - 1;
                            const valid = data.map((x,i)=>({x,i})).filter(o => o.x != null);
                            const maxIdx = valid.reduce((m,o)=>o.x>m.x?o:m, valid[0]).i;
                            const minIdx = valid.reduce((m,o)=>o.x<m.x?o:m, valid[0]).i;
                            if ([0, last, maxIdx, minIdx].includes(ctx.dataIndex)) {
                                return '×' + v.toFixed(2);
                            }
                            return '';
                        }
                    }
                }))
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                layout: { padding: { top: 24, bottom: 8 } },
                plugins: {
                    legend: { position: 'top', align: 'end' },
                    tooltip: { callbacks: { label: c => ` ${c.dataset.label}: ×${c.parsed.y.toFixed(2)}` } },
                },
                scales: {
                    x: { grid: { display: false }, border: { display: false }, ticks: { maxRotation: 0, autoSkip: true, font: { size: 10 } } },
                    y: { display: false, min: liftMin - liftPad, max: liftMax + liftPad }
                }
            }
        });

        // --- Chart 2: Stacked counts по сегментам, по месяцам ---
        const countMonthsSet = new Set();
        const countPivot = {};   // seg -> {ym -> n}
        for (const r of rows) {
            const ym = r.YEAR_MONTH, seg = r.SEGMENT_NAME;
            if (ym == null || !seg) continue;
            countMonthsSet.add(ym);
            (countPivot[seg] = countPivot[seg] || {})[ym] = +r.N_CONTACTS || 0;
        }
        const countMonths = Array.from(countMonthsSet).map(Number).sort((a,b)=>a-b);
        const countSegs = SEG_ORDER.filter(s => countPivot[s]);
        const totalsArr = countMonths.map(m => countSegs.reduce((s, seg) => s + (countPivot[seg]?.[m] || 0), 0));

        destroyChart('coffee-firstseg-counts');
        chartInstances['coffee-firstseg-counts'] = new Chart(ctxCounts, {
            type: 'bar',
            plugins: [ChartDataLabels],
            data: {
                labels: countMonths.map(YM_LABEL),
                datasets: countSegs.map((seg, segIdx) => ({
                    label: seg,
                    data: countMonths.map(m => countPivot[seg]?.[m] || 0),
                    backgroundColor: SEG_COLORS[seg] || COFFEE_GRAY,
                    borderRadius: segIdx === countSegs.length - 1 ? 3 : 0,
                    stack: 'a',
                    datalabels: segIdx === countSegs.length - 1 ? {
                        // ТОЛЬКО на самом верхнем сегменте — пишем общий тотал бара
                        display: true,
                        anchor: 'end',
                        align: 'end',
                        clip: false,
                        color: COFFEE_TXT,
                        font: { size: 10, weight: 700 },
                        formatter: (_, ctx) => fmtInt(totalsArr[ctx.dataIndex]),
                    } : { display: false },
                }))
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                layout: { padding: { top: 30 } },
                plugins: {
                    legend: { position: 'top', align: 'end' },
                    tooltip: {
                        callbacks: {
                            title: items => items[0].label,
                            label: c => {
                                const total = totalsArr[c.dataIndex];
                                const v = c.parsed.y;
                                const pct = total > 0 ? (v / total * 100) : 0;
                                return ` ${c.dataset.label}: ${fmtInt(v)} (${pct.toFixed(1)}%)`;
                            },
                            footer: items => ` Всего: ${fmtInt(totalsArr[items[0].dataIndex])}`
                        }
                    }
                },
                scales: {
                    x: { stacked: true, grid: { display: false }, border: { display: false }, ticks: { maxRotation: 0, autoSkip: true, font: { size: 10 } } },
                    y: { stacked: true, display: false }
                }
            }
        });
    }

    /* ----------------- BLOCK 6.65: РЕГИОНЫ И КАНАЛЫ ----------------- */
    function renderCoffeeRegionChannel(d) {
        const REGION_MAP = { 'Moscow + MO': 'Москва + МО', 'Spb+LO': 'СПб + ЛО', 'OTHER': 'Регионы' };
        const REGION_ORDER  = ['Москва + МО', 'СПб + ЛО', 'Регионы'];
        const REGION_COLORS = { 'Москва + МО': COFFEE_ORANGE, 'СПб + ЛО': COFFEE_PURPLE, 'Регионы': COFFEE_GRAY };
        const CH_ORDER  = ['Только оффлайн', 'Омни'];
        const CH_COLORS = { 'Только оффлайн': COFFEE_BROWN, 'Омни': COFFEE_ORANGE };

        // подписи на ~6 точках линии (первая, последняя + равномерно)
        const sparseLabel = (fmt) => ({
            display: true, align: 'top', offset: 5, font: { size: 9, weight: 700 },
            formatter: (v, ctx) => {
                if (v == null) return '';
                const n = ctx.dataset.data.length, last = n - 1;
                const step = Math.max(1, Math.round(n / 6));
                return (ctx.dataIndex % step === 0 || ctx.dataIndex === last) ? fmt(v) : '';
            }
        });

        const buildLine = (canvasId, rowsByKey, months, keys, colors, fmt) => {
            const ctx = document.getElementById(canvasId);
            if (!ctx) return;
            destroyChart(canvasId);
            chartInstances[canvasId] = new Chart(ctx, {
                type: 'line',
                plugins: [ChartDataLabels],
                data: {
                    labels: months.map(YM_LABEL),
                    datasets: keys.map(k => ({
                        label: k,
                        data: months.map(m => rowsByKey[k]?.[m] != null ? rowsByKey[k][m] : null),
                        borderColor: colors[k] || COFFEE_GRAY,
                        backgroundColor: colors[k] || COFFEE_GRAY,
                        borderWidth: 2.5, pointRadius: 2.5, pointHoverRadius: 5,
                        tension: 0.3, spanGaps: true,
                        datalabels: Object.assign(sparseLabel(fmt), { color: colors[k] || COFFEE_GRAY }),
                    }))
                },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    layout: { padding: { top: 22 } },
                    plugins: {
                        legend: { position: 'top', align: 'end' },
                        tooltip: { callbacks: { label: c => ` ${c.dataset.label}: ${fmt(c.parsed.y)}` } },
                    },
                    scales: {
                        x: { grid: { display: false }, border: { display: false }, ticks: { maxRotation: 0, autoSkip: true, font: { size: 10 } } },
                        y: { display: false, grace: '12%' }
                    }
                }
            });
        };

        // ── РЕГИОНЫ ──
        const reg = (d.dynamics_by_region || []).map(r => ({ ...r, RU: REGION_MAP[r.REGION] || r.REGION }));
        const regMonths = Array.from(new Set(reg.map(r => r.YEAR_MONTH))).sort((a, b) => a - b);
        const regShare = {}, regTO = {};
        for (const r of reg) {
            (regShare[r.RU] = regShare[r.RU] || {})[r.YEAR_MONTH] = +(r.SHARE_CHECKS * 100).toFixed(3);
            (regTO[r.RU]    = regTO[r.RU]    || {})[r.YEAR_MONTH] = +(r.TO_COFFEE / 1e6).toFixed(1);
        }
        const regKeys = REGION_ORDER.filter(k => regShare[k]);
        buildLine('coffee-region-share', regShare, regMonths, regKeys, REGION_COLORS, v => v.toFixed(3) + '%');
        buildLine('coffee-region-to',    regTO,    regMonths, regKeys, REGION_COLORS, v => v.toLocaleString('ru-RU') + ' млн');

        // KPI по регионам — последний месяц
        const elRegKpi = document.getElementById('coffee-region-kpi');
        if (elRegKpi && regMonths.length) {
            const lastYm = regMonths[regMonths.length - 1];
            elRegKpi.innerHTML = regKeys.map(k => {
                const row = reg.find(r => r.RU === k && r.YEAR_MONTH === lastYm) || {};
                const color = REGION_COLORS[k];
                return `
                    <div class="coffee-decomp-card" style="--accent:${color}">
                        <div class="coffee-decomp-label">${k} · ${YM_LABEL(lastYm)}</div>
                        <div class="coffee-decomp-to">${((row.SHARE_CHECKS||0)*100).toFixed(3)}%</div>
                        <div class="coffee-profile-sub">чеков с кофе · ТО ${(row.TO_COFFEE/1e6).toLocaleString('ru-RU',{maximumFractionDigits:1})} млн ₽ · витрина в ${((row.SHARE_SHOPS_COFFEE||0)*100).toFixed(1)}% магазинов региона</div>
                    </div>`;
            }).join('');
        }

        // ── КАНАЛЫ (профиль клиента) ──
        const ch = (d.dynamics_by_channel || []);
        const chMonths = Array.from(new Set(ch.map(r => r.YEAR_MONTH))).sort((a, b) => a - b);
        const chShare = {};
        for (const r of ch) {
            (chShare[r.CHANNEL_NAME] = chShare[r.CHANNEL_NAME] || {})[r.YEAR_MONTH] = +(r.SHARE_CHECKS * 100).toFixed(3);
        }
        const chKeys = CH_ORDER.filter(k => chShare[k]);
        buildLine('coffee-channel-share', chShare, chMonths, chKeys, CH_COLORS, v => v.toFixed(3) + '%');

        // KPI по каналам — последний месяц + во сколько раз омни активнее
        const elChKpi = document.getElementById('coffee-channel-kpi');
        if (elChKpi && chMonths.length) {
            const lastYm = chMonths[chMonths.length - 1];
            const off = ch.find(r => r.CHANNEL_NAME === 'Только оффлайн' && r.YEAR_MONTH === lastYm);
            const omni = ch.find(r => r.CHANNEL_NAME === 'Омни' && r.YEAR_MONTH === lastYm);
            const cards = chKeys.map(k => {
                const row = (k === 'Омни' ? omni : off) || {};
                return `
                    <div class="coffee-decomp-card" style="--accent:${CH_COLORS[k]}">
                        <div class="coffee-decomp-label">${k} · ${YM_LABEL(lastYm)}</div>
                        <div class="coffee-decomp-to">${((row.SHARE_CHECKS||0)*100).toFixed(3)}%</div>
                        <div class="coffee-profile-sub">чеков с кофе · ${fmtInt(row.N_CONTACTS_COFFEE)} клиентов берут кофе</div>
                    </div>`;
            });
            if (off && omni && off.SHARE_CHECKS) {
                const ratio = omni.SHARE_CHECKS / off.SHARE_CHECKS;
                cards.push(`
                    <div class="coffee-decomp-card" style="--accent:${COFFEE_PURPLE}">
                        <div class="coffee-decomp-label">Омни vs только оффлайн</div>
                        <div class="coffee-decomp-to">в ${ratio.toFixed(1)} раза чаще</div>
                        <div class="coffee-profile-sub">омни-клиенты берут готовый кофе в ${ratio.toFixed(1)} раза чаще, чем клиенты, покупающие только оффлайн</div>
                    </div>`);
            }
            elChKpi.innerHTML = cards.join('');
        }
    }

    /* ----------------- BLOCK 6.7: BASKET COMPOSITION ----------------- */
    function renderCoffeeBasketComposition(d) {
        const elSize  = document.getElementById('coffee-basket-size');
        const elCup   = document.getElementById('coffee-cup-per-check');
        if (!elSize || !elCup) return;

        // --- размер корзины ---
        const sizes = (d.basket_size || []).slice().sort((a,b)=>(a.SORT_ORDER||0)-(b.SORT_ORDER||0));
        const maxSize = sizes.length ? Math.max(...sizes.map(s=>s.SHARE_CHECKS||0)) : 1;
        elSize.innerHTML = sizes.map(s => {
            const w = ((s.SHARE_CHECKS||0) / maxSize) * 100;
            return `
                <div class="coffee-bar-row">
                    <div class="coffee-bar-name">${s.BASKET_SIZE_BUCKET}</div>
                    <div class="coffee-bar-track">
                        <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${COFFEE_BROWN}"></div>
                    </div>
                    <div class="coffee-bar-val">${((s.SHARE_CHECKS||0)*100).toFixed(1)}% · ${fmtInt(s.N_CHECKS)}</div>
                </div>
            `;
        }).join('');

        // --- сколько кофе в чеке ---
        const cups = (d.coffee_per_check || []).slice().sort((a,b)=>(a.SORT_ORDER||0)-(b.SORT_ORDER||0));
        const maxCup = cups.length ? Math.max(...cups.map(c=>c.SHARE_CHECKS||0)) : 1;
        elCup.innerHTML = cups.map(c => {
            const w = ((c.SHARE_CHECKS||0) / maxCup) * 100;
            const qty = c.AVG_QTY != null && c.AVG_QTY > 0 ? ` · ср ${(+c.AVG_QTY).toFixed(2)}` : '';
            return `
                <div class="coffee-bar-row">
                    <div class="coffee-bar-name">${c.COFFEE_PER_CHECK_BUCKET}</div>
                    <div class="coffee-bar-track">
                        <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${COFFEE_ORANGE}"></div>
                    </div>
                    <div class="coffee-bar-val">${((c.SHARE_CHECKS||0)*100).toFixed(2)}% · ${fmtInt(c.N_CHECKS)}${qty}</div>
                </div>
            `;
        }).join('');
    }

    function renderCoffeeCohorts(d) {
        const strip = document.getElementById('coffee-cohort-strip');
        const elCat  = document.getElementById('coffee-upsell-cat');
        const elProd = document.getElementById('coffee-upsell-prod');
        const elMiss = document.getElementById('coffee-upsell-mission');
        if (!strip || !elCat || !elProd) return;
        if (!d.cohort_size || !d.cohort_size.length) {
            strip.innerHTML = '<div class="coffee-card-footer">Когорты по сумме чека ещё не выгружены</div>';
            elCat.innerHTML = '';
            elProd.innerHTML = '';
            return;
        }

        // ── Cohort strip (3 cards): размер + средний чек ──
        const sortedCohorts = d.cohort_size.slice().sort((a, b) => (a.SORT_ORDER||0) - (b.SORT_ORDER||0));
        const colors = [COFFEE_GRAY, COFFEE_ORANGE, COFFEE_PURPLE];
        strip.innerHTML = '<div class="coffee-cohort-strip">' + sortedCohorts.map((c, i) => {
            const accent = colors[i] || COFFEE_BROWN;
            const cleanName = (c.BUCKET || '').replace(/^\d+\s*/, '').replace(/\?/g, '≤');
            return `
                <div class="coffee-cohort-card" style="border-top-color:${accent}">
                    <div class="coffee-cohort-name">${cleanName}</div>
                    <div class="coffee-cohort-val">${fmtInt(c.N_CHECKS)}</div>
                    <div class="coffee-cohort-sub">${(c.SHARE_CHECKS*100).toFixed(1)}% всех кофе-чеков · средний ${Math.round(c.AVG_COST)} ₽</div>
                </div>
            `;
        }).join('') + '</div>';

        // ── Top-10 categories by LIFT мини → ~средний ──
        // Минимум 0.1 п.п. дельты — отсекаем только «нули» (×40 при +0.0 п.п.).
        const topCat = (d.upsell_category || [])
            .filter(c => c.LIFT_MINI_TO_STD && c.LIFT_MINI_TO_STD > 1
                      && (c.DELTA_MINI_TO_STD || 0) >= 0.001
                      && !isBagOrLoyaltySku(c))
            .sort((a,b) => b.LIFT_MINI_TO_STD - a.LIFT_MINI_TO_STD)
            .slice(0, 10);
        const maxCatLift = topCat.length ? Math.max(...topCat.map(c => c.LIFT_MINI_TO_STD)) : 1;
        elCat.innerHTML = topCat.map(c => {
            const w = (c.LIFT_MINI_TO_STD / maxCatLift) * 100;
            const deltaTxt = c.DELTA_MINI_TO_STD ? ` (+${(c.DELTA_MINI_TO_STD*100).toFixed(1)} п.п.)` : '';
            return `
                <div class="coffee-bar-row">
                    <div class="coffee-bar-name" title="${c.I_CATEGORY_5}">${c.I_CATEGORY_5}</div>
                    <div class="coffee-bar-track">
                        <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${COFFEE_ORANGE}"></div>
                    </div>
                    <div class="coffee-bar-val coffee-bar-val-pos">×${c.LIFT_MINI_TO_STD.toFixed(1)}${deltaTxt}</div>
                </div>
            `;
        }).join('');

        // ── Top-10 products by LIFT мини → ~средний ──
        const topProd = (d.upsell_product || [])
            .filter(p => p.LIFT_MINI_TO_STD && p.LIFT_MINI_TO_STD > 1
                      && (p.DELTA_MINI_TO_STD || 0) >= 0.0005
                      && !isBagOrLoyaltySku(p))
            .sort((a,b) => b.LIFT_MINI_TO_STD - a.LIFT_MINI_TO_STD)
            .slice(0, 10);
        const maxProdLift = topProd.length ? Math.max(...topProd.map(p => p.LIFT_MINI_TO_STD)) : 1;
        elProd.innerHTML = topProd.map(p => {
            const w = (p.LIFT_MINI_TO_STD / maxProdLift) * 100;
            const deltaTxt = p.DELTA_MINI_TO_STD ? ` (+${(p.DELTA_MINI_TO_STD*100).toFixed(1)} п.п.)` : '';
            return `
                <div class="coffee-bar-row">
                    <div class="coffee-bar-name" title="${cleanProdName(p.PRODUCT_NAME)}">${cleanProdName(p.PRODUCT_NAME)}</div>
                    <div class="coffee-bar-track">
                        <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${COFFEE_PURPLE}"></div>
                    </div>
                    <div class="coffee-bar-val coffee-bar-val-pos">×${p.LIFT_MINI_TO_STD.toFixed(1)}${deltaTxt}</div>
                </div>
            `;
        }).join('');

        // ── Top-10 missions by LIFT мини → ~средний ──
        // Минимум 0.1 п.п. дельты — отсекаем только «нули» (Постное меню,
        // 23 февраля с микро-долей, где LIFT раздувается до ×40 от нуля).
        if (elMiss) {
            const topMiss = (d.upsell_mission || [])
                .filter(m => m.LIFT_MINI_TO_STD && m.LIFT_MINI_TO_STD > 1
                          && (m.DELTA_MINI_TO_STD || 0) >= 0.001
                          && !isBagOrLoyaltySku(m))
                .sort((a,b) => b.LIFT_MINI_TO_STD - a.LIFT_MINI_TO_STD)
                .slice(0, 10);
            const maxMissLift = topMiss.length ? Math.max(...topMiss.map(m => m.LIFT_MINI_TO_STD)) : 1;
            elMiss.innerHTML = topMiss.map(m => {
                const w = (m.LIFT_MINI_TO_STD / maxMissLift) * 100;
                const deltaTxt = m.DELTA_MINI_TO_STD ? ` (+${(m.DELTA_MINI_TO_STD*100).toFixed(1)} п.п.)` : '';
                return `
                    <div class="coffee-bar-row">
                        <div class="coffee-bar-name" title="${m.MISSION}">${m.MISSION}</div>
                        <div class="coffee-bar-track">
                            <div class="coffee-bar-fill" style="width:${w.toFixed(1)}%; background:${COFFEE_BROWN}"></div>
                        </div>
                        <div class="coffee-bar-val coffee-bar-val-pos">×${m.LIFT_MINI_TO_STD.toFixed(1)}${deltaTxt}</div>
                    </div>
                `;
            }).join('');
        }
    }

    function renderCoffeeGoals(d) {
        const el = document.getElementById('coffee-goals-grid');
        if (!el) return;

        const order = ['1 — разовая','2-3 — пару раз','4-7 — почти еженедельно','8-15 — часто','16+ — почти каждый день'];
        const segments = order.map(k => d.frequency.find(f => f.FREQ_BUCKET === k)).filter(Boolean);

        const missionTiming = inferMissionTiming(d.daytype);

        const rows = segments.map(seg => {
            const { archetype } = classifySegment(seg);
            const ent      = archetype ? METHODOLOGY_CATALOG[archetype] : null;
            const goal     = ent ? ent.goal     : null;
            const strategy = ent ? ent.strategy : null;
            const promos   = ent ? filterPromosForMission(ent.promos, missionTiming) : [];
            const accent   = ent ? ent.color  : '#C8CDD3';

            const mechHtml = promos.length
                ? `<ul class="coffee-goal-mech-list">${promos.map(p => `<li><span class="coffee-goal-mech-name">${p.название}</span><span class="coffee-goal-mech-tag">${p.механика}</span></li>`).join('')}</ul>`
                : '<span class="coffee-goal-empty">—</span>';

            return `
                <tr style="--accent:${accent}">
                    <td class="coffee-goal-seg-cell">
                        <div class="coffee-goal-seg-bar"></div>
                        <div>
                            <div class="coffee-goal-segname">${seg.FREQ_BUCKET}</div>
                            <div class="coffee-goal-segmeta">
                                ${fmtInt(seg.N_CONTACTS)} контактов ·
                                ${(seg.SHARE_CONTACTS*100).toFixed(1)}% покупателей кофе ·
                                ${(seg.SHARE_OF_CHECKS*100).toFixed(1)}% чеков миссии
                            </div>
                        </div>
                    </td>
                    <td class="coffee-goal-goal-cell">
                        <span class="coffee-goal-archpill" style="background:${accent}">${archetype || '—'}</span>
                        <div class="coffee-goal-value ${goal ? '' : 'is-empty'}">${goal || '—'}</div>
                    </td>
                    <td class="coffee-goal-strategy-cell">${strategy || '—'}</td>
                    <td class="coffee-goal-mech-cell">${mechHtml}</td>
                </tr>
            `;
        }).join('');

        el.innerHTML = `
            <table class="coffee-goal-table">
                <thead>
                    <tr>
                        <th class="coffee-goal-col-seg">Сегмент</th>
                        <th class="coffee-goal-col-goal">Цель</th>
                        <th class="coffee-goal-col-strategy">Стратегия</th>
                        <th class="coffee-goal-col-mech">Предлагаемая механика</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        `;
    }

    // Auto-init if tab is already active on first load
    if (document.querySelector('#tab-coffee.active')) buildCoffee();
})();


/* =================================================================
   MISSION REPORT — analytics sub-tab (McKinsey-style)
   Источник: данные/DATA_dashbord.xlsx → I_MISSION_REPORT → mission_report_data.js
   ================================================================= */
(function () {
    const ORANGE = '#FF7900';
    const PURPLE = '#5F259F';
    const GRAY   = '#94A3B8';
    const GREEN  = '#047857';
    const RED    = '#B91C1C';

    const fmtInt = n => (n == null || isNaN(n)) ? '—' : Math.round(n).toLocaleString('ru-RU');
    const fmtBn  = n => (n == null || isNaN(n)) ? '—' : (n >= 1e9 ? (n/1e9).toFixed(2) + ' млрд' : n >= 1e6 ? (n/1e6).toFixed(1) + ' млн' : n >= 1e3 ? (n/1e3).toFixed(0) + ' тыс' : Math.round(n).toString());
    const fmtPct = n => (n == null || isNaN(n)) ? '—' : (n * 100).toFixed(1) + '%';
    const fmtPctSigned = n => (n == null || isNaN(n)) ? '—' : (n > 0 ? '+' : '') + (n * 100).toFixed(1) + '%';
    const fmtRub = n => (n == null || isNaN(n)) ? '—' : Math.round(n).toLocaleString('ru-RU') + ' ₽';
    const ymToLabel = ym => {
        const y = Math.floor(ym / 100); const m = ym % 100;
        const months = ['','янв','фев','мар','апр','май','июн','июл','авг','сен','окт','ноя','дек'];
        return months[m] + ' ' + String(y).slice(2);
    };

    let built = false;
    let _chart = null;
    let _selectedMission = null;
    let _yoyMetric  = 'to';            // 'to' | 'checks' | 'clients' — что в YoY-столбиках
    let _cmpMonth   = null;            // YYYYMM — какой месяц сравниваем
    let _cmpMetric  = 'share_checks';  // share_checks | share_to | share_clients
    let _sortKey    = 'to';
    let _sortDir    = 'desc';

    function chartContainerHTML(id) { return `<div class="mr-chart-wrap"><canvas id="${id}"></canvas></div>`; }

    // ID миссий, исключаемых из view (служебные/мусорные).
    const EXCLUDED_MISSION_IDS = new Set([999]);   // 999 «Служебное (не миссия)»

    // ----- Aggregated data shapes -----
    function byMission() {
        // Map<id, { mission, group, icon, rows: [...] }>
        const map = new Map();
        MISSION_REPORT_DATA.forEach(r => {
            if (EXCLUDED_MISSION_IDS.has(r.id_mission)) return;
            if (!map.has(r.id_mission)) {
                map.set(r.id_mission, { id: r.id_mission, mission: r.mission, rows: [] });
            }
            map.get(r.id_mission).rows.push(r);
        });
        // group + icon from MISSIONS_DATA if available
        const groupOf = {};
        const iconOf  = {};
        if (typeof MISSIONS_DATA !== 'undefined') {
            MISSIONS_DATA.forEach(m => {
                const id = m.id ?? m.ID_MISSION;
                groupOf[id] = m.group ?? m.GROUP;
                iconOf[id]  = m.icon_url ?? m.ICON_URL;
            });
        }
        map.forEach(v => {
            v.rows.sort((a,b) => a.year_month - b.year_month);
            v.group = groupOf[v.id] || '';
            v.icon  = iconOf[v.id]  || '';
            const last12 = v.rows.slice(-12);
            const prev12 = v.rows.slice(-24, -12);
            const sumTO  = last12.reduce((s,r) => s + (r.to || 0), 0);
            const prevTO = prev12.reduce((s,r) => s + (r.to || 0), 0);
            const sumChk = last12.reduce((s,r) => s + (r.count_check || 0), 0);
            const sumCli = last12.reduce((s,r) => s + (r.count_client || 0), 0);
            v.to_last12       = sumTO;
            v.to_prev12       = prevTO;
            v.yoy_to          = prevTO > 0 ? (sumTO - prevTO) / prevTO : null;
            v.checks_last12   = sumChk;
            v.clients_last12  = sumCli;
            v.checks_avg_mo   = sumChk / Math.max(1, last12.length);
            v.to_avg_mo       = sumTO  / Math.max(1, last12.length);
            const last = v.rows[v.rows.length - 1];
            v.last_share_to       = last ? last.share_to : null;
            v.last_share_checks   = last ? last.share_checks : null;
            v.last_share_clients  = last ? last.share_clients : null;
            v.last_avg_check      = last ? last.avg_check_mission : null;
            v.last_price_index    = last ? last.price_index_to_all : null;
            v.last_penetration    = last ? last.penetration_to_all : null;
            v.last_avg_check_to_all = last ? last.avg_check_to_all : null;
        });
        return map;
    }

    function topline() {
        // ВАЖНО: НЕ суммируем TO / count_check через миссии — один чек попадает
        // в несколько миссий (44/34/16/5/1% checks по 1/2/3/4/5+ миссий).
        // Любые «компанейские» цифры из этого источника удваиваются. Только
        // per-mission показатели и сравнения между миссиями.
        const filtered = MISSION_REPORT_DATA.filter(r => !EXCLUDED_MISSION_IDS.has(r.id_mission));
        const months = [...new Set(filtered.map(r => r.year_month))].sort();
        return {
            months,
            lastYM:     months[months.length - 1],
            n_missions: new Set(filtered.map(r => r.id_mission)).size,
        };
    }

    // ----- Rendering -----
    function render(root) {
        const missions = byMission();
        const tl = topline();
        // Per-mission лидеры — никаких cross-mission сумм
        const arr = [...missions.values()];
        const byShareChecks = [...arr].filter(m => m.last_share_checks != null).sort((a,b) => b.last_share_checks - a.last_share_checks);
        const byShareTO     = [...arr].filter(m => m.last_share_to     != null).sort((a,b) => b.last_share_to     - a.last_share_to);
        // Для роста / падения берём только миссии с заметной долей (>0.5% share_to L12M)
        const material      = arr.filter(m => m.yoy_to != null && (m.last_share_to || 0) > 0.005);
        const growers       = [...material].sort((a,b) => b.yoy_to - a.yoy_to);
        const fallers       = [...material].sort((a,b) => a.yoy_to - b.yoy_to);
        const premiumArr    = arr.filter(m => m.last_price_index != null && (m.last_share_to || 0) > 0.005)
                                  .sort((a,b) => b.last_price_index - a.last_price_index);

        const leadChecks = byShareChecks[0];
        const leadTO     = byShareTO[0];
        const topGrower  = growers[0];
        const topFaller  = fallers[0];
        const topPremium = premiumArr[0];

        // Action-title без cross-mission сумм
        const headline = `Лидер по доле чеков — <strong>«${leadChecks?.mission ?? '—'}»</strong> (${fmtPct(leadChecks?.last_share_checks)}); ` +
                         `быстрее всех растёт <strong>«${topGrower?.mission ?? '—'}»</strong> ` +
                         `(${fmtPctSigned(topGrower?.yoy_to)} год к году по обороту миссии), ` +
                         `падает <strong>«${topFaller?.mission ?? '—'}»</strong> ` +
                         `(${fmtPctSigned(topFaller?.yoy_to)} год к году)`;

        root.innerHTML = `
          <div class="mr-root">

            <!-- HEADLINE -->
            <div class="mr-headline">
                <div class="mr-headline-sub">Анализ миссий · ${ymToLabel(tl.months[tl.months.length-1])} · ${tl.months.length} мес · ${tl.n_missions} миссий</div>
                <div class="mr-headline-title">${headline}</div>
            </div>

            <!-- KPI STRIP — 4 per-mission лидера, без cross-mission сумм -->
            <div class="mr-kpi-strip">
                <div class="mr-kpi" style="--mr-accent:${ORANGE}">
                    <div class="mr-kpi-label">Лидер по доле чеков</div>
                    <div class="mr-kpi-mission">${leadChecks?.mission ?? '—'}</div>
                    <div class="mr-kpi-value">${fmtPct(leadChecks?.last_share_checks)}</div>
                    <div class="mr-kpi-sub">% всех чеков базы за ${ymToLabel(tl.lastYM)}</div>
                </div>
                <div class="mr-kpi" style="--mr-accent:${PURPLE}">
                    <div class="mr-kpi-label">Лидер по доле оборота</div>
                    <div class="mr-kpi-mission">${leadTO?.mission ?? '—'}</div>
                    <div class="mr-kpi-value">${fmtPct(leadTO?.last_share_to)}</div>
                    <div class="mr-kpi-sub">% оборота базы за ${ymToLabel(tl.lastYM)}</div>
                </div>
                <div class="mr-kpi" style="--mr-accent:${GREEN}">
                    <div class="mr-kpi-label">Самый сильный рост</div>
                    <div class="mr-kpi-mission">${topGrower?.mission ?? '—'}</div>
                    <div class="mr-kpi-value mr-delta-pos">${fmtPctSigned(topGrower?.yoy_to)}</div>
                    <div class="mr-kpi-sub">год к году по обороту · последние 12 мес vs предыдущие 12</div>
                </div>
                <div class="mr-kpi" style="--mr-accent:#0EA5E9">
                    <div class="mr-kpi-label">Самый высокий индекс цен корзины</div>
                    <div class="mr-kpi-mission">${topPremium?.mission ?? '—'}</div>
                    <div class="mr-kpi-value">${fmtPctSigned(topPremium?.last_price_index)}</div>
                    <div class="mr-kpi-sub">клиенты миссии покупают более дорогие SKU, чем средний клиент сети</div>
                </div>
            </div>

            <!-- COMPARISON TABLE — все миссии период vs год назад -->
            <div class="mr-card">
                <div class="mr-card-head">
                    <div>
                        <div class="mr-card-title">Доли миссий: текущий месяц vs тот же месяц годом ранее</div>
                        <div class="mr-card-subtitle">Все миссии в одном виде · клик по строке открывает помесячный график ниже</div>
                    </div>
                    <div class="mr-card-controls">
                        <select id="mr-cmp-month" title="Месяц для сравнения"></select>
                        <div class="seg-channel-toggle" id="mr-cmp-metric-switch">
                            <button type="button" class="seg-channel-btn active" data-m="share_checks">Чеки</button>
                            <button type="button" class="seg-channel-btn" data-m="share_to">Оборот</button>
                            <button type="button" class="seg-channel-btn" data-m="share_clients">Клиенты</button>
                        </div>
                    </div>
                </div>
                <div class="mr-cmp-layout">
                    <div class="mr-cmp-table-wrap">
                        <table class="mr-cmp-table" id="mr-cmp-table">
                            <thead>
                                <tr>
                                    <th>Миссия</th>
                                    <th class="num" id="mr-cmp-col-curr">Текущий</th>
                                    <th class="num" id="mr-cmp-col-prev">Год назад</th>
                                    <th class="num">Δ п.п.</th>
                                    <th class="num">Δ %</th>
                                    <th>Тренд 12 мес</th>
                                </tr>
                            </thead>
                            <tbody></tbody>
                        </table>
                    </div>
                    <div class="mr-cmp-conclusions">
                        <div class="mr-cmp-conclusions-title">Выводы</div>
                        <div id="mr-cmp-conclusions-body"></div>
                    </div>
                </div>
            </div>

            <!-- YoY BARS — детальный drill-down для выбранной миссии -->
            <div class="mr-card">
                <div class="mr-card-head">
                    <div>
                        <div class="mr-card-title">Год к году по месяцам — выбранная миссия</div>
                    </div>
                    <div class="mr-card-controls">
                        <select id="mr-mission-select"></select>
                        <div class="seg-channel-toggle" id="mr-metric-switch">
                            <button type="button" class="seg-channel-btn active" data-m="to">Оборот</button>
                            <button type="button" class="seg-channel-btn" data-m="checks">Чеки</button>
                            <button type="button" class="seg-channel-btn" data-m="clients">Клиенты</button>
                            <button type="button" class="seg-channel-btn" data-m="avg_check">Ср. чек</button>
                            <button type="button" class="seg-channel-btn" data-m="freq">Чеков/клиента</button>
                            <button type="button" class="seg-channel-btn" data-m="qty">Лояльность</button>
                            <button type="button" class="seg-channel-btn" data-m="sku">SKU/чек</button>
                        </div>
                    </div>
                </div>
                <div class="mr-yoy-header" id="mr-yoy-header"></div>
                ${chartContainerHTML('mr-yoy-chart')}
                <div class="mr-mission-detail" id="mr-mission-detail"></div>
            </div>

            <!-- RANKING MATRIX -->
            <div class="mr-card">
                <div class="mr-card-head">
                    <div>
                        <div class="mr-card-title">Рейтинг миссий</div>
                        <div class="mr-card-subtitle">Сортировка по любому показателю · последние 12 месяцев</div>
                    </div>
                </div>
                <div class="mr-rank-table-wrap">
                    <table class="mr-rank-table" id="mr-rank-table">
                        <thead>
                            <tr>
                                <th data-k="mission" class="sortable">Миссия</th>
                                <th data-k="to" class="sortable num sorted-desc">Оборот за 12 мес</th>
                                <th data-k="share_to" class="sortable num">Доля оборота</th>
                                <th data-k="yoy" class="sortable num">Год к году</th>
                                <th data-k="checks" class="sortable num">Чеки/мес</th>
                                <th data-k="avg_check" class="sortable num">Ср. чек</th>
                                <th data-k="avg_to_all" class="sortable num">Чек vs база</th>
                                <th data-k="price_idx" class="sortable num">Индекс цен</th>
                                <th data-k="penet" class="sortable num">Лояльность</th>
                            </tr>
                        </thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>

            <!-- INSIGHTS -->
            <div class="mr-card">
                <div class="mr-card-head">
                    <div>
                        <div class="mr-card-title">Ключевые выводы</div>
                        <div class="mr-card-subtitle">Авто-выводы из I_MISSION_REPORT — McKinsey-style</div>
                    </div>
                </div>
                <div class="mr-insights-grid" id="mr-insights"></div>
            </div>

          </div>
        `;

        // populate selector
        const sel = root.querySelector('#mr-mission-select');
        arr.forEach(m => {
            const opt = document.createElement('option');
            opt.value = m.id;
            opt.textContent = `${m.mission}${m.group ? ' · ' + m.group : ''}`;
            sel.appendChild(opt);
        });
        _selectedMission = leadTO?.id ?? arr[0]?.id;
        sel.value = _selectedMission;
        sel.addEventListener('change', () => { _selectedMission = +sel.value; drawYoY(); drawDetail(); highlightRankRow(); highlightCmpRow(); });

        // metric switch — единый стиль с остальным дашбордом (seg-channel-toggle)
        root.querySelectorAll('#mr-metric-switch .seg-channel-btn').forEach(b => {
            b.addEventListener('click', () => {
                root.querySelectorAll('#mr-metric-switch .seg-channel-btn').forEach(x => x.classList.remove('active'));
                b.classList.add('active');
                _yoyMetric = b.dataset.m;
                drawYoY();
            });
        });

        // comparison block — month picker + metric switcher
        const monthsWithYoY = tl.months.filter(m => tl.months.includes(m - 100));
        const cmpSel = root.querySelector('#mr-cmp-month');
        monthsWithYoY.forEach(ym => {
            const opt = document.createElement('option');
            opt.value = ym;
            opt.textContent = `${ymToLabel(ym)} vs ${ymToLabel(ym - 100)}`;
            cmpSel.appendChild(opt);
        });
        _cmpMonth = monthsWithYoY[monthsWithYoY.length - 1];
        cmpSel.value = _cmpMonth;
        cmpSel.addEventListener('change', () => { _cmpMonth = +cmpSel.value; drawComparison(); });

        root.querySelectorAll('#mr-cmp-metric-switch .seg-channel-btn').forEach(b => {
            b.addEventListener('click', () => {
                root.querySelectorAll('#mr-cmp-metric-switch .seg-channel-btn').forEach(x => x.classList.remove('active'));
                b.classList.add('active');
                _cmpMetric = b.dataset.m;
                drawComparison();
            });
        });

        // sort header click
        root.querySelectorAll('#mr-rank-table thead th.sortable').forEach(th => {
            th.addEventListener('click', () => {
                const k = th.dataset.k;
                if (_sortKey === k) {
                    _sortDir = _sortDir === 'desc' ? 'asc' : 'desc';
                } else {
                    _sortKey = k;
                    _sortDir = (k === 'mission') ? 'asc' : 'desc';
                }
                drawRank();
            });
        });

        drawComparison();
        drawYoY();
        drawDetail();
        drawRank();
        drawInsights();
    }

    // Inline SVG sparkline (12 точек)
    function renderSpark(values, accent) {
        if (!values || !values.length) return '';
        const vs = values.map(v => v == null ? 0 : v);
        const max = Math.max(...vs);
        const min = Math.min(...vs);
        const range = max - min || max || 1;
        const W = 80, H = 22, PAD = 1;
        const pts = vs.map((v, i) => {
            const x = PAD + (i / Math.max(1, vs.length - 1)) * (W - PAD * 2);
            const y = H - PAD - ((v - min) / range) * (H - PAD * 2);
            return `${x.toFixed(1)},${y.toFixed(1)}`;
        }).join(' ');
        return `<svg width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" class="mr-spark"><polyline points="${pts}" fill="none" stroke="${accent || ORANGE}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/></svg>`;
    }

    function drawComparison() {
        const missions = byMission();
        const monthYM = _cmpMonth;
        const prevYM  = monthYM - 100;
        const metric  = _cmpMetric;
        const arr = [...missions.values()].map(m => {
            const idx = new Map(m.rows.map(r => [r.year_month, r]));
            const curr = idx.get(monthYM);
            const prev = idx.get(prevYM);
            const last12 = m.rows.slice(-12).map(r => r[metric] || 0);
            const cv = curr ? curr[metric] : null;
            const pv = prev ? prev[metric] : null;
            return {
                id: m.id,
                mission: m.mission,
                group: m.group,
                curr: cv,
                prev: pv,
                delta_pp:  (cv != null && pv != null) ? (cv - pv) : null,
                delta_rel: (cv != null && pv > 0) ? (cv - pv) / pv : null,
                spark: last12,
            };
        }).sort((a, b) => (b.curr || 0) - (a.curr || 0));

        const maxV = Math.max(...arr.map(r => r.curr || 0), ...arr.map(r => r.prev || 0)) || 1;

        // Update column headers with picked period labels
        document.getElementById('mr-cmp-col-curr').textContent = ymToLabel(monthYM);
        document.getElementById('mr-cmp-col-prev').textContent = ymToLabel(prevYM);

        const fmtVal = v => v == null ? '—' : (v * 100).toFixed(2) + '%';
        const deltaPP = v => {
            if (v == null) return '<span class="mr-delta-flat">—</span>';
            const cls = v > 0.0005 ? 'mr-delta-pos' : v < -0.0005 ? 'mr-delta-neg' : 'mr-delta-flat';
            const sign = v > 0 ? '+' : '';
            return `<span class="${cls}">${sign}${(v * 100).toFixed(2)} п.п.</span>`;
        };
        const deltaRel = v => {
            if (v == null) return '<span class="mr-delta-flat">—</span>';
            const cls = v > 0.005 ? 'mr-delta-pos' : v < -0.005 ? 'mr-delta-neg' : 'mr-delta-flat';
            const sign = v > 0 ? '+' : '';
            return `<span class="${cls}">${sign}${(v * 100).toFixed(1)}%</span>`;
        };

        const tbody = document.querySelector('#mr-cmp-table tbody');
        tbody.innerHTML = arr.map(r => {
            const wCurr = r.curr != null ? Math.max(2, r.curr / maxV * 100) : 0;
            const wPrev = r.prev != null ? Math.max(2, r.prev / maxV * 100) : 0;
            const sparkColor = r.delta_pp == null ? GRAY : r.delta_pp >= 0 ? '#10B981' : '#EF4444';
            return `
            <tr data-mid="${r.id}" class="${r.id === _selectedMission ? 'is-selected' : ''}">
                <td class="mr-cmp-name"><span class="mr-cmp-mission">${r.mission}</span></td>
                <td class="mr-cmp-cell">
                    <div class="mr-cmp-bar mr-cmp-bar-curr" style="width:${wCurr.toFixed(1)}%"></div>
                    <span class="mr-cmp-val">${fmtVal(r.curr)}</span>
                </td>
                <td class="mr-cmp-cell">
                    <div class="mr-cmp-bar mr-cmp-bar-prev" style="width:${wPrev.toFixed(1)}%"></div>
                    <span class="mr-cmp-val">${fmtVal(r.prev)}</span>
                </td>
                <td class="num">${deltaPP(r.delta_pp)}</td>
                <td class="num">${deltaRel(r.delta_rel)}</td>
                <td class="mr-cmp-spark-cell">${renderSpark(r.spark, sparkColor)}</td>
            </tr>`;
        }).join('');

        // row click → select mission
        tbody.querySelectorAll('tr').forEach(tr => {
            tr.addEventListener('click', () => {
                _selectedMission = +tr.dataset.mid;
                document.getElementById('mr-mission-select').value = _selectedMission;
                drawYoY(); drawDetail(); highlightRankRow(); highlightCmpRow();
                document.getElementById('mr-yoy-chart').scrollIntoView({ behavior: 'smooth', block: 'center' });
            });
        });

        // Auto-conclusions — McKinsey-style
        const grew = arr.filter(r => r.delta_rel != null && r.curr > 0.005)
                         .sort((a,b) => b.delta_rel - a.delta_rel).slice(0, 5);
        const fell = arr.filter(r => r.delta_rel != null && r.prev > 0.005)
                         .sort((a,b) => a.delta_rel - b.delta_rel).slice(0, 5);

        document.getElementById('mr-cmp-conclusions-body').innerHTML = `
            <div class="mr-cmp-concl-block">
                <div class="mr-cmp-concl-tag" style="color:${GREEN}">Топ-5 рост</div>
                <ul class="mr-cmp-concl-list">
                    ${grew.map(r => `<li><span class="mr-cmp-concl-name">${r.mission}</span><span class="mr-delta-pos">+${(r.delta_rel * 100).toFixed(1)}%</span></li>`).join('')}
                </ul>
            </div>
            <div class="mr-cmp-concl-block">
                <div class="mr-cmp-concl-tag" style="color:${RED}">Топ-5 падение</div>
                <ul class="mr-cmp-concl-list">
                    ${fell.map(r => `<li><span class="mr-cmp-concl-name">${r.mission}</span><span class="mr-delta-neg">${(r.delta_rel * 100).toFixed(1)}%</span></li>`).join('')}
                </ul>
            </div>
        `;
    }

    function highlightCmpRow() {
        document.querySelectorAll('#mr-cmp-table tbody tr').forEach(tr => {
            tr.classList.toggle('is-selected', +tr.dataset.mid === _selectedMission);
        });
    }

    // Парные столбики: для каждого месяца последних 12 (где есть YoY-пара)
    // показываем 2 бара — прошлый год (серый) и текущий (цвет метрики).
    // Линия сверху — относительный рост базы сети (YoY% базы) для сравнения.
    function drawYoY() {
        const missions = byMission();
        const m = missions.get(_selectedMission);
        if (!m) return;

        // Конфиг метрик: значение из строки, формат подписи, цвет, нужна ли база
        const metricCfg = {
            to:        { label: 'Оборот',          accent: ORANGE,    fmt: fmtBn, unit: ' ₽',
                         val: r => r.to,
                         baseVal: r => r.share_to > 0 ? r.to / r.share_to : null },
            checks:    { label: 'Чеки',            accent: PURPLE,    fmt: fmtBn, unit: '',
                         val: r => r.count_check,
                         baseVal: r => r.share_checks > 0 ? r.count_check / r.share_checks : null },
            clients:   { label: 'Клиенты',         accent: '#0EA5E9', fmt: fmtBn, unit: '',
                         val: r => r.count_client,
                         baseVal: r => r.share_clients > 0 ? r.count_client / r.share_clients : null },
            avg_check: { label: 'Средний чек миссии', accent: '#0F766E', fmt: v => fmtInt(v) + ' ₽', unit: '',
                         val: r => r.avg_check_mission,
                         baseVal: r => {
                            const bTO = r.share_to     > 0 ? r.to / r.share_to : null;
                            const bCk = r.share_checks > 0 ? r.count_check / r.share_checks : null;
                            return (bTO && bCk) ? bTO / bCk : null;
                         } },
            freq:      { label: 'Чеков на клиента', accent: '#9333EA', fmt: v => v.toFixed(2), unit: '',
                         val: r => r.count_client > 0 ? r.count_check / r.count_client : null,
                         baseVal: r => {
                            const bCk = r.share_checks  > 0 ? r.count_check / r.share_checks   : null;
                            const bCl = r.share_clients > 0 ? r.count_client / r.share_clients : null;
                            return (bCk && bCl) ? bCk / bCl : null;
                         } },
            qty:       { label: 'Лояльность миссии', accent: '#DC2626', fmt: v => (v * 100).toFixed(1) + '%', unit: '',
                         val: r => r.quantity_mission,
                         baseVal: () => null /* база = 1.0 по определению */ },
            sku:       { label: 'SKU/чек миссии', accent: '#7C3AED', fmt: v => v.toFixed(2), unit: '',
                         val: r => r.avg_sku_per_check,
                         baseVal: () => null /* база = средний SKU на чек по сети; нет данных */ },
        };
        const cfg = metricCfg[_yoyMetric] || metricCfg.to;
        const accent = cfg.accent;

        // Последние 12 месяцев с YoY-парой
        const idx = new Map(m.rows.map(r => [r.year_month, r]));
        const pairs = [];
        m.rows.forEach(r => {
            const prev = idx.get(r.year_month - 100);
            if (!prev) return;
            const cv = cfg.val(r), pv = cfg.val(prev);
            if (cv == null || pv == null || pv <= 0) return;
            const baseCurr = cfg.baseVal(r);
            const basePrev = cfg.baseVal(prev);
            const baseYoy  = (baseCurr != null && basePrev > 0) ? (baseCurr - basePrev) / basePrev : null;
            pairs.push({ ym: r.year_month, curr: cv, prev: pv, baseYoy });
        });
        const last12 = pairs.slice(-12);
        const labels  = last12.map(p => ymToLabel(p.ym));
        const currArr = last12.map(p => p.curr);
        const prevArr = last12.map(p => p.prev);
        const yoyArr  = last12.map(p => (p.curr - p.prev) / p.prev);
        const baseYoyArr = last12.map(p => p.baseYoy != null ? p.baseYoy * 100 : null);
        const hasBaseLine = baseYoyArr.some(v => v != null);

        const ctx = document.getElementById('mr-yoy-chart').getContext('2d');
        if (_chart) { _chart.destroy(); _chart = null; }

        // Header с иконкой + метрикой
        const header = document.getElementById('mr-yoy-header');
        if (header) {
            const iconHtml = m.icon ? `<img src="${m.icon}" alt="" class="mr-yoy-icon" onerror="this.style.display='none'">` : '';
            header.innerHTML = `${iconHtml}<span>${m.mission} · ${cfg.label}</span>`;
        }

        const datasets = [
            {
                label: 'Прошлый год',
                data: prevArr,
                backgroundColor: '#CBD5E1',
                borderColor: '#94A3B8',
                borderWidth: 0,
                borderRadius: 3,
                barPercentage: 0.85,
                categoryPercentage: 0.75,
                yAxisID: 'y',
                order: 2,
            },
            {
                label: 'Текущий год',
                data: currArr,
                backgroundColor: accent,
                borderColor: accent,
                borderWidth: 0,
                borderRadius: 3,
                barPercentage: 0.85,
                categoryPercentage: 0.75,
                yAxisID: 'y',
                order: 2,
            },
        ];
        if (hasBaseLine) {
            datasets.push({
                type: 'line',
                label: 'Год к году по сети (база)',
                data: baseYoyArr,
                borderColor: '#0F172A',
                backgroundColor: 'transparent',
                borderWidth: 1.8,
                borderDash: [5, 4],
                pointRadius: 3,
                pointBackgroundColor: '#0F172A',
                tension: .25,
                yAxisID: 'yPct',
                order: 1,
            });
        }

        _chart = new Chart(ctx, {
            type: 'bar',
            plugins: [ChartDataLabels],
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { boxWidth: 12, boxHeight: 12, padding: 16, font: { size: 12, weight: 500 } },
                    },
                    datalabels: {
                        display: ctx => ctx.datasetIndex === 1,    // подписи только над текущим годом
                        anchor: 'end',
                        align: 'top',
                        formatter: v => (v == null || isNaN(v)) ? '' : cfg.fmt(v),
                        font: { size: 10, weight: '700' },
                        color: '#0F172A',
                        offset: 2,
                    },
                    tooltip: {
                        callbacks: {
                            label: ctx => {
                                const v = ctx.parsed.y;
                                if (ctx.dataset.yAxisID === 'yPct') {
                                    return `${ctx.dataset.label}: ${(v > 0 ? '+' : '') + v.toFixed(1)}%`;
                                }
                                return `${ctx.dataset.label}: ${cfg.fmt(v)}${cfg.unit}`;
                            },
                            afterBody: items => {
                                const idx = items[0].dataIndex;
                                const y = yoyArr[idx];
                                const b = baseYoyArr[idx];
                                const lines = [`Миссия год к году: ${(y > 0 ? '+' : '') + (y * 100).toFixed(1)}%`];
                                if (b != null) lines.push(`Сеть год к году:   ${(b > 0 ? '+' : '') + b.toFixed(1)}%`);
                                return lines;
                            },
                        },
                    },
                    title: { display: false },
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        display: false,
                        grid: { display: false },
                    },
                    yPct: {
                        position: 'right',
                        display: true,
                        grid: { display: false },
                        ticks: {
                            callback: v => (v > 0 ? '+' : '') + v + '%',
                            font: { size: 10 },
                            color: '#64748B',
                        },
                        title: { display: false },
                    },
                    x: { ticks: { font: { size: 11 } }, grid: { display: false } },
                },
                layout: { padding: { top: 24 } },
            },
        });
    }

    function drawDetail() {
        const missions = byMission();
        const m = missions.get(_selectedMission);
        if (!m) return;
        const last = m.rows[m.rows.length - 1];
        const prevYear = m.rows.find(r => r.year_month === (last?.year_month || 0) - 100);

        // Помесячные ряды для sparklines (последние 12 месяцев)
        const last12 = m.rows.slice(-12);
        const sparkCheck = last12.map(r => r.avg_check_mission || 0);
        const sparkFreq  = last12.map(r => r.count_client > 0 ? r.count_check / r.count_client : 0);
        const sparkQty   = last12.map(r => (r.quantity_mission || 0) * 100);

        // LIFT — рост миссии vs рост сети (последние 12мес vs предыдущие 12мес)
        const idx = new Map(m.rows.map(r => [r.year_month, r]));
        let baseTOlast12 = 0, baseTOprev12 = 0;
        m.rows.slice(-12).forEach(r => { if (r.share_to > 0) baseTOlast12 += r.to / r.share_to; });
        m.rows.slice(-24, -12).forEach(r => { if (r.share_to > 0) baseTOprev12 += r.to / r.share_to; });
        // Усредняем (т.к. каждый месяц у разных миссий разный base — мы дублируем),
        // достаточно для оценки направления: берём среднее по числу месяцев.
        const avgBaseTOlast = baseTOlast12 / Math.max(1, last12.length);
        const avgBaseTOprev = baseTOprev12 / Math.max(1, last12.length);
        const baseYoy = (avgBaseTOprev > 0) ? (avgBaseTOlast - avgBaseTOprev) / avgBaseTOprev : null;
        const liftYoy = (m.yoy_to != null && baseYoy != null && (1 + baseYoy) !== 0)
                        ? ((1 + m.yoy_to) / (1 + baseYoy)) - 1
                        : null;

        // Helpers
        const lastVal = (field) => last ? last[field] : null;
        const yoy = (curr, prev) => (curr != null && prev > 0) ? (curr - prev) / prev : null;
        const avgCheckYoy = yoy(lastVal('avg_check_mission'), prevYear?.avg_check_mission);
        const freqCurr = (last && last.count_client > 0) ? last.count_check / last.count_client : null;
        const freqPrev = (prevYear && prevYear.count_client > 0) ? prevYear.count_check / prevYear.count_client : null;
        const freqYoy = yoy(freqCurr, freqPrev);
        const qtyCurr = lastVal('quantity_mission');
        const qtyYoy  = yoy(qtyCurr, prevYear?.quantity_mission);

        const pill = v => {
            if (v == null) return '<span class="mr-detail-pill flat">—</span>';
            const cls = v > 0.005 ? 'up' : v < -0.005 ? 'down' : 'flat';
            const sign = v > 0 ? '+' : '';
            return `<span class="mr-detail-pill ${cls}">${sign}${(v * 100).toFixed(1)}% YoY</span>`;
        };

        const el = document.getElementById('mr-mission-detail');
        el.innerHTML = `
            <div class="mr-detail-kpi">
                <div class="mr-detail-label">Средний чек миссии</div>
                <div class="mr-detail-value">${fmtInt(lastVal('avg_check_mission'))} ₽</div>
                ${pill(avgCheckYoy)}
                <div class="mr-detail-spark">${renderSpark(sparkCheck, ORANGE)}</div>
                <div class="mr-detail-hint">vs среднего чека базы: ${fmtPctSigned(lastVal('avg_check_to_all'))}</div>
            </div>
            <div class="mr-detail-kpi">
                <div class="mr-detail-label">Чеков на клиента</div>
                <div class="mr-detail-value">${freqCurr != null ? freqCurr.toFixed(2) : '—'}</div>
                ${pill(freqYoy)}
                <div class="mr-detail-spark">${renderSpark(sparkFreq, PURPLE)}</div>
                <div class="mr-detail-hint">частота покупок миссии = чеков / клиента</div>
            </div>
            <div class="mr-detail-kpi">
                <div class="mr-detail-label">Глубина проникновения в чек</div>
                <div class="mr-detail-value">${qtyCurr != null ? (qtyCurr * 100).toFixed(1) + '%' : '—'}</div>
                ${pill(qtyYoy)}
                <div class="mr-detail-spark">${renderSpark(sparkQty, '#0EA5E9')}</div>
                <div class="mr-detail-hint">доля SKU чека из категорий миссии</div>
            </div>
            <div class="mr-detail-kpi">
                <div class="mr-detail-label">LIFT vs сеть</div>
                <div class="mr-detail-value ${liftYoy == null ? '' : (liftYoy > 0 ? 'mr-delta-pos' : liftYoy < 0 ? 'mr-delta-neg' : '')}">${fmtPctSigned(liftYoy)}</div>
                <div class="mr-detail-hint">
                    миссия ${m.yoy_to == null ? '' : (m.yoy_to > 0 ? '+' : '') + (m.yoy_to * 100).toFixed(1) + '%'}
                    vs сеть ${baseYoy == null ? '' : (baseYoy > 0 ? '+' : '') + (baseYoy * 100).toFixed(1) + '%'}
                </div>
            </div>
        `;
    }

    function sortedMissions() {
        const missions = byMission();
        const arr = [...missions.values()];
        const key = _sortKey;
        const dir = _sortDir === 'desc' ? -1 : 1;
        const k = (m) => {
            switch (key) {
                case 'mission':    return m.mission || '';
                case 'to':         return m.to_last12 || 0;
                case 'share_to':   return m.last_share_to || 0;
                case 'yoy':        return m.yoy_to == null ? -Infinity * dir : m.yoy_to;
                case 'checks':     return m.checks_avg_mo || 0;
                case 'avg_check':  return m.last_avg_check || 0;
                case 'avg_to_all': return m.last_avg_check_to_all == null ? -Infinity * dir : m.last_avg_check_to_all;
                case 'price_idx':  return m.last_price_index == null ? -Infinity * dir : m.last_price_index;
                case 'penet':      return m.last_penetration == null ? -Infinity * dir : m.last_penetration;
                default:           return m.to_last12 || 0;
            }
        };
        arr.sort((a,b) => {
            const va = k(a), vb = k(b);
            if (typeof va === 'string') return va.localeCompare(vb, 'ru') * dir;
            return (va - vb) * dir;
        });
        return arr;
    }

    function drawRank() {
        const arr = sortedMissions();
        const tbody = document.querySelector('#mr-rank-table tbody');
        const delta = v => {
            if (v == null || isNaN(v)) return '<span class="mr-delta-flat">—</span>';
            const cls = v > 0.005 ? 'mr-delta-pos' : v < -0.005 ? 'mr-delta-neg' : 'mr-delta-flat';
            return `<span class="${cls}">${fmtPctSigned(v)}</span>`;
        };
        tbody.innerHTML = arr.map(m => `
            <tr data-mid="${m.id}" class="${m.id === _selectedMission ? 'is-selected' : ''}">
                <td><div class="mr-rank-mission">${m.mission}${m.group ? `<span class="mr-rank-group-pill">${m.group}</span>` : ''}</div></td>
                <td class="num">${fmtBn(m.to_last12)} ₽</td>
                <td class="num">${fmtPct(m.last_share_to)}</td>
                <td class="num">${delta(m.yoy_to)}</td>
                <td class="num">${fmtBn(m.checks_avg_mo)}</td>
                <td class="num">${fmtInt(m.last_avg_check)} ₽</td>
                <td class="num">${delta(m.last_avg_check_to_all)}</td>
                <td class="num">${delta(m.last_price_index)}</td>
                <td class="num">${delta(m.last_penetration)}</td>
            </tr>
        `).join('');
        // update sort-header markers
        document.querySelectorAll('#mr-rank-table thead th').forEach(th => {
            th.classList.remove('sorted-asc', 'sorted-desc');
            if (th.dataset.k === _sortKey) th.classList.add(_sortDir === 'desc' ? 'sorted-desc' : 'sorted-asc');
        });
        // row click → select
        tbody.querySelectorAll('tr').forEach(tr => {
            tr.addEventListener('click', () => {
                _selectedMission = +tr.dataset.mid;
                document.getElementById('mr-mission-select').value = _selectedMission;
                drawTrend(); drawDetail(); highlightRankRow();
            });
        });
    }

    function highlightRankRow() {
        document.querySelectorAll('#mr-rank-table tbody tr').forEach(tr => {
            tr.classList.toggle('is-selected', +tr.dataset.mid === _selectedMission);
        });
    }

    function drawInsights() {
        // Только per-mission сравнения — никаких cross-mission сумм (один чек
        // в нескольких миссиях = удвоение).
        const missions = byMission();
        const arr = [...missions.values()];

        // Sigh — only missions with material share_to (>0.5%) for growth/decline
        const material = arr.filter(m => (m.last_share_to || 0) > 0.005 && m.yoy_to != null);
        const grower = [...material].sort((a,b) => b.yoy_to - a.yoy_to)[0];
        const faller = [...material].sort((a,b) => a.yoy_to - b.yoy_to)[0];

        // Premium tilt
        const premium = arr.filter(m => m.last_price_index != null && (m.last_share_to || 0) > 0.005)
                            .sort((a,b) => b.last_price_index - a.last_price_index)[0];

        // Long tail: missions with last_share_to < 0.5%
        const tail = arr.filter(m => (m.last_share_to || 0) < 0.005);

        const card = (tag, accent, title, body) => `
            <div class="mr-insight" style="--mr-accent:${accent}">
                <div class="mr-insight-tag">${tag}</div>
                <div class="mr-insight-title">${title}</div>
                <div class="mr-insight-body">${body}</div>
            </div>
        `;

        document.getElementById('mr-insights').innerHTML = [
            card('Рост', GREEN,
                `«${grower?.mission ?? '—'}» растёт быстрее всех — <strong>${fmtPctSigned(grower?.yoy_to)}</strong> год к году`,
                `За последние 12 месяцев ${fmtBn(grower?.to_last12)} ₽ оборота миссии против ${fmtBn(grower?.to_prev12)} ₽ за предыдущие 12 месяцев. Доля чеков базы: ${fmtPct(grower?.last_share_checks)}.`
            ),
            card('Падение', RED,
                `«${faller?.mission ?? '—'}» — самое сильное падение: <strong>${fmtPctSigned(faller?.yoy_to)}</strong> год к году`,
                `${fmtBn(faller?.to_last12)} ₽ за последние 12 месяцев против ${fmtBn(faller?.to_prev12)} ₽ за предыдущие 12. Нужна диагностика — ассортимент, цена или поведенческий сдвиг?`
            ),
            card('Индекс цен корзины', PURPLE,
                `«${premium?.mission ?? '—'}» — самый высокий индекс цен корзины: <strong>${fmtPctSigned(premium?.last_price_index)}</strong>`,
                `Клиенты этой миссии покупают по ценам на ${fmtPctSigned(premium?.last_price_index)} дороже среднего клиента сети. Лояльность vs база: ${fmtPctSigned(premium?.last_penetration)}.`
            ),
            card('Длинный хвост', GRAY,
                `${tail.length} миссий с долей чеков &lt; 0.5%`,
                `Эти миссии тянут мало оборота индивидуально и часто пересекаются с более крупными. Кандидаты на слияние или удаление при следующей калибровке каталога.`
            ),
        ].join('');
    }

    // ----- Sub-tab switcher + init -----
    function initOnce() {
        if (built) return;
        if (typeof MISSION_REPORT_DATA === 'undefined') return;
        built = true;
        render(document.getElementById('mr-analytics-root'));
    }

    function wireSubTabs() {
        const switchEl = document.getElementById('missions-section-switch');
        if (!switchEl || switchEl.dataset.wired) return;
        switchEl.dataset.wired = '1';
        switchEl.addEventListener('click', e => {
            const btn = e.target.closest('.seg-channel-btn');
            if (!btn) return;
            switchEl.querySelectorAll('.seg-channel-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            const section = btn.dataset.section;
            document.getElementById('missions-section-catalog').classList.toggle('is-hidden', section !== 'catalog');
            document.getElementById('missions-section-analytics').classList.toggle('is-hidden', section !== 'analytics');
            const sub = document.getElementById('missions-page-subtitle');
            if (section === 'analytics') {
                if (sub) sub.textContent = 'Анализ I_MISSION_REPORT · динамика по месяцам · 52 миссии × 28 мес';
                initOnce();
            } else {
                if (sub) sub.textContent = '53 потребительские миссии · 30 787 продуктовых сопоставлений · 100% покрытие';
            }
        });
        // Дефолт = аналитика (как и в HTML). Запускаем рендер сразу.
        initOnce();
    }

    // Init switcher when missions tab becomes visible
    document.addEventListener('click', e => {
        const nav = e.target.closest('[data-tab="missions"]');
        if (nav) setTimeout(wireSubTabs, 60);
    });
    if (document.querySelector('#tab-missions.active')) setTimeout(wireSubTabs, 60);
})();

// --- Init: вкладка из URL (#tab=<key>) ---
applyTabFromHash();
