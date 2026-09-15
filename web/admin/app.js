/* Release Desk client — vanilla, no dependencies.
   Data: /queue · /schedule · /calendar (admin JSON feeds). */
(() => {
'use strict';

const root = location.pathname.replace(/\/$/, '');
const $ = (sel) => document.querySelector(sel);
const state = {
  queue: [], schedule: [], calendar: [],
  filter: 'all', query: '', sort: 'title-asc',
  loadedAt: null,
};

/* ---------------- utilities ---------------- */

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function fill(node, dataRole, value) {
  const target = node.querySelector(`[data-role="${dataRole}"]`);
  if (!target) return;
  if (value instanceof Node) { target.replaceChildren(value); return; }
  if (typeof value === 'boolean') { target.hidden = !value; return; }
  target.textContent = value === undefined || value === null ? '' : String(value);
  return target;
}

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function asDate(v) { const d = v ? new Date(v) : null; return d && !isNaN(d) ? d : null; }

function relDay(date) {
  const days = Math.round((date - Date.now()) / 86400000);
  if (days === 0) return 'today';
  if (days === 1) return 'tomorrow';
  if (days > 1 && days <= 30) return `in ${days} days`;
  if (days < 0 && days >= -30) return `${-days}d ago`;
  return null;
}
function fmtWhen(value) {
  const d = asDate(value);
  if (!d || d.getFullYear() < 1971) return '';
  const base = `${MONTHS[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
  const rel = relDay(d);
  return rel ? `${base} · ${rel}` : base;
}
function countdown(value) {
  const d = asDate(value);
  if (!d) return '';
  const rel = relDay(d);
  if (rel === 'today') return 'Airs today';
  if (rel === 'tomorrow') return 'Airs tomorrow';
  if (rel && rel.startsWith('in ')) return `Airs ${rel}`;
  if (d > new Date()) return 'Upcoming';
  return '';
}

/* deterministic hue from string → monogram gradient fallback */
function hashHue(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (h * 31 + str.charCodeAt(i)) >>> 0;
  return h % 360;
}
function setPoster(node, url, label) {
  const title = (label || '?').trim();
  if (url) {
    node.style.backgroundImage = `url("${url.replace(/"/g, '%22')}")`;
    node.textContent = '';
    return;
  }
  node.style.backgroundImage =
    `linear-gradient(145deg, hsl(${hashHue(title)} 32% 22%), hsl(${(hashHue(title) + 40) % 360} 30% 14%))`;
  node.textContent = title.slice(0, 1).toUpperCase() || '?';
}

/* ---------------- toasts & modal ---------------- */

function toast(message, kind) {
  const host = $('#toasts');
  const t = el('div', 'toast' + (kind === 'error' ? ' error' : ''), message);
  host.appendChild(t);
  setTimeout(() => { t.classList.add('gone'); setTimeout(() => t.remove(), 300); }, 4500);
}

let modalResolve = null;
function confirmRemove(titleText) {
  $('#modal-title').textContent = 'Remove from monitor queue?';
  $('#modal-body').textContent =
    `"${titleText}" will stop being tracked for release. Registered library items are not affected.`;
  const rootEl = $('#modal-root');
  rootEl.hidden = false;
  $('#modal-cancel').focus();
  return new Promise((resolve) => { modalResolve = resolve; });
}
function closeModal(result) {
  $('#modal-root').hidden = true;
  if (modalResolve) { modalResolve(result); modalResolve = null; }
}
$('#modal-confirm').addEventListener('click', () => closeModal(true));
$('#modal-cancel').addEventListener('click', () => closeModal(false));
$('.modal-backdrop').addEventListener('click', () => closeModal(false));
document.addEventListener('keydown', (e) => {
  if ($('#modal-root').hidden) return;
  if (e.key === 'Escape') closeModal(false);
  if (e.key === 'Tab') {
    const focusables = $('#modal-root').querySelectorAll('button');
    const first = focusables[0], last = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) { last.focus(); e.preventDefault(); }
    else if (!e.shiftKey && document.activeElement === last) { first.focus(); e.preventDefault(); }
  }
});

/* ---------------- actions ---------------- */

async function postAction(path, key, btn) {
  if (btn) {
    if (btn.disabled) return {};
    btn.disabled = true;
    setTimeout(() => { try { btn.disabled = false; } catch (_) {} }, 1500);
  }
  let data = {};
  try {
    const res = await fetch(`${root}/queue/${path}?key=${encodeURIComponent(key || '')}`, { method: 'POST' });
    data = await res.json();
    if (!res.ok) toast(data.error || data.message || 'Request failed', 'error');
  } catch (_) {
    toast('Network error while contacting Vio', 'error');
  }
  await load();
  return data;
}

async function refreshSchedules(btn) {
  if (!btn || btn.disabled) return;
  btn.disabled = true;
  const original = btn.innerHTML;
  btn.innerHTML = 'Refreshing…';
  try {
    const res = await fetch(`${root}/schedule/refresh`, { method: 'POST' });
    const data = await res.json();
    if (!res.ok) toast(data.error || 'Schedule refresh failed', 'error');
    else {
      const count = data.summary && data.summary.count != null ? data.summary.count : '?';
      toast(`Schedules refreshed — ${count} show(s) tracked`);
    }
  } catch (_) {
    toast('Network error during schedule refresh', 'error');
  } finally {
    btn.innerHTML = original;
    btn.disabled = false;
  }
  await load();
}

/* ---------------- rendering ---------------- */

function sectionError(containerId, message, retry) {
  const box = $(containerId);
  box.hidden = false;
  box.className = 'section-msg error';
  box.replaceChildren(
    el('div', '', message),
    retry ? Object.assign(el('button', 'btn small', 'Retry'), { type: 'button', onclick: () => load() }) : null
  );
}
function clearSection(containerId) { const b = $(containerId); b.hidden = true; b.replaceChildren(); }

function renderQueue() {
  const list = $('#queue-list');
  const q = state.query.trim().toLowerCase();
  let items = state.queue.filter((i) => {
    if (state.filter === 'movie' && i.media_type !== 'movie') return false;
    if (state.filter === 'series' && i.media_type !== 'series') return false;
    if (state.filter === 'forced' && !i.force) return false;
    if (q) {
      const hay = `${i.title || ''} ${i.imdb_id || ''} ${i.key || ''}`.toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
  const sorters = {
    'title-asc': (a, b) => (a.title || a.key).localeCompare(b.title || b.key),
    'title-desc': (a, b) => (b.title || b.key).localeCompare(a.title || a.key),
    'year-desc': (a, b) => (b.year || 0) - (a.year || 0),
    'release-asc': (a, b) => {
      const da = asDate(a.release), db = asDate(b.release);
      if (!da && !db) return 0;
      if (!da) return 1; if (!db) return -1;
      return da - db;
    },
  };
  items.sort(sorters[state.sort] || sorters['title-asc']);

  $('#badge-queue').textContent = state.queue.length;

  if (!items.length) {
    list.replaceChildren(emptyState(
      q || state.filter !== 'all' ? 'Nothing matches this view' : 'The monitor queue is clear',
      q || state.filter !== 'all' ? 'Try a different search or filter.' : 'Requested media will appear here while waiting for release.'
    ));
    return;
  }

  const frag = document.createDocumentFragment();
  const tpl = $('#tpl-queue-row');
  for (const item of items) {
    const row = tpl.content.firstElementChild.cloneNode(true);
    const displayTitle = item.title || item.key;
    setPoster(fill(row, 'poster'), item.poster, displayTitle);
    fill(row, 'title', displayTitle);
    fill(row, 'year', item.year ? `(${item.year})` : '');

    const typePill = fill(row, 'type', item.media_type === 'series' ? 'Series' : 'Movie');
    typePill.classList.add(item.media_type === 'series' ? 'accent' : 'subtle');

    const missingEps = (item.episodes || []).filter((e) => e.season > 0 && e.episode > 0);
    if (item.force) {
      const s = fill(row, 'state', 'Force-added');
      s.classList.add('warn');
    } else {
      fill(row, 'state', 'Waiting');
    }
    if (missingEps.length && item.media_type === 'series') {
      const pill = fill(row, 'missing', `${missingEps.length} upcoming episode${missingEps.length === 1 ? '' : 's'}`);
      pill.hidden = false;
      const sub = fill(row, 'missing-list',
        'Next: ' + missingEps.slice(0, 3)
          .map((e) => `S${String(e.season).padStart(2, '0')}E${String(e.episode).padStart(2, '0')}`)
          .join(' · '));
      sub.hidden = false;
    }

    const when = fmtWhen(item.release);
    const whenNode = fill(row, 'when', when || 'Release date unknown');
    if (item.release && asDate(item.release) && relDay(asDate(item.release))) whenNode.classList.add('soon');

    for (const btn of row.querySelectorAll('[data-action]')) {
      btn.dataset.key = item.key;
      btn.addEventListener('click', onQueueAction);
    }
    const searchBtn = row.querySelector('[data-action="search"]');
    if (item.force && searchBtn) searchBtn.remove();
    frag.appendChild(row);
  }
  list.replaceChildren(frag);
}

async function onQueueAction(e) {
  const btn = e.currentTarget;
  const key = btn.dataset.key;
  const action = btn.dataset.action;
  if (action === 'remove') {
    const item = state.queue.find((i) => i.key === key);
    const ok = await confirmRemove(item ? item.title || key : key);
    if (!ok) return;
    const d = await postAction('remove', key, btn);
    if (d.message) toast(d.message);
    return;
  }
  if (action === 'force') {
    const d = await postAction('force', key, btn);
    if (d.message) toast(d.message);
    return;
  }
  if (action === 'search') {
    const d = await postAction('search', key, btn);
    if (!d || d.releases === undefined) return;
    if (!d.releases.length) { toast(`No releases returned for ${d.title || key}`, 'error'); return; }
    const top = d.releases.slice(0, 2).map((r) => r.title).join(' | ');
    toast(
      (d.matched ? `Release found for ${d.title || ''}` : `No exact match for ${d.title || ''}`) +
      ` — ${d.releases.length} result(s): ${top}`
    );
  }
}

function renderSchedule() {
  const host = $('#schedule-groups');
  const shows = state.schedule || [];
  $('#badge-schedule').textContent = shows.length;

  if (!shows.length) {
    host.replaceChildren(emptyState('No series schedules tracked yet', 'Tracked shows appear here with their next air dates.'));
    return;
  }
  const now = Date.now();
  const WEEK = 7 * 86400000;
  const withNext = shows.map((s) => {
    const eps = Object.values(s.episodes || {})
      .filter((e) => e && asDate(e.air_date))
      .sort((a, b) => asDate(a.air_date) - asDate(b.air_date));
    const upcoming = eps.filter((e) => asDate(e.air_date) > now);
    return { ...s, eps, upcoming, next: upcoming[0] || null };
  });
  withNext.sort((a, b) => (a.next ? asDate(a.next.air_date) : Infinity) - (b.next ? asDate(b.next.air_date) : Infinity));

  const groups = [
    ['This week', (s) => s.next && asDate(s.next.air_date) - now <= WEEK],
    ['Later', (s) => s.next && asDate(s.next.air_date) - now > WEEK],
    ['Between seasons or ended', (s) => !s.next],
  ];
  const frag = document.createDocumentFragment();
  const tpl = $('#tpl-schedule-card');
  for (const [label, pred] of groups) {
    const groupShows = withNext.filter(pred);
    if (!groupShows.length) continue;
    const heading = el('h3', 'sched-group-title', `${label} (${groupShows.length})`);
    frag.appendChild(heading);
    for (const show of groupShows) {
      const card = tpl.content.firstElementChild.cloneNode(true);
      setPoster(fill(card, 'poster'), '', show.title || show.imdb_id);
      fill(card, 'title', show.title || show.imdb_id);
      fill(card, 'imdb', show.imdb_id);
      const statusPill = fill(card, 'status', show.status || 'Unknown');
      if ((show.status || '').toLowerCase() === 'ended') statusPill.classList.add('warn');
      const cd = show.next ? countdown(show.next.air_date) : '';
      fill(card, 'next', cd || 'No future episodes');
      const ol = card.querySelector('[data-role="eps"]');
      for (const ep of show.upcoming.slice(0, 3)) {
        const li = el('li');
        const b = el('b', '', `S${String(ep.season).padStart(2, '0')}E${String(ep.episode).padStart(2, '0')}`);
        li.appendChild(b);
        li.appendChild(document.createTextNode(
          (ep.title ? ep.title + ' · ' : '') + fmtWhen(ep.air_date)));
        ol.appendChild(li);
      }
      if (!ol.children.length) ol.remove();
      frag.appendChild(card);
    }
  }
  host.replaceChildren(frag);
}

function renderCalendar() {
  const list = $('#calendar-list');
  const items = state.calendar || [];
  $('#badge-calendar').textContent = items.length;
  if (!items.length) {
    list.replaceChildren(emptyState('No upcoming release dates known', 'Titles appear once metadata provides a home-media date.'));
    return;
  }
  const sorted = [...items].sort((a, b) => (asDate(a.release) || Infinity) - (asDate(b.release) || Infinity));
  const frag = document.createDocumentFragment();
  const tpl = $('#tpl-calendar-card');
  for (const item of sorted) {
    const card = tpl.content.firstElementChild.cloneNode(true);
    const d = asDate(item.release);
    fill(card, 'day', d ? d.getDate() : '—');
    fill(card, 'month', d ? MONTHS[d.getMonth()] : '');
    fill(card, 'title', item.title || item.key);
    fill(card, 'sub', fmtWhen(item.release) || 'Home-media release gate is being monitored.');
    fill(card, 'type', item.media_type === 'series' ? 'Series' : 'Movie');
    frag.appendChild(card);
  }
  list.replaceChildren(frag);
}

function emptyState(title, body) {
  const wrap = el('div', 'empty');
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('aria-hidden', 'true');
  const use = document.createElementNS('http://www.w3.org/2000/svg', 'use');
  use.setAttribute('href', '#i-inbox');
  svg.appendChild(use);
  wrap.appendChild(svg);
  wrap.appendChild(el('strong', '', title));
  wrap.appendChild(el('div', '', body));
  return wrap;
}

/* ---------------- data loading ---------------- */

async function fetchJSON(path) {
  const res = await fetch(root + path);
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return res.json();
}

async function load() {
  const [q, s, c] = await Promise.allSettled([
    fetchJSON('/queue'), fetchJSON('/schedule'), fetchJSON('/calendar'),
  ]);
  if (q.status === 'fulfilled') {
    state.queue = q.value.items || [];
    clearSection('#queue-status');
    renderQueue();
  } else {
    sectionError('#queue-status', 'Queue unavailable. Check the plugin connection.', true);
    $('#queue-list').replaceChildren();
    $('#badge-queue').textContent = '·';
  }
  if (s.status === 'fulfilled') {
    state.schedule = (s.value.shows || []).filter(Boolean);
    clearSection('#schedule-status');
    renderSchedule();
  } else {
    sectionError('#schedule-status', 'Schedule unavailable.', true);
    $('#schedule-groups').replaceChildren();
    $('#badge-schedule').textContent = '·';
  }
  if (c.status === 'fulfilled') {
    state.calendar = c.value.items || [];
    clearSection('#calendar-status');
    renderCalendar();
  } else {
    sectionError('#calendar-status', 'Calendar unavailable.', true);
    $('#calendar-list').replaceChildren();
    $('#badge-calendar').textContent = '·';
  }
  state.loadedAt = Date.now();
  stampFreshness();
  $('#kpi-monitored').textContent = state.queue.length;
  $('#kpi-waiting').textContent = state.queue.filter((i) => !i.ready).length;
  $('#kpi-forced').textContent = state.queue.filter((i) => i.force).length;
  $('#kpi-shows').textContent = state.schedule.length;
}

function stampFreshness() {
  const stamp = $('#live-stamp');
  if (!state.loadedAt) return;
  const secs = Math.max(0, Math.round((Date.now() - state.loadedAt) / 1000));
  stamp.innerHTML = '<span class="dot"></span> ';
  stamp.appendChild(document.createTextNode(secs < 5 ? 'Updated just now' : `Updated ${secs}s ago`));
  stamp.classList.toggle('is-fresh', secs < 60);
}

/* ---------------- tabs & toolbar wiring ---------------- */

function selectView(name) {
  for (const tab of document.querySelectorAll('.tab')) {
    const active = tab.dataset.view === name;
    tab.classList.toggle('is-active', active);
    tab.setAttribute('aria-selected', active ? 'true' : 'false');
  }
  for (const view of document.querySelectorAll('.view')) view.hidden = view.id !== `view-${name}`;
  if (location.hash !== `#${name}`) history.replaceState(null, '', `#${name}`);
}
for (const tab of document.querySelectorAll('.tab')) {
  tab.addEventListener('click', () => selectView(tab.dataset.view));
}
document.addEventListener('keydown', (e) => {
  if (e.target.matches('input,select,textarea')) return;
  const tabs = [...document.querySelectorAll('.tab')];
  const idx = tabs.findIndex((t) => t.classList.contains('is-active'));
  if (e.key === 'ArrowRight') { tabs[(idx + 1) % tabs.length].focus(); tabs[(idx + 1) % tabs.length].click(); }
  if (e.key === 'ArrowLeft') { tabs[(idx - 1 + tabs.length) % tabs.length].focus(); tabs[(idx - 1 + tabs.length) % tabs.length].click(); }
});

$('#q-search').addEventListener('input', (e) => { state.query = e.target.value; renderQueue(); });
for (const chip of document.querySelectorAll('.chip')) {
  chip.addEventListener('click', () => {
    for (const c of document.querySelectorAll('.chip')) c.classList.toggle('is-active', c === chip);
    state.filter = chip.dataset.filter;
    renderQueue();
  });
}
$('#q-sort').addEventListener('change', (e) => { state.sort = e.target.value; renderQueue(); });
$('#btn-refresh-schedules').addEventListener('click', (e) => refreshSchedules(e.currentTarget));
setInterval(stampFreshness, 10000);

/* ---------------- theme & navigation ---------------- */
const themeParam = new URLSearchParams(location.search).get('theme');
if (themeParam) {
  document.documentElement.dataset.theme = themeParam;
} else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
  document.documentElement.dataset.theme = 'light';
}

const backBtn = $('#btn-back');
if (backBtn && window.history.length > 1) {
  backBtn.addEventListener('click', (e) => {
    e.preventDefault();
    window.history.back();
  });
}

/* ---------------- boot ---------------- */
const initial = (location.hash || '#queue').replace('#', '');
selectView(['queue', 'schedule', 'calendar'].includes(initial) ? initial : 'queue');
load();
})();
