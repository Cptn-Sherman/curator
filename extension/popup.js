// ── Config ────────────────────────────────────────────────────────────────────

let curatorUrl = 'http://localhost:8080';
let statusRefreshInterval = null;
let countdownInterval = null;
let lastConnectionOk = null;
let lastConnectionError = '';
let lastPing = null;
let progressTimeout = null;
let progressEmptyStreak = 0;
const PROGRESS_FAST_MS = 1000;
const PROGRESS_SLOW_MS = 10000;
const PROGRESS_SLOW_AFTER = 3;

browser.storage.local.get(['curatorUrl'], (result) => {
    if (result.curatorUrl) curatorUrl = result.curatorUrl;
});

// ── Tab navigation ────────────────────────────────────────────────────────────

function switchTab(tabId) {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

    const btn = document.querySelector(`.tab-btn[data-tab="${tabId}"]`);
    if (btn) btn.classList.add('active');
    document.getElementById(tabId).classList.add('active');


    if (tabId === 'status') {
        loadStatus();
        startStatusRefresh();
    } else {
        stopStatusRefresh();
    }
    if (tabId === 'save') loadBatchTabs();
    if (tabId === 'settings') loadSettingsTab();
}

document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
});


// ── Status auto-refresh ───────────────────────────────────────────────────────

function updateRefreshCountdown() {
    if (countdownInterval) {
        clearInterval(countdownInterval);
        countdownInterval = null;
    }
    const button = document.getElementById('refresh-status-btn');
    let timeLeft = 10;
    const update = () => {
        button.textContent = `Refresh (${timeLeft}s)`;
        timeLeft--;
        if (timeLeft < 0) {
            clearInterval(countdownInterval);
            countdownInterval = null;
            button.textContent = 'Refresh';
        }
    };
    update();
    countdownInterval = setInterval(update, 1000);
}

function startStatusRefresh() {
    if (statusRefreshInterval) clearInterval(statusRefreshInterval);
    updateRefreshCountdown();
    statusRefreshInterval = setInterval(() => {
        loadStatus();
        updateRefreshCountdown();
    }, 10000);
}

function stopStatusRefresh() {
    if (statusRefreshInterval) {
        clearInterval(statusRefreshInterval);
        statusRefreshInterval = null;
    }
    if (countdownInterval) {
        clearInterval(countdownInterval);
        countdownInterval = null;
    }
    const button = document.getElementById('refresh-status-btn');
    button.textContent = 'Refresh';
}

// ── Save tab ──────────────────────────────────────────────────────────────────

function initializePopup() {
    browser.storage.local.get(['tabScope'], result => {
        const scope = result.tabScope || 'all';
        const radio = document.querySelector(`input[name="tab-scope"][value="${scope}"]`);
        if (radio) radio.checked = true;
    });
    document.getElementById('server-url').textContent = curatorUrl;
    loadBatchTabs();
}

document.getElementById('save-current-btn').addEventListener('click', saveCurrentPage);

async function saveCurrentPage() {
    const btn = document.getElementById('save-current-btn');
    const msgEl = document.getElementById('batch-message');
    btn.disabled = true;
    btn.textContent = 'Saving…';
    try {
        const [tab] = await browser.tabs.query({ active: true, currentWindow: true });
        if (!tab || !isSaveableUrl(tab.url)) {
            showMessage('Current page cannot be saved', 'error', msgEl, 3000);
            return;
        }
        const response = await fetch(`${curatorUrl}/ingest`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ links: [tab.url] }),
            mode: 'cors',
            credentials: 'omit'
        });
        const data = await response.json();
        if (data.ingested_count > 0) {
            showMessage('✓ Current page saved', 'success', msgEl, 3000);
            boostProgressPolling();
        } else if (data.duplicate_count > 0) {
            showMessage('⚠ Already saved', 'info', msgEl, 3000);
        } else {
            showMessage('Failed to save page', 'error', msgEl, 3000);
        }
    } catch (err) {
        showMessage(`Error: ${err.message}`, 'error', msgEl, 3000);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Save Current Page';
    }
}

// ── Status tab ────────────────────────────────────────────────────────────────

document.getElementById('refresh-status-btn').addEventListener('click', () => {
    startStatusRefresh();
});

async function pingServer() {
    try {
        const startTime = performance.now();
        const response = await fetch(`${curatorUrl}/health`, { mode: 'cors', credentials: 'omit' });
        lastPing = Math.round(performance.now() - startTime);
        lastConnectionOk = response.ok;
        if (!response.ok) lastConnectionError = `Server returned status ${response.status}`;
        else lastConnectionError = '';
    } catch (error) {
        lastConnectionOk = false;
        lastConnectionError = error.message;
        lastPing = null;
    }
    updateConnectionStatusLine();
}

function updateConnectionStatusLine() {
    const statusEl = document.getElementById('connection-status');
    if (lastConnectionOk) {
        statusEl.textContent = `Connected (${lastPing}ms)`;
        statusEl.style.color = '#10b981';
    } else {
        statusEl.textContent = lastConnectionError ? `Disconnected (${lastConnectionError})` : 'Disconnected';
        statusEl.style.color = '#ef4444';
    }
}

async function loadStatus() {
    document.getElementById('server-url').textContent = curatorUrl;
    await pingServer();
    try {
        const [, statsRes, processingRes, subsRes] = await Promise.all([
            fetch(`${curatorUrl}/check?url=dummy`),
            fetch(`${curatorUrl}/links/stats`),
            fetch(`${curatorUrl}/processing/status`),
            fetch(`${curatorUrl}/subscriptions`)
        ]);
        const statsData = await statsRes.json();
        const processingData = await processingRes.json();
        const subsData = await subsRes.json();
        document.getElementById('db-name').textContent = 'curator.db';
        document.getElementById('unprocessed-count').textContent = statsData.unprocessed ?? 0;
        document.getElementById('total-links').textContent = statsData.total ?? 0;
        document.getElementById('processing-checkbox').checked = processingData.processing_enabled;
        document.getElementById('subscriptions-count').textContent = subsData.total_count || 0;
    } catch (error) {
        console.error('Error loading status:', error);
    }
}

// ── Settings tab ──────────────────────────────────────────────────────────────

function loadSettingsTab() {
    document.getElementById('settings-url').value = curatorUrl;
    loadSettingsSubscriptions();
    loadIgnoredDomains();
}

document.querySelectorAll('input[name="tab-scope"]').forEach(radio => {
    radio.addEventListener('change', () => {
        browser.storage.local.set({ tabScope: radio.value });
        loadBatchTabs();
    });
});

// Server URL
document.getElementById('settings-save-url-btn').addEventListener('click', () => {
    const val = document.getElementById('settings-url').value.trim();
    const msgEl = document.getElementById('settings-url-message');
    if (!val) { showMessage('Please enter a URL', 'error', msgEl); return; }
    curatorUrl = val;
    browser.storage.local.set({ curatorUrl: val }, () => {
        showMessage('✓ Saved', 'success', msgEl, 2000);
        document.getElementById('server-url').textContent = val;
    });
});

// Processing toggle
document.getElementById('processing-checkbox').addEventListener('change', async () => {
    const checkbox = document.getElementById('processing-checkbox');
    const messageEl = document.getElementById('management-message');
    checkbox.disabled = true;
    try {
        const response = await fetch(`${curatorUrl}/processing/toggle`, { method: 'POST' });
        const data = await response.json();
        showMessage(data.message, 'success', messageEl, 2000);
        startStatusRefresh();
    } catch (error) {
        checkbox.checked = !checkbox.checked;
        showMessage(`Error: ${error.message}`, 'error', messageEl);
    } finally {
        checkbox.disabled = false;
    }
});

// Subscriptions
async function loadSettingsSubscriptions() {
    const list = document.getElementById('subscriptions-list');
    try {
        const response = await fetch(`${curatorUrl}/subscriptions`);
        const data = await response.json();
        list.innerHTML = '';
        if (data.subscriptions && data.subscriptions.length > 0) {
            data.subscriptions.forEach(sub => {
                const item = document.createElement('div');
                item.className = 'list-item';
                item.innerHTML = `
                    <div>
                        <div class="list-item-name">${escapeHtml(sub.name)}</div>
                        <div class="list-item-url">${escapeHtml(sub.url)}</div>
                    </div>`;
                list.appendChild(item);
            });
        } else {
            list.innerHTML = '<div class="list-empty">No subscriptions yet.</div>';
        }
    } catch (error) {
        list.innerHTML = '<div class="list-empty">Unable to load subscriptions.</div>';
    }
}

document.getElementById('add-subscription-btn').addEventListener('click', () => {
    document.getElementById('subscription-form').style.display = 'block';
    document.getElementById('add-subscription-btn').style.display = 'none';
});

document.getElementById('cancel-sub-btn').addEventListener('click', () => {
    document.getElementById('subscription-form').style.display = 'none';
    document.getElementById('add-subscription-btn').style.display = '';
    clearSubscriptionForm();
});

document.getElementById('save-sub-btn').addEventListener('click', async () => {
    const name = document.getElementById('sub-name').value.trim();
    const url = document.getElementById('sub-url').value.trim();
    const interval = parseInt(document.getElementById('sub-interval').value);
    if (!name || !url) { alert('Please fill in all fields'); return; }

    const btn = document.getElementById('save-sub-btn');
    btn.disabled = true;
    btn.textContent = 'Saving…';
    try {
        const response = await fetch(`${curatorUrl}/subscriptions`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, url, polling_interval_seconds: interval })
        });
        if (response.ok) {
            clearSubscriptionForm();
            document.getElementById('subscription-form').style.display = 'none';
            document.getElementById('add-subscription-btn').style.display = '';
            loadSettingsSubscriptions();
        } else {
            alert('Failed to create subscription');
        }
    } catch (error) {
        alert(`Error: ${error.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Save';
    }
});

function clearSubscriptionForm() {
    document.getElementById('sub-name').value = '';
    document.getElementById('sub-url').value = '';
    document.getElementById('sub-interval').value = '3600';
}

// Ignored domains — stored server-side for persistence across extension reinstalls
async function loadIgnoredDomains() {
    const list = document.getElementById('ignored-domains-list');
    try {
        const data = await fetch(`${curatorUrl}/ignored-domains`).then(r => r.json());
        const domains = (data.domains || []).map(d => d.domain);
        renderIgnoredDomains(domains);
    } catch {
        list.innerHTML = '<div class="list-empty">Could not load (server unreachable).</div>';
    }
}

function renderIgnoredDomains(domains) {
    const list = document.getElementById('ignored-domains-list');
    list.innerHTML = '';
    if (domains.length === 0) {
        list.innerHTML = '<div class="list-empty">No ignored domains.</div>';
        return;
    }
    domains.forEach(domain => {
        const item = document.createElement('div');
        item.className = 'list-item';
        item.innerHTML = `
            <div class="list-item-name">${escapeHtml(domain)}</div>
            <button class="btn-remove" title="Remove">✕</button>`;
        item.querySelector('.btn-remove').addEventListener('click', async () => {
            await fetch(`${curatorUrl}/ignored-domains/${encodeURIComponent(domain)}`, { method: 'DELETE' });
            loadIgnoredDomains();
        });
        list.appendChild(item);
    });
}

async function addIgnoredDomain() {
    const input = document.getElementById('ignored-domain-input');
    const raw = input.value.trim().toLowerCase();
    if (!raw) return;
    let domain = raw;
    try { domain = new URL(raw.includes('://') ? raw : `https://${raw}`).hostname; } catch {}
    await fetch(`${curatorUrl}/ignored-domains`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ domain }),
    });
    input.value = '';
    loadIgnoredDomains();
}

document.getElementById('add-ignored-domain-btn').addEventListener('click', addIgnoredDomain);
document.getElementById('ignored-domain-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); addIgnoredDomain(); }
});

// Backup
document.getElementById('backup-btn').addEventListener('click', async () => {
    const btn = document.getElementById('backup-btn');
    const msgEl = document.getElementById('backup-message');
    btn.disabled = true;
    btn.textContent = 'Creating…';
    try {
        const response = await fetch(`${curatorUrl}/backup`, { method: 'POST', mode: 'cors', credentials: 'omit' });
        const data = await response.json();
        if (data.error) {
            showMessage(`Error: ${data.error}`, 'error', msgEl);
        } else {
            showMessage(`✓ Backup saved to ${data.path}`, 'success', msgEl);
        }
    } catch (err) {
        showMessage(`Error: ${err.message}`, 'error', msgEl);
    } finally {
        btn.disabled = false;
        btn.textContent = 'Create Backup';
    }
});

// Debug reset
document.getElementById('debug-reset-btn').addEventListener('click', async () => {
    if (!confirm('Delete ALL links and subscriptions from the database? This cannot be undone.')) return;
    const btn = document.getElementById('debug-reset-btn');
    const msgEl = document.getElementById('debug-message');
    btn.disabled = true;
    try {
        const response = await fetch(`${curatorUrl}/debug/reset`, { method: 'POST', mode: 'cors', credentials: 'omit' });
        const data = await response.json();
        const parts = [
            data.links_deleted && `${data.links_deleted} links`,
            data.subscriptions_deleted && `${data.subscriptions_deleted} subscriptions`,
            data.sources_deleted && `${data.sources_deleted} sources`,
            data.creators_deleted && `${data.creators_deleted} creators`,
            data.tags_deleted && `${data.tags_deleted} tags`,
            data.content_deleted && `${data.content_deleted} content`,
        ].filter(Boolean);
        showMessage(`Deleted: ${parts.join(', ') || 'nothing'}`, 'success', msgEl);
        loadSettingsSubscriptions();
    } catch (err) {
        showMessage(`Error: ${err.message}`, 'error', msgEl);
    } finally {
        btn.disabled = false;
    }
});

// ── Batch tab ─────────────────────────────────────────────────────────────────

let batchSavedTabIds = [];

const ALWAYS_IGNORED_DOMAINS = new Set(['localhost', '127.0.0.1', '::1']);

function isSaveableUrl(url) {
    if (!url || (!url.startsWith('http://') && !url.startsWith('https://'))) return false;
    try { return !ALWAYS_IGNORED_DOMAINS.has(new URL(url).hostname); } catch { return false; }
}

function getDomain(url) {
    try { return new URL(url).hostname; } catch { return url; }
}

function updateBatchCount() {
    const count = document.querySelectorAll('.batch-tab-cb:checked').length;
    document.getElementById('batch-selected-count').textContent = `${count} selected`;
    document.getElementById('batch-save-btn').disabled = count === 0;
    document.getElementById('batch-save-close-btn').disabled = count === 0;
}

function updateDomainCb(domainCb) {
    const group = domainCb.closest('.batch-domain-group');
    const cbs = [...group.querySelectorAll('.batch-tab-cb')];
    const checked = cbs.filter(c => c.checked).length;
    domainCb.checked = checked === cbs.length;
    domainCb.indeterminate = checked > 0 && checked < cbs.length;
}

document.getElementById('batch-select-all').addEventListener('click', () => {
    document.querySelectorAll('.batch-tab-cb, .batch-domain-cb').forEach(cb => {
        cb.checked = true; cb.indeterminate = false;
    });
    updateBatchCount();
});

document.getElementById('batch-deselect-all').addEventListener('click', () => {
    document.querySelectorAll('.batch-tab-cb, .batch-domain-cb').forEach(cb => {
        cb.checked = false; cb.indeterminate = false;
    });
    updateBatchCount();
});

document.getElementById('batch-save-btn').addEventListener('click', () => batchSave(false));
document.getElementById('batch-save-close-btn').addEventListener('click', () => batchSave(true));

function loadBatchTabs() {
    const list = document.getElementById('batch-tabs-list');
    list.innerHTML = '<p class="batch-empty">Loading tabs…</p>';
    document.getElementById('batch-message').className = 'message';
    document.getElementById('batch-message').textContent = '';
    batchSavedTabIds = [];

    Promise.all([
        browser.storage.local.get(['tabScope']),
        fetch(`${curatorUrl}/ignored-domains`).then(r => r.json()).catch(() => ({ domains: [] })),
    ]).then(([stored, ignoredData]) => {
        const query = stored.tabScope === 'current' ? { currentWindow: true } : {};
        return Promise.all([browser.tabs.query(query), Promise.resolve(stored), Promise.resolve(ignoredData)]);
    }).then(([tabs, stored, ignoredData]) => {
        const ignoredDomains = new Set((ignoredData.domains || []).map(d => d.domain));
        const groups = new Map();
        for (const tab of tabs) {
            if (!isSaveableUrl(tab.url)) continue;
            const domain = getDomain(tab.url);
            if (!groups.has(domain)) groups.set(domain, []);
            groups.get(domain).push(tab);
        }
        const sorted = new Map([...groups.entries()].sort(([a], [b]) => {
            const aIgnored = ignoredDomains.has(a) ? 1 : 0;
            const bIgnored = ignoredDomains.has(b) ? 1 : 0;
            if (aIgnored !== bIgnored) return aIgnored - bIgnored;
            return a.localeCompare(b);
        }));

        list.innerHTML = '';
        if (sorted.size === 0) {
            list.innerHTML = '<p class="batch-empty">No saveable tabs found.</p>';
            return;
        }

        for (const [domain, domainTabs] of sorted) {
            const ignored = ignoredDomains.has(domain);
            const group = document.createElement('div');
            group.className = 'batch-domain-group' + (ignored ? ' batch-domain-ignored' : '');

            const header = document.createElement('div');
            header.className = 'batch-domain-header';

            const domainCb = document.createElement('input');
            domainCb.type = 'checkbox';
            domainCb.checked = !ignored;
            domainCb.className = 'batch-domain-cb';

            const nameSpan = document.createElement('span');
            nameSpan.className = 'batch-domain-name';
            nameSpan.textContent = domain;

            const countSpan = document.createElement('span');
            countSpan.className = 'batch-domain-count';
            countSpan.textContent = `${domainTabs.length}`;

            const ignoreBtn = document.createElement('button');
            ignoreBtn.className = 'batch-ignore-btn';
            ignoreBtn.title = ignored ? 'Remove from ignored' : 'Always ignore this domain';
            ignoreBtn.textContent = ignored ? 'unignore' : 'ignore';
            ignoreBtn.addEventListener('click', async e => {
                e.stopPropagation();
                if (ignoredDomains.has(domain)) {
                    await fetch(`${curatorUrl}/ignored-domains/${encodeURIComponent(domain)}`, { method: 'DELETE' });
                } else {
                    await fetch(`${curatorUrl}/ignored-domains`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ domain }),
                    });
                }
                loadBatchTabs();
            });

            header.appendChild(domainCb);
            header.appendChild(nameSpan);
            header.appendChild(countSpan);
            header.appendChild(ignoreBtn);

            header.addEventListener('click', e => {
                if (e.target === domainCb || e.target === ignoreBtn) return;
                domainCb.checked = !domainCb.checked;
                group.querySelectorAll('.batch-tab-cb').forEach(cb => { cb.checked = domainCb.checked; });
                updateBatchCount();
            });
            domainCb.addEventListener('change', () => {
                group.querySelectorAll('.batch-tab-cb').forEach(cb => { cb.checked = domainCb.checked; });
                updateBatchCount();
            });

            group.appendChild(header);

            for (const tab of domainTabs) {
                const row = document.createElement('div');
                row.className = 'batch-tab-row';

                const cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.checked = !ignored;
                cb.className = 'batch-tab-cb';
                cb.dataset.url = tab.url;
                cb.dataset.tabId = tab.id;
                cb.addEventListener('change', () => { updateDomainCb(domainCb); updateBatchCount(); });

                const info = document.createElement('div');
                info.className = 'batch-tab-info';
                info.innerHTML = `<div class="batch-tab-title">${escapeHtml(tab.title || tab.url)}</div>
                                  <div class="batch-tab-url">${escapeHtml(tab.url)}</div>`;

                row.appendChild(cb);
                row.appendChild(info);
                group.appendChild(row);
            }

            list.appendChild(group);
        }
        updateBatchCount();
    });
}

async function batchSave(andClose = false) {
    const saveBtn = document.getElementById('batch-save-btn');
    const saveCloseBtn = document.getElementById('batch-save-close-btn');
    const msgEl = document.getElementById('batch-message');
    const checked = [...document.querySelectorAll('.batch-tab-cb:checked')];
    const urls = checked.map(cb => cb.dataset.url);
    batchSavedTabIds = checked.map(cb => parseInt(cb.dataset.tabId));

    saveBtn.disabled = true;
    saveCloseBtn.disabled = true;
    saveBtn.textContent = 'Saving…';
    msgEl.className = 'message';
    msgEl.textContent = '';
    boostProgressPolling();

    try {
        const response = await fetch(`${curatorUrl}/ingest`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ links: urls }),
            mode: 'cors',
            credentials: 'omit'
        });
        const data = await response.json();
        if (andClose && batchSavedTabIds.length > 0) {
            browser.tabs.remove(batchSavedTabIds).then(() => loadBatchTabs());
            return;
        }
        const saved = data.ingested_count || 0;
        const dupes = data.duplicate_count || 0;
        const parts = [];
        if (saved > 0) parts.push(`${saved} saved`);
        if (dupes > 0) parts.push(`${dupes} already existed`);
        msgEl.textContent = parts.join(', ') || 'Nothing saved';
        msgEl.className = 'message show success';
    } catch (err) {
        msgEl.textContent = `Error: ${err.message}`;
        msgEl.className = 'message show error';
    } finally {
        saveBtn.disabled = false;
        saveCloseBtn.disabled = false;
        saveBtn.textContent = 'Save Selected';
    }
}

// ── Progress strip ────────────────────────────────────────────────────────────

function scheduleProgress(delay) {
    clearTimeout(progressTimeout);
    progressTimeout = setTimeout(pollProgress, delay);
}

async function pollProgress() {
    try {
        const res = await fetch(`${curatorUrl}/links/stats`, { mode: 'cors', credentials: 'omit' });
        const data = await res.json();
        const total = data.total || 0;
        const unprocessed = data.unprocessed || 0;
        const processed = data.processed || 0;

        const fill = document.getElementById('progress-bar-fill');
        const label = document.getElementById('progress-label');

        if (total === 0) {
            fill.style.width = '0%';
            fill.classList.remove('pulsing');
            label.textContent = 'queue empty';
        } else {
            const pct = Math.round((processed / total) * 100);
            fill.style.width = `${pct}%`;
            if (unprocessed > 0) {
                fill.classList.add('pulsing');
                label.textContent = `${unprocessed} in queue`;
            } else {
                fill.classList.remove('pulsing');
                label.textContent = 'all processed';
            }
        }

        if (unprocessed > 0) {
            progressEmptyStreak = 0;
            scheduleProgress(PROGRESS_FAST_MS);
        } else {
            progressEmptyStreak++;
            scheduleProgress(progressEmptyStreak >= PROGRESS_SLOW_AFTER ? PROGRESS_SLOW_MS : PROGRESS_FAST_MS);
        }
    } catch {
        document.getElementById('progress-label').textContent = 'server offline';
        document.getElementById('progress-bar-fill').style.width = '0%';
        scheduleProgress(PROGRESS_SLOW_MS);
    }
}

function startProgressPolling() {
    progressEmptyStreak = 0;
    pollProgress();
}

function boostProgressPolling() {
    progressEmptyStreak = 0;
    scheduleProgress(PROGRESS_FAST_MS);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function showMessage(text, type, element, duration = 0) {
    element.textContent = text;
    element.className = `message show ${type}`;
    if (duration > 0) setTimeout(() => element.classList.remove('show'), duration);
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ── Init ──────────────────────────────────────────────────────────────────────

window.addEventListener('beforeunload', () => {
    stopStatusRefresh();
    clearTimeout(progressTimeout);
});

initializePopup();
startProgressPolling();
