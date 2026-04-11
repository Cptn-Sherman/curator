// Load settings on page load
document.addEventListener('DOMContentLoaded', loadSettings);

// Form submission
document.getElementById('settings-form').addEventListener('submit', saveSettings);

// Add subscription button
document.getElementById('add-sub-btn').addEventListener('click', () => {
    document.getElementById('subscription-form').style.display = 'block';
    document.getElementById('add-sub-btn').style.display = 'none';
});

// Cancel subscription form
document.getElementById('cancel-sub-btn').addEventListener('click', () => {
    document.getElementById('subscription-form').style.display = 'none';
    document.getElementById('add-sub-btn').style.display = 'block';
    clearSubscriptionForm();
});

// Save subscription
document.getElementById('save-sub-btn').addEventListener('click', saveSubscription);

let curatorUrl = 'http://localhost:8080';

function loadSettings() {
    browser.storage.local.get(['curatorUrl'], (result) => {
        if (result.curatorUrl) {
            curatorUrl = result.curatorUrl;
            document.getElementById('curator-url').value = result.curatorUrl;
        }
    });

    loadSubscriptions();
    loadIgnoredDomains();
}

// ── Ignored Domains ───────────────────────────────────────────────────────────

function loadIgnoredDomains() {
    browser.storage.local.get(['ignoredDomains'], (result) => {
        renderIgnoredDomains(result.ignoredDomains || []);
    });
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
            <div class="list-item-content">
                <div class="list-item-name">${escapeHtml(domain)}</div>
            </div>
            <button class="btn-remove" data-domain="${escapeHtml(domain)}" title="Remove">✕</button>
        `;
        item.querySelector('.btn-remove').addEventListener('click', () => removeIgnoredDomain(domain));
        list.appendChild(item);
    });
}

function addIgnoredDomain() {
    const input = document.getElementById('ignored-domain-input');
    const raw = input.value.trim().toLowerCase();
    if (!raw) return;
    // Strip protocol if user pasted a full URL
    let domain = raw;
    try { domain = new URL(raw.includes('://') ? raw : `https://${raw}`).hostname; } catch {}

    browser.storage.local.get(['ignoredDomains'], (result) => {
        const domains = result.ignoredDomains || [];
        if (!domains.includes(domain)) {
            domains.push(domain);
            browser.storage.local.set({ ignoredDomains: domains }, () => renderIgnoredDomains(domains));
        }
        input.value = '';
    });
}

function removeIgnoredDomain(domain) {
    browser.storage.local.get(['ignoredDomains'], (result) => {
        const domains = (result.ignoredDomains || []).filter(d => d !== domain);
        browser.storage.local.set({ ignoredDomains: domains }, () => renderIgnoredDomains(domains));
    });
}

document.getElementById('add-ignored-domain-btn').addEventListener('click', addIgnoredDomain);
document.getElementById('ignored-domain-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); addIgnoredDomain(); }
});

function saveSettings(event) {
    event.preventDefault();
    
    const url = document.getElementById('curator-url').value.trim();
    const messageEl = document.getElementById('save-message');
    
    if (!url) {
        showMessage('Please enter a service URL', 'error', messageEl);
        return;
    }
    
    browser.storage.local.set({ curatorUrl: url }, () => {
        curatorUrl = url;
        showMessage('✓ Settings saved successfully', 'success', messageEl, 2000);
    });
}

async function loadSubscriptions() {
    try {
        const response = await fetch(`${curatorUrl}/subscriptions`);
        const data = await response.json();
        
        const list = document.getElementById('subscriptions-list');
        list.innerHTML = '';
        
        if (data.subscriptions && data.subscriptions.length > 0) {
            data.subscriptions.forEach(sub => {
                const item = document.createElement('div');
                item.className = 'list-item';
                item.innerHTML = `
                    <div class="list-item-content">
                        <div class="list-item-name">${escapeHtml(sub.name)}</div>
                        <div class="list-item-url">${escapeHtml(sub.url)}</div>
                    </div>
                `;
                list.appendChild(item);
            });
        } else {
            list.innerHTML = '<div class="list-empty">No subscriptions yet. Add one to get started!</div>';
        }
    } catch (error) {
        console.error('Failed to load subscriptions:', error);
        document.getElementById('subscriptions-list').innerHTML = 
            '<div class="list-empty">Unable to load subscriptions</div>';
    }
}

async function saveSubscription() {
    const name = document.getElementById('sub-name').value.trim();
    const url = document.getElementById('sub-url').value.trim();
    const interval = parseInt(document.getElementById('sub-interval').value);
    
    if (!name || !url) {
        alert('Please fill in all fields');
        return;
    }
    
    const button = document.getElementById('save-sub-btn');
    button.disabled = true;
    button.textContent = 'Saving...';
    
    try {
        const response = await fetch(`${curatorUrl}/subscriptions`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                name,
                url,
                polling_interval_seconds: interval
            })
        });
        
        if (response.ok) {
            clearSubscriptionForm();
            document.getElementById('subscription-form').style.display = 'none';
            document.getElementById('add-sub-btn').style.display = 'block';
            await loadSubscriptions();
            alert('✓ Subscription created successfully');
        } else {
            alert('Failed to create subscription');
        }
    } catch (error) {
        alert(`Error: ${error.message}`);
    } finally {
        button.disabled = false;
        button.textContent = 'Save Subscription';
    }
}

function clearSubscriptionForm() {
    document.getElementById('sub-name').value = '';
    document.getElementById('sub-url').value = '';
    document.getElementById('sub-interval').value = '3600';
}

function showMessage(text, type, element, duration = 0) {
    element.textContent = text;
    element.className = `message show ${type}`;
    
    if (duration > 0) {
        setTimeout(() => {
            element.classList.remove('show');
        }, duration);
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
