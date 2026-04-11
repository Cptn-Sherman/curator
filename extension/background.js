// Service Worker for Curator extension

// Handle context menu for saving links
browser.contextMenus.create({
    id: "save-link",
    title: "Save to Curator",
    contexts: ["link"],
    onclick: (info) => {
        saveLinkToCurator(info.linkUrl);
    }
});

// Handle context menu for page
browser.contextMenus.create({
    id: "save-page",
    title: "Save This Page to Curator",
    contexts: ["page"],
    onclick: (info, tab) => {
        saveLinkToCurator(tab.url, tab.title);
    }
});

async function saveLinkToCurator(url, title = null) {
    const result = await browser.storage.local.get(['curatorUrl']);
    const curatorUrl = result.curatorUrl || 'http://localhost:8080';

    try {
        const response = await fetch(`${curatorUrl}/ingest`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                links: [url]
            }),
            mode: 'cors',
            credentials: 'omit'
        });

        const data = await response.json();

        if (data.ingested_count > 0) {
            browser.notifications.create({
                type: 'basic',
                iconUrl: 'icons/icon-48.png',
                title: 'Curator',
                message: 'Link saved successfully!'
            });
        } else if (data.duplicate_count > 0) {
            browser.notifications.create({
                type: 'basic',
                iconUrl: 'icons/icon-48.png',
                title: 'Curator',
                message: 'Link already exists in Curator'
            });
        }
    } catch (error) {
        browser.notifications.create({
            type: 'basic',
            iconUrl: 'icons/icon-48.png',
            title: 'Curator Error',
            message: `Failed to save link: ${error.message}`
        });
    }
}
