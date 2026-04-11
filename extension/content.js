// Content script for Curator extension
// This runs in the context of web pages

// Listen for messages from popup or background
browser.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === 'getPageInfo') {
        sendResponse({
            url: window.location.href,
            title: document.title,
            selectedText: window.getSelection().toString()
        });
    }
});
