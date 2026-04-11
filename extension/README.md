# Curator Firefox Extension

A Firefox browser extension for the Curator link management service.

## Features

- **Save Links**: Quickly save the current page or any link to Curator
- **Manage Subscriptions**: View and manage content subscriptions
- **Check Status**: View real-time stats about your links, processing status, and subscriptions
- **Processing Control**: Enable/disable link processing directly from the extension
- **Context Menu**: Right-click any link or page to save to Curator

## Installation

1. Open `about:debugging#/runtime/this-firefox` in Firefox
2. Click "Load Temporary Add-on"
3. Select the `manifest.json` file from this directory
4. The extension will appear in your toolbar

For permanent installation, you would need to package it as a `.xpi` file and sign it with Mozilla.

## Configuration

1. Click the Curator icon in your toolbar
2. Click the ⚙️ settings icon
3. Set your Curator service URL (default: `http://localhost:8080`)
4. Create subscriptions for tracking specific content sources

## Usage

### Save Current Page
1. Click the Curator icon
2. The current page URL will be pre-filled
3. Optionally select a subscription
4. Click "Save Link"

### Save Any Link
Right-click a link and select "Save to Curator" from the context menu.

### View Status
Click the "Status" tab to see:
- Database name
- Total links
- Unprocessed links count
- Processing status
- Total subscriptions

### Manage Processing
Click the "Manage" tab to:
- Toggle link processing on/off
- View all subscriptions
- Add new subscriptions

## Building

The extension uses vanilla JavaScript with no build step required. All files are ready to use immediately.

## Files

- `manifest.json` - Extension manifest with permissions and configuration
- `popup.html/css/js` - Main popup interface with tabs for saving, status, and management
- `options.html/css/js` - Settings page for configuring URL and subscriptions
- `background.js` - Service worker for context menus and notifications
- `content.js` - Content script for page interaction

## Permissions

- `activeTab` - To access the current tab's URL
- `scripting` - To inject scripts if needed
- `storage` - To save your configuration
- `host_permissions` - To communicate with the Curator service
