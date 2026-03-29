# MLB The Show 26 — Diamond Dynasty Tracker

A comprehensive web-based tracking tool for managing your MLB The Show 26 Diamond Dynasty missions, programs, and card collection.

> **Version:** PROD 1.0 · **Last Updated:** March 28, 2026 · **Status:** Final Production Release ✅

> **Release Policy:** This is the final baseline production release (PROD 1.0). All future feature additions and improvements will ship as incremental production releases (PROD 1.x).

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.10+** installed and on your PATH
  - Mac: [https://www.python.org/downloads/](https://www.python.org/downloads/) or `brew install python`
  - Windows: [https://www.python.org/downloads/](https://www.python.org/downloads/) — check "Add Python to PATH" during install

---

### Installation — Mac

```bash
# 1. Navigate to the project folder
cd path/to/MLB_TheShow26_Tracker

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the app
cd DD_app
python app.py
```

### Installation — Windows

```powershell
# 1. Navigate to the project folder
cd "path\to\MLB_TheShow26_Tracker"

# 2. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the app
cd DD_app
python app.py
```

### 5. Open in your browser

Navigate to [http://127.0.0.1:5000](http://127.0.0.1:5000)

> **Tip:** To stop the app, press `Ctrl + C` in the terminal. To restart later, activate the virtual environment again (step 2) and run `python app.py` from the `DD_app` folder.

---

## 📋 Features

### 🏠 Features Home (Landing Page)
- Central dashboard showing all available features
- **Today at a Glance** strip: live At-Bat mission count, tracked card count, and owned card count
- Feature cards with descriptions and quick-start info

### ⚾ Programs Tracker
- Track missions across all Diamond Dynasty programs
- **Inline progress editing** — click Remaining Quantity to update
- **Filters** by Priority, Program Category, and Status
- **Bulk actions**: mark complete or delete multiple missions at once
- **Auto-status updates**: Not Started → In-Progress → Completed automatically
- **Sortable columns** and infinite scroll
- **Import/export** via CSV for manual editing

### 🃏 Card Tracker
- **API-powered search**: find any card from The Show 26 API by player name
- **Full card data**: attributes, quirks, pitches, and live market prices fetched automatically
- Track purchase price, quantity, on-team status, and grind targets
- **Filters** by rarity, position, team, series, and more
- **Card detail page**: full attribute breakdown with visual bars
- **Profit tracking**: calculated profit after 10% tax

### 🎴 Actual Card Tracker
- Track physical or collectible card inventory separately from the digital Card Tracker
- Focused view for cards you actually own vs. targeting

### 📊 Live Scores
- Real-time MLB Scores dashboard with date-based slate selection and manual refresh
- Adaptive polling cadence: 10s when live games exist, 60m when idle
- Rolling 9-inning box score window for extra-inning games
- Dedicated alert feeds: General, Watchlist (silent), and ABS Challenges
- ABS challenge alerts include original call and review result when available
- Live game card quick actions for at-bats, lineups, and MLB.TV
- Player links in lineup and leaders route to Baseball Savant
- Spoiler controls and watchlist preferences persist across sessions

---

## 📂 File Structure

```
MLB_TheShow26_Tracker/
├── DD_app/
│   ├── app.py                      # Flask application entry point
│   ├── config.py                   # Configuration settings
│   ├── modules/
│   │   ├── api_client.py           # The Show API client
│   │   ├── backup.py               # Automatic backup system
│   │   ├── cards.py                # Card management functions
│   │   ├── database.py             # SQLite database initialization
│   │   ├── logger.py               # Logging configuration
│   │   ├── missions.py             # Mission handlers
│   │   └── scores.py               # Live scores integration
│   ├── templates/                  # HTML templates
│   ├── static/                     # CSS, JS, images
│   ├── data/                       # SQLite databases (missions.db, cards.db)
│   └── logs/                       # Application logs (auto-generated)
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

---

## 🔧 Configuration

Edit `DD_app/config.py` to customize:
- API settings and rate limiting
- Backup retention count
- Pagination size
- Database paths

---

## 🐛 Troubleshooting

**App won't start?**
- Make sure the virtual environment is activated (you should see `(.venv)` in your terminal prompt)
- Confirm all dependencies installed: `pip install -r requirements.txt`
- Check that port 5000 is not already in use

**Port 5000 busy on Mac?**
- macOS AirPlay Receiver uses port 5000. Disable it in **System Settings → General → AirDrop & Handoff** and try again, or change the port in `config.py`.

**Dependency errors?**
- Ensure you are using Python 3.10 or higher: `python3 --version` (Mac) / `python --version` (Windows)

**Logs:**
- Application logs are written to `DD_app/logs/app.log` — check here for detailed error info.
