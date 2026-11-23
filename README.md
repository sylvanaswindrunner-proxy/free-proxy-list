# 🌐 Free Proxy List — Verified HTTP & SOCKS5 Proxies

[![Update & Verify Free Proxies](https://github.com/sylvanaswindrunner-proxy/free-proxy-list/actions/workflows/proxy-bot.yml/badge.svg)](https://github.com/sylvanaswindrunner-proxy/free-proxy-list/actions/workflows/proxy-bot.yml)
![Auto-Update](https://img.shields.io/badge/Auto--Update-Every%206%20Hours-blue)
![Go Version](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go&logoColor=white)
![Protocols](https://img.shields.io/badge/Protocols-HTTP%20%7C%20SOCKS5-success)
![License](https://img.shields.io/badge/License-MIT-green)

> **High-speed, deduplicated, and live-verified HTTP & SOCKS5 proxy lists** — automatically scraped, tested against real endpoints every **6 hours** via a concurrent **Go** scanner running on **GitHub Actions**, and optionally delivered straight to **Telegram**.

---

## 📊 Live Proxy Statistics

<!-- STATS:START -->
> **Last Updated:** `2026-11-23 01:47 UTC` · **Next Scan:** Automatic every 6 hours (`0 */6 * * *` UTC)

| Protocol | Tested | Working | Failed | Pass Rate | Raw List |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **HTTP** | 7142 | **674** | 6468 | 9.4% | [http.txt](https://raw.githubusercontent.com/sylvanaswindrunner-proxy/free-proxy-list/main/proxies/http.txt) |
| **SOCKS5** | 7685 | **165** | 7520 | 2.1% | [socks5.txt](https://raw.githubusercontent.com/sylvanaswindrunner-proxy/free-proxy-list/main/proxies/socks5.txt) |
<!-- STATS:END -->

> JSON stats feed: [proxies/stats.json](https://raw.githubusercontent.com/sylvanaswindrunner-proxy/free-proxy-list/main/proxies/stats.json)

---

## ⚡ Why This Proxy List?

- **Real End-to-End Verification (Not Ping-Only):**
  - **HTTP proxies** are tested with full HTTP proxy requests against `http://cp.cloudflare.com/generate_204` with redirect hijacking disabled.
  - **SOCKS5 proxies** complete a full **RFC 1928** handshake + `CONNECT` tunnel + live HTTP `204/200` check.
- **Ultra-Fast Concurrent Go Engine:** Written in pure Go standard library (`1,000` concurrent workers, zero external dependencies).
- **Clean & Deduplicated:** Extracts valid public IPv4 `IP:PORT` pairs, strips duplicates, loopback, and private RFC 1918 addresses, and sorts deterministically.
- **100% Free & Serverless Automation:** Runs every 6 hours on public GitHub Actions runners and pushes fresh lists back to the repository and Telegram.

---

## 🚀 Quick Usage

### Download with `curl`
```bash
# Fetch verified HTTP proxies
curl -sL https://raw.githubusercontent.com/sylvanaswindrunner-proxy/free-proxy-list/main/proxies/http.txt -o http.txt

# Fetch verified SOCKS5 proxies
curl -sL https://raw.githubusercontent.com/sylvanaswindrunner-proxy/free-proxy-list/main/proxies/socks5.txt -o socks5.txt
```

### Use in Python (`requests`)
```python
import random
import requests

proxies = requests.get(
    "https://raw.githubusercontent.com/sylvanaswindrunner-proxy/free-proxy-list/main/proxies/http.txt",
    timeout=10,
).text.splitlines()

proxy = random.choice(proxies)
resp = requests.get(
    "https://httpbin.org/ip",
    proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
    timeout=6,
)
print(resp.json())
```

### Run Locally
```bash
git clone https://github.com/sylvanaswindrunner-proxy/free-proxy-list.git
cd free-proxy-list
CONCURRENCY=1000 TIMEOUT_SEC=6 go run ./cmd/bot
```

---

## 🤖 Telegram Auto-Delivery Setup

Want the verified `working_http_*.txt` and `working_socks5_*.txt` files delivered to your Telegram chat or channel every 6 hours?

1. **Fork** this repository (keep it **Public** for unlimited free GitHub Actions minutes).
2. Create a bot via [@BotFather](https://t.me/BotFather) and copy its standard **Bot API Token** (`/mybots` → select bot → `API Token`).
3. Go to **Settings → Secrets and variables → Actions → New repository secret** and add:
   - `BOT_TOKEN` — Your BotFather HTTP API token
   - `CHAT_ID` — Target user ID, group ID, or channel `@username`
4. Open the **Actions** tab and click **Run workflow** on `Update & Verify Free Proxies`.

---

## 🔍 Aggregated Public Sources

Lists are gathered from trusted open-source feeds including `TheSpeedX/PROXY-List`, `monosans/proxy-list`, `clarketm/proxy-list`, `ShiftyTR/Proxy-List`, `mmpx12/proxy-list`, `hookzof/socks5_list`, `roosterkid/openproxylist`, `proxifly/free-proxy-list`, `vakhov/fresh-proxy-list`, `Zaeem20/FREE_PROXIES_LIST`, and `ProxyScrape`.

---

## ⚠️ Disclaimer

This repository aggregates and verifies publicly available free proxy servers strictly for educational, research, and network testing purposes. Free public proxies are operated by third parties; do not transmit sensitive or unencrypted credentials through public proxies.

---

<p align="center">
  If you find these auto-updated lists useful, consider giving this repository a ⭐!
</p>
