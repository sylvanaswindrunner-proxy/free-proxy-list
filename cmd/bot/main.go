package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	repoURL       = "https://github.com/sylvanaswindrunner-proxy/free-proxy-list"
	rawBaseURL    = "https://raw.githubusercontent.com/sylvanaswindrunner-proxy/free-proxy-list/main/proxies"
	probeHost     = "www.youtube.com"
	probePort     = 443
	probeURL      = "https://www.youtube.com/robots.txt"
	probeUA       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	statsStartTag = "<!-- STATS:START -->"
	statsEndTag   = "<!-- STATS:END -->"
)

var httpSources = []string{
	"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/http.txt",
	"https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt",
	"https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
	"https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/http.txt",
	"https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
	"https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt",
	"https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
	"https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt",
	"https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt",
	"https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
	"https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
	"https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt",
	"https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt",
	"https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt",
	"https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
	"https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
	"https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt",
	"https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/http_proxies.txt",
	"https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/xResults/Proxies/HTTP.txt",
	"https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/xResults/Proxies/HTTPS.txt",
	"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt",
	"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies_anonymous/http.txt",
	"https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt",
	"https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt",
	"https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/http/global/http_checked.txt",
	"https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt",
	"https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/http.txt",
	"https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/https.txt",
	"https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt",
	"https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/http.txt",
	"https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/main/free.txt",
	"https://raw.githubusercontent.com/HyperBeats/proxy-list/main/http.txt",
	"https://raw.githubusercontent.com/mertguvencli/http-proxy-list/main/proxy-list/data.txt",
	"https://raw.githubusercontent.com/almroot/proxylist/master/list.txt",
	"https://raw.githubusercontent.com/aslisk/proxyhttps/main/https.txt",
	"https://raw.githubusercontent.com/hendrikbgr/Free-Proxy-Repo/master/proxy_list.txt",
	"https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTP.txt",
	"https://raw.githubusercontent.com/RX4096/proxy-list/main/online/http.txt",
	"https://raw.githubusercontent.com/RX4096/proxy-list/main/online/https.txt",
	"https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt",
	"https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/http_proxies.txt",
	"https://raw.githubusercontent.com/tuanminpay/live-proxy/master/http.txt",
	"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
	"https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=http&proxy_format=ipport&format=text&timeout=20000",
	"https://www.proxy-list.download/api/v1/get?type=http",
	"https://www.proxy-list.download/api/v1/get?type=https",
	"https://api.openproxylist.xyz/http.txt",
	"https://proxyspace.pro/http.txt",
	"https://proxyspace.pro/https.txt",
}

var socks5Sources = []string{
	"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
	"https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/socks5.txt",
	"https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt",
	"https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
	"https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/socks5.txt",
	"https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
	"https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
	"https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt",
	"https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
	"https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
	"https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
	"https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
	"https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
	"https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/generated/socks5_proxies.txt",
	"https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/xResults/Proxies/SOCKS5.txt",
	"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt",
	"https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies_anonymous/socks5.txt",
	"https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt",
	"https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/socks5/global/socks5_checked.txt",
	"https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt",
	"https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/socks5.txt",
	"https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/socks5.txt",
	"https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/socks5.txt",
	"https://raw.githubusercontent.com/HyperBeats/proxy-list/main/socks5.txt",
	"https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/SOCKS5.txt",
	"https://raw.githubusercontent.com/dpangestuw/Free-Proxy/main/socks5_proxies.txt",
	"https://raw.githubusercontent.com/tuanminpay/live-proxy/master/socks5.txt",
	"https://raw.githubusercontent.com/manuGMG/proxy-365/main/SOCKS5.txt",
	"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=10000&country=all",
	"https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&protocol=socks5&proxy_format=ipport&format=text&timeout=20000",
	"https://www.proxy-list.download/api/v1/get?type=socks5",
	"https://api.openproxylist.xyz/socks5.txt",
	"https://proxyspace.pro/socks5.txt",
}

var ipPortRe = regexp.MustCompile(`(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})`)

type ProtocolStats struct {
	Tested  int `json:"tested"`
	Working int `json:"working"`
	Failed  int `json:"failed"`
}

type Stats struct {
	UpdatedUTC string        `json:"updated_utc"`
	HTTP       ProtocolStats `json:"http"`
	SOCKS5     ProtocolStats `json:"socks5"`
}

func envInt(key string, def, minVal, maxVal int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	if v < minVal {
		return minVal
	}
	if v > maxVal {
		return maxVal
	}
	return v
}

func isValidPublicIPv4Port(ipStr, portStr string) (string, bool) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return "", false
	}
	ip4 := ip.To4()
	if ip4 == nil || ip4.IsLoopback() || ip4.IsUnspecified() || ip4.IsPrivate() || ip4.IsLinkLocalUnicast() || ip4.IsMulticast() {
		return "", false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", false
	}
	return fmt.Sprintf("%s:%d", ip4.String(), port), true
}

func gatherProxies(sources []string, timeout time.Duration) []string {
	client := &http.Client{Timeout: timeout}
	seen := make(map[string]struct{})
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, src := range sources {
		wg.Add(1)
		go func(srcURL string) {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodGet, srcURL, nil)
			if err != nil {
				return
			}
			req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ProxyGatherBot/1.0)")
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("warn: fetch %s failed: %v", srcURL, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("warn: fetch %s returned status %d", srcURL, resp.StatusCode)
				return
			}
			body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
			if err != nil {
				return
			}
			matches := ipPortRe.FindAllStringSubmatch(string(body), -1)
			local := make([]string, 0, len(matches))
			for _, m := range matches {
				if addr, ok := isValidPublicIPv4Port(m[1], m[2]); ok {
					local = append(local, addr)
				}
			}
			mu.Lock()
			for _, addr := range local {
				seen[addr] = struct{}{}
			}
			mu.Unlock()
		}(src)
	}
	wg.Wait()

	list := make([]string, 0, len(seen))
	for addr := range seen {
		list = append(list, addr)
	}
	sort.Strings(list)
	return list
}

type checkedProxy struct {
	addr    string
	latency time.Duration
}

func verifyAll(candidates []string, concurrency int, checkFn func(string) (time.Duration, bool), onProgress func(checked, working int)) []string {
	if len(candidates) == 0 {
		if onProgress != nil {
			onProgress(0, 0)
		}
		return nil
	}
	jobs := make(chan string, concurrency*2)
	var mu sync.Mutex
	var working []checkedProxy
	var wg sync.WaitGroup
	var checked atomic.Int64
	var workingCount atomic.Int64

	workerCount := concurrency
	if len(candidates) < workerCount {
		workerCount = len(candidates)
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range jobs {
				latency, ok := checkFn(addr)
				checked.Add(1)
				if ok {
					workingCount.Add(1)
					mu.Lock()
					working = append(working, checkedProxy{addr: addr, latency: latency})
					mu.Unlock()
				}
			}
		}()
	}

	done := make(chan struct{})
	var tickerWg sync.WaitGroup
	if onProgress != nil {
		tickerWg.Add(1)
		go func() {
			defer tickerWg.Done()
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					onProgress(int(checked.Load()), int(workingCount.Load()))
				}
			}
		}()
	}

	for _, c := range candidates {
		jobs <- c
	}
	close(jobs)
	wg.Wait()

	if onProgress != nil {
		close(done)
		tickerWg.Wait()
		onProgress(len(candidates), len(working))
	}

	sort.Slice(working, func(i, j int) bool {
		if working[i].latency == working[j].latency {
			return working[i].addr < working[j].addr
		}
		return working[i].latency < working[j].latency
	})
	result := make([]string, len(working))
	for i, p := range working {
		result[i] = p.addr
	}
	return result
}

func isValidYouTubeRobots(body []byte) bool {
	s := strings.ToLower(string(body))
	return strings.Contains(s, "user-agent:") && strings.Contains(s, "disallow:")
}

func checkHTTP(proxyAddr string, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	u := &url.URL{Scheme: "http", Host: proxyAddr}
	dialer := &net.Dialer{Timeout: timeout}
	tr := &http.Transport{
		Proxy:               http.ProxyURL(u),
		DisableKeepAlives:   true,
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: timeout,
	}
	defer tr.CloseIdleConnections()

	client := &http.Client{
		Transport: tr,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return 0, false
	}
	req.Header.Set("User-Agent", probeUA)
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil || !isValidYouTubeRobots(body) {
		return 0, false
	}
	return time.Since(start), true
}

// checkSOCKS5 performs an RFC 1928 handshake (no-auth + CONNECT :443), verifies TLS, and checks YouTube robots.txt.
func checkSOCKS5(proxyAddr string, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	deadline := start.Add(timeout)

	conn, err := net.DialTimeout("tcp", proxyAddr, timeout)
	if err != nil {
		return 0, false
	}
	defer conn.Close()
	_ = conn.SetDeadline(deadline)

	// 1. Greeting: VER=5, NMETHODS=1, METHOD=0 (no auth)
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return 0, false
	}
	var greet [2]byte
	if _, err := io.ReadFull(conn, greet[:]); err != nil {
		return 0, false
	}
	if greet[0] != 0x05 || greet[1] != 0x00 {
		return 0, false
	}

	// 2. CONNECT request to probeHost:443 (ATYP=0x03 domain)
	req := make([]byte, 0, 7+len(probeHost))
	req = append(req, 0x05, 0x01, 0x00, 0x03, byte(len(probeHost)))
	req = append(req, probeHost...)
	req = append(req, byte(probePort>>8), byte(probePort&0xff))
	if _, err := conn.Write(req); err != nil {
		return 0, false
	}

	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return 0, false
	}
	if hdr[0] != 0x05 || hdr[1] != 0x00 {
		return 0, false
	}

	// 3. Skip bound address + 2-byte port
	switch hdr[3] {
	case 0x01: // IPv4
		var b [6]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return 0, false
		}
	case 0x03: // Domain
		var ln [1]byte
		if _, err := io.ReadFull(conn, ln[:]); err != nil {
			return 0, false
		}
		buf := make([]byte, int(ln[0])+2)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return 0, false
		}
	case 0x04: // IPv6
		var b [18]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}

	// 4. Upgrade tunnel to verified TLS for probeHost
	tlsConn := tls.Client(conn, &tls.Config{ServerName: probeHost})
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return 0, false
	}
	_ = tlsConn.SetDeadline(deadline)

	// 5. Send HTTPS request through the verified TLS tunnel
	httpReq := "GET /robots.txt HTTP/1.1\r\nHost: " + probeHost + "\r\nUser-Agent: " + probeUA + "\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n"
	if _, err := tlsConn.Write([]byte(httpReq)); err != nil {
		return 0, false
	}

	resp, err := http.ReadResponse(bufio.NewReader(io.LimitReader(tlsConn, 128<<10)), nil)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil || !isValidYouTubeRobots(body) {
		return 0, false
	}
	return time.Since(start), true
}

func formatLines(items []string) []byte {
	if len(items) == 0 {
		return []byte{}
	}
	return []byte(strings.Join(items, "\n") + "\n")
}

func writeOutputs(nowUTC time.Time, stats Stats, httpWorking, socks5Working []string) error {
	if err := os.MkdirAll("proxies", 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join("proxies", "http.txt"), formatLines(httpWorking), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join("proxies", "socks5.txt"), formatLines(socks5Working), 0o644); err != nil {
		return err
	}
	statsJSON, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join("proxies", "stats.json"), append(statsJSON, '\n'), 0o644); err != nil {
		return err
	}
	return updateReadmeStats("README.md", nowUTC, stats)
}

func successRate(working, tested int) string {
	if tested == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", float64(working)*100.0/float64(tested))
}

func updateReadmeStats(path string, nowUTC time.Time, s Stats) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	content := string(raw)
	startIdx := strings.Index(content, statsStartTag)
	endIdx := strings.Index(content, statsEndTag)
	if startIdx == -1 || endIdx == -1 || endIdx <= startIdx {
		return nil
	}

	block := fmt.Sprintf(`%s
> **Last Updated:** `+"`%s`"+` · **Next Scan:** Automatic every 6 hours (`+"`0 */6 * * *`"+` UTC)

| Protocol | Tested | Working | Failed | Pass Rate | Raw List |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **HTTP** | %d | **%d** | %d | %s | [http.txt](%s/http.txt) |
| **SOCKS5** | %d | **%d** | %d | %s | [socks5.txt](%s/socks5.txt) |
%s`,
		statsStartTag,
		nowUTC.Format("2006-01-02 15:04 UTC"),
		s.HTTP.Tested, s.HTTP.Working, s.HTTP.Failed, successRate(s.HTTP.Working, s.HTTP.Tested), rawBaseURL,
		s.SOCKS5.Tested, s.SOCKS5.Working, s.SOCKS5.Failed, successRate(s.SOCKS5.Working, s.SOCKS5.Tested), rawBaseURL,
		statsEndTag,
	)

	updated := content[:startIdx] + block + content[endIdx+len(statsEndTag):]
	return os.WriteFile(path, []byte(updated), 0o644)
}

func sendTelegramDocument(botToken, chatID, filename string, data []byte, caption string) error {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	if err := mw.WriteField("chat_id", chatID); err != nil {
		return err
	}
	if err := mw.WriteField("parse_mode", "HTML"); err != nil {
		return err
	}
	if err := mw.WriteField("caption", caption); err != nil {
		return err
	}
	part, err := mw.CreateFormFile("document", filename)
	if err != nil {
		return err
	}
	if _, err := part.Write(data); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", botToken)
	req, err := http.NewRequest(http.MethodPost, apiURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sendDocument %s failed (%d): %s", filename, resp.StatusCode, string(respBody))
	}
	return nil
}

func postTelegramJSON(botToken, method string, payload any) ([]byte, error) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/%s", botToken, method)
	req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewReader(jsonBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode != http.StatusOK {
		return respBody, fmt.Errorf("%s failed (%d): %s", method, resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

func sendTelegramStatus(botToken, chatID, text string) (int64, error) {
	respBody, err := postTelegramJSON(botToken, "sendMessage", map[string]any{
		"chat_id": chatID,
		"text":    text,
	})
	if err != nil {
		return 0, err
	}
	var parsed struct {
		Ok     bool `json:"ok"`
		Result struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return 0, err
	}
	if !parsed.Ok || parsed.Result.MessageID == 0 {
		return 0, fmt.Errorf("unexpected sendMessage response: %s", string(respBody))
	}
	return parsed.Result.MessageID, nil
}

func editTelegramStatus(botToken, chatID string, messageID int64, text string) error {
	_, err := postTelegramJSON(botToken, "editMessageText", map[string]any{
		"chat_id":    chatID,
		"message_id": messageID,
		"text":       text,
	})
	return err
}

func renderBar(done, total int) string {
	const width = 10
	if total <= 0 {
		return "[" + strings.Repeat("░", width) + "]"
	}
	filled := (done * width) / total
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

func formatDuration(d time.Duration) string {
	sec := int(d.Round(time.Second).Seconds())
	if sec < 0 {
		sec = 0
	}
	if sec < 60 {
		return fmt.Sprintf("%ds", sec)
	}
	return fmt.Sprintf("%dm %ds", sec/60, sec%60)
}

type liveTracker struct {
	botToken      string
	chatID        string
	msgID         int64
	startedAt     time.Time
	lastText      string
	mu            sync.Mutex
	phase         string
	httpTotal     int
	httpChecked   int
	httpWorking   int
	socks5Total   int
	socks5Checked int
	socks5Working int
}

func (t *liveTracker) start() {
	if t.botToken == "" || t.chatID == "" {
		return
	}
	t.mu.Lock()
	t.phase = "gathering"
	initial := "🔄 Proxy Scan Started\n📥 Gathering proxy lists from sources..."
	t.mu.Unlock()

	msgID, err := sendTelegramStatus(t.botToken, t.chatID, initial)
	if err != nil {
		log.Printf("warn: failed sending initial live status: %v", err)
		return
	}
	t.mu.Lock()
	t.msgID = msgID
	t.lastText = initial
	t.mu.Unlock()
}

func (t *liveTracker) setTotals(httpTotal, socks5Total int) {
	t.mu.Lock()
	t.httpTotal = httpTotal
	t.socks5Total = socks5Total
	t.mu.Unlock()
}

func (t *liveTracker) setPhase(phase string) {
	t.mu.Lock()
	t.phase = phase
	t.mu.Unlock()
}

func (t *liveTracker) setHTTPProgress(checked, working int) {
	t.mu.Lock()
	t.httpChecked = checked
	t.httpWorking = working
	t.mu.Unlock()
}

func (t *liveTracker) setSOCKS5Progress(checked, working int) {
	t.mu.Lock()
	t.socks5Checked = checked
	t.socks5Working = working
	t.mu.Unlock()
}

func pct(done, total int) int {
	if total <= 0 {
		return 0
	}
	p := (done * 100) / total
	if p > 100 {
		p = 100
	}
	return p
}

func (t *liveTracker) renderLocked() string {
	header := "🔄 Live Proxy Scan"
	switch t.phase {
	case "gathering":
		return "🔄 Proxy Scan Started\n📥 Gathering proxy lists from sources..."
	case "http":
		header = "🔄 Live Proxy Scan — Checking HTTP..."
	case "socks5":
		header = "🔄 Live Proxy Scan — Checking SOCKS5..."
	case "uploading":
		header = "📤 Uploading verified proxy files to Telegram..."
	case "done":
		header = "✅ Proxy Scan Completed"
	}

	httpFailed := t.httpChecked - t.httpWorking
	if httpFailed < 0 {
		httpFailed = 0
	}
	socks5Failed := t.socks5Checked - t.socks5Working
	if socks5Failed < 0 {
		socks5Failed = 0
	}

	return fmt.Sprintf(
		"%s\n⏱ Elapsed: %s\n\n"+
			"🌐 HTTP Proxies (Collected: %d)\n"+
			"%s %d%% (%d / %d)\n"+
			"✅ Working: %d | ❌ Failed: %d\n\n"+
			"🔒 SOCKS5 Proxies (Collected: %d)\n"+
			"%s %d%% (%d / %d)\n"+
			"✅ Working: %d | ❌ Failed: %d",
		header,
		formatDuration(time.Since(t.startedAt)),
		t.httpTotal,
		renderBar(t.httpChecked, t.httpTotal), pct(t.httpChecked, t.httpTotal), t.httpChecked, t.httpTotal,
		t.httpWorking, httpFailed,
		t.socks5Total,
		renderBar(t.socks5Checked, t.socks5Total), pct(t.socks5Checked, t.socks5Total), t.socks5Checked, t.socks5Total,
		t.socks5Working, socks5Failed,
	)
}

func (t *liveTracker) update() {
	t.mu.Lock()
	if t.botToken == "" || t.chatID == "" || t.msgID == 0 {
		t.mu.Unlock()
		return
	}
	text := t.renderLocked()
	if text == t.lastText {
		t.mu.Unlock()
		return
	}
	msgID := t.msgID
	botToken := t.botToken
	chatID := t.chatID
	t.mu.Unlock()

	if err := editTelegramStatus(botToken, chatID, msgID, text); err != nil {
		if !strings.Contains(err.Error(), "message is not modified") {
			log.Printf("warn: live status edit failed: %v", err)
		}
		return
	}

	t.mu.Lock()
	t.lastText = text
	t.mu.Unlock()
}

func sendTelegramSummary(botToken, chatID string, httpWorking, socks5Working int) error {
	payload := map[string]any{
		"chat_id":                  chatID,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
		"text": fmt.Sprintf(
			"✅ <b>Delivery complete</b>\n🌐 HTTP: <code>%d</code> | 🔒 SOCKS5: <code>%d</code>\n⏱ Next auto scan in <b>6 hours</b>",
			httpWorking, socks5Working,
		),
		"reply_markup": map[string]any{
			"inline_keyboard": [][]map[string]string{
				{
					{"text": "🛡️ OneForAllHub", "url": "https://t.me/OneForAllHub"},
					{"text": "🔥 HQPROX", "url": "https://t.me/HQPROX"},
				},
				{
					{"text": "⭐ Star on GitHub", "url": repoURL},
				},
			},
		},
	}
	_, err := postTelegramJSON(botToken, "sendMessage", payload)
	return err
}

func main() {
	concurrency := envInt("CONCURRENCY", 1500, 1, 5000)
	timeoutSec := envInt("TIMEOUT_SEC", 7, 1, 60)
	timeout := time.Duration(timeoutSec) * time.Second

	botToken := strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	chatID := strings.TrimSpace(os.Getenv("CHAT_ID"))

	tracker := &liveTracker{
		botToken:  botToken,
		chatID:    chatID,
		startedAt: time.Now(),
	}
	tracker.start()

	log.Printf("Gathering proxies (concurrency=%d, timeout=%s)...", concurrency, timeout)
	httpCandidates := gatherProxies(httpSources, 15*time.Second)
	socks5Candidates := gatherProxies(socks5Sources, 15*time.Second)
	log.Printf("Gathered unique candidates: HTTP=%d, SOCKS5=%d", len(httpCandidates), len(socks5Candidates))

	tracker.setTotals(len(httpCandidates), len(socks5Candidates))
	tracker.setPhase("http")
	tracker.update()

	httpWorking := verifyAll(httpCandidates, concurrency, func(addr string) (time.Duration, bool) {
		return checkHTTP(addr, timeout)
	}, func(checked, working int) {
		tracker.setHTTPProgress(checked, working)
		tracker.update()
	})
	log.Printf("Verified HTTP proxies: %d / %d working", len(httpWorking), len(httpCandidates))

	tracker.setPhase("socks5")
	tracker.update()

	socks5Working := verifyAll(socks5Candidates, concurrency, func(addr string) (time.Duration, bool) {
		return checkSOCKS5(addr, timeout)
	}, func(checked, working int) {
		tracker.setSOCKS5Progress(checked, working)
		tracker.update()
	})
	log.Printf("Verified SOCKS5 proxies: %d / %d working", len(socks5Working), len(socks5Candidates))

	nowUTC := time.Now().UTC()
	stats := Stats{
		UpdatedUTC: nowUTC.Format(time.RFC3339),
		HTTP: ProtocolStats{
			Tested:  len(httpCandidates),
			Working: len(httpWorking),
			Failed:  len(httpCandidates) - len(httpWorking),
		},
		SOCKS5: ProtocolStats{
			Tested:  len(socks5Candidates),
			Working: len(socks5Working),
			Failed:  len(socks5Candidates) - len(socks5Working),
		},
	}

	if err := writeOutputs(nowUTC, stats, httpWorking, socks5Working); err != nil {
		log.Fatalf("Failed writing outputs: %v", err)
	}

	if botToken == "" || chatID == "" {
		log.Println("BOT_TOKEN or CHAT_ID not set; skipping Telegram delivery")
		return
	}

	tracker.setPhase("uploading")
	tracker.update()

	fileTS := nowUTC.Format("20060102_150405")
	displayTS := nowUTC.Format("2006-01-02 15:04 UTC")

	if len(httpWorking) > 0 {
		caption := fmt.Sprintf(
			"🌐 <b>HTTP PROXY LIST</b>\n📊 Tested: <code>%d</code>\n✅ Working: <code>%d</code>\n❌ Failed: <code>%d</code>\n🕒 <code>%s</code>\n\n======================\n<b>List Of My Telegram Channel</b>\nhttps://t.me/HQPROX\nhttps://t.me/OneForAllHub\n======================\n⭐ <b>Star on GitHub:</b>\n%s",
			stats.HTTP.Tested, stats.HTTP.Working, stats.HTTP.Failed, displayTS, repoURL,
		)
		if err := sendTelegramDocument(botToken, chatID, fmt.Sprintf("working_http_%s.txt", fileTS), formatLines(httpWorking), caption); err != nil {
			log.Fatalf("Telegram HTTP document error: %v", err)
		}
	}

	if len(socks5Working) > 0 {
		caption := fmt.Sprintf(
			"🔒 <b>SOCKS5 PROXY LIST</b>\n📊 Tested: <code>%d</code>\n✅ Working: <code>%d</code>\n❌ Failed: <code>%d</code>\n🕒 <code>%s</code>\n\n======================\n<b>List Of My Telegram Channel</b>\nhttps://t.me/HQPROX\nhttps://t.me/OneForAllHub\n======================\n⭐ <b>Star on GitHub:</b>\n%s",
			stats.SOCKS5.Tested, stats.SOCKS5.Working, stats.SOCKS5.Failed, displayTS, repoURL,
		)
		if err := sendTelegramDocument(botToken, chatID, fmt.Sprintf("working_socks5_%s.txt", fileTS), formatLines(socks5Working), caption); err != nil {
			log.Fatalf("Telegram SOCKS5 document error: %v", err)
		}
	}

	if err := sendTelegramSummary(botToken, chatID, stats.HTTP.Working, stats.SOCKS5.Working); err != nil {
		log.Fatalf("Telegram summary error: %v", err)
	}

	tracker.setPhase("done")
	tracker.update()
	log.Println("Telegram delivery completed successfully")
}
