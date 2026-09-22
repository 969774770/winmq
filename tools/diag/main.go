package main

// HTTP 压测客户端：send=仅发送 / recv=接收（接收即删除） / both=收发全链路
import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"winmq/internal/benchcore"
)

// fetchFirstKey 通过管理接口取第一个可用 API Key
func fetchFirstKey(base, secret string) string {
	hm := hmac.New(sha256.New, []byte(secret))
	hm.Write([]byte("winmq-admin-session"))
	req, _ := http.NewRequest("GET", base+"/admin/api/keys", nil)
	req.Header.Set("X-Admin-Token", hex.EncodeToString(hm.Sum(nil)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var out struct {
		Keys []struct {
			Key string `json:"key"`
		} `json:"keys"`
	}
	d, _ := io.ReadAll(resp.Body)
	if json.Unmarshal(d, &out) != nil || len(out.Keys) == 0 {
		return ""
	}
	return out.Keys[0].Key
}

func main() {
	data, err := os.ReadFile(`D:\AI\MNS\config.json`)
	if err != nil {
		fmt.Println("读取配置失败:", err)
		return
	}
	var c struct {
		AK string `json:"accessKeyId"`
		SK string `json:"accessKeySecret"`
	}
	json.Unmarshal(data, &c)

	mode := "both"
	count := 20000
	queue := "bench"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			count = n
		}
	}
	if len(os.Args) > 3 {
		queue = os.Args[3]
	}

	base := "http://127.0.0.1:6166"
	apiKey := fetchFirstKey(base, c.SK)
	if apiKey == "" {
		fmt.Println("未取到 API Key，请确认服务在运行")
		return
	}

	opts := benchcore.Options{
		BaseURL:     base,
		APIKey:      apiKey,
		Queue:       queue,
		Count:       count,
		Concurrency: 32,
		BodySize:    64,
		Mode:        mode,
	}
	sum := benchcore.Run(opts, nil)
	if mode != "recv" {
		fmt.Printf("发送: %d 成功 / %d 失败 / %.0f 条每秒\n", sum.SendCount, sum.SendFail, sum.SendRate)
	}
	if mode != "send" {
		fmt.Printf("接收(即删除): %d 成功 / %d 失败 / %.0f 条每秒\n", sum.RecvCount, sum.RecvFail, sum.RecvRate)
	}
}
