package engine

// message.go —— 消息模型与序号编码（重构版）
//
// 简化后不再有队列属性、延迟可见、可见性超时、TTL：
//   - 消息体就是 KV 的 value（m:{queue}:{seq:016X} → body）
//   - 消息 ID = seq 的 16 位十六进制字符串
//   - 语义为"接收即删除"：没有收据句柄，也就不需要 ack 接口

func itoh16(v uint64) string {
	const hexDigits = "0123456789ABCDEF"
	b := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		b[i] = hexDigits[v&0x0F]
		v >>= 4
	}
	return string(b)
}

// SeqOf 解析消息 ID 对应的序号（压测/诊断用）
func SeqOf(id string) (uint64, bool) {
	var v uint64
	if len(id) != 16 {
		return 0, false
	}
	for i := 0; i < 16; i++ {
		c := id[i]
		var d uint64
		switch {
		case c >= '0' && c <= '9':
			d = uint64(c - '0')
		case c >= 'A' && c <= 'F':
			d = uint64(c-'A') + 10
		case c >= 'a' && c <= 'f':
			d = uint64(c-'a') + 10
		default:
			return 0, false
		}
		v = v<<4 | d
	}
	return v, true
}

// Message 消息（对外视图）
type Message struct {
	ID   string `json:"id"`
	Seq  uint64 `json:"-"`
	Body string `json:"body"`
}
