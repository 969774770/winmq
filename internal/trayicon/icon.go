package trayicon

// 运行时生成 32x32 32bpp ICO 图标字节（青色圆角底 + 白色队列线）

func Bytes() []byte {
	const w, h = 32, 32
	px := make([]byte, 0, w*h*4)

	bg := [3]byte{0, 150, 199}   // 青蓝
	fg := [3]byte{255, 255, 255} // 白

	isBg := func(x, y int) bool {
		const r = 4
		if x < r && y < r && (r-x)*(r-y) > r*r/2 {
			return false
		}
		if x >= w-r && y < r && (x-(w-r-1))*(r-y) > r*r/2 {
			return false
		}
		if x < r && y >= h-r && (r-x)*(y-(h-r-1)) > r*r/2 {
			return false
		}
		if x >= w-r && y >= h-r && (x-(w-r-1))*(y-(h-r-1)) > r*r/2 {
			return false
		}
		return true
	}
	line := func(y int) bool {
		return (y >= 9 && y <= 10) || (y >= 15 && y <= 16) || (y >= 21 && y <= 22)
	}

	// BMP 自底向上
	for y := h - 1; y >= 0; y-- {
		for x := 0; x < w; x++ {
			c := bg
			if !isBg(x, y) {
				c = [3]byte{0, 0, 0} // 圆角外透明（alpha=0）
			} else if line(y) && x >= 6 && x <= 25 {
				c = fg
			}
			px = append(px, c[2], c[1], c[0], 255)
			if !isBg(x, y) {
				px[len(px)-1] = 0
			}
		}
	}

	// AND mask（每行 4 字节，全 0 = 用 alpha）
	mask := make([]byte, w/8*h)

	img := make([]byte, 0, 40+len(px)+len(mask))
	img = appendU32LE(img, 40)  // biSize
	img = appendU32LE(img, w)   // biWidth
	img = appendU32LE(img, h*2) // biHeight（含 mask）
	img = appendU16LE(img, 1)   // biPlanes
	img = appendU16LE(img, 32)  // biBitCount
	img = appendU32LE(img, 0)   // biCompression
	img = appendU32LE(img, uint32(len(px)+len(mask)))
	img = appendU32LE(img, 0)
	img = appendU32LE(img, 0)
	img = appendU32LE(img, 0)
	img = appendU32LE(img, 0)
	img = append(img, px...)
	img = append(img, mask...)

	// ICONDIR + ICONDIRENTRY
	out := make([]byte, 0, 22+len(img))
	out = appendU16LE(out, 0)
	out = appendU16LE(out, 1)
	out = appendU16LE(out, 1)
	out = append(out, byte(w), byte(h), 0, 0)
	out = appendU16LE(out, 1)
	out = appendU16LE(out, 32)
	out = appendU32LE(out, uint32(len(img)))
	out = appendU32LE(out, 22)
	out = append(out, img...)
	return out
}

func appendU16LE(b []byte, v uint16) []byte {
	return append(b, byte(v), byte(v>>8))
}

func appendU32LE(b []byte, v uint32) []byte {
	return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}
