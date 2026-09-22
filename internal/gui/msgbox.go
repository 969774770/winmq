package gui

// msgbox.go —— 原生消息框。
// 用于"没有控制台"的场景反馈结果：例如从图形界面提权调用服务安装命令时，
// 该子进程没有父控制台，命令行输出无处可看。

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	mbOK        = 0x00000000
	mbIconError = 0x00000010
	mbIconInfo  = 0x00000040
)

var procMessageBox = windows.NewLazySystemDLL("user32.dll").NewProc("MessageBoxW")

// MessageBox 弹出系统消息框；isErr=true 显示错误图标
func MessageBox(title, text string, isErr bool) {
	t, err1 := windows.UTF16PtrFromString(text)
	c, err2 := windows.UTF16PtrFromString(title)
	if err1 != nil || err2 != nil {
		return
	}
	style := uintptr(mbOK | mbIconInfo)
	if isErr {
		style = uintptr(mbOK | mbIconError)
	}
	procMessageBox.Call(0, uintptr(unsafe.Pointer(t)), uintptr(unsafe.Pointer(c)), style)
}
