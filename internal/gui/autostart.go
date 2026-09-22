package gui

// autostart.go —— "登录后自启"（注册表 HKCU Run 键）。
//
// 注意：HKCU Run 只在**该用户登录后**才执行，服务器重启停在登录界面时不会启动。
// 需要"无人登录也随机器启动"，请安装成 Windows 服务：winmq.exe -install

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows/registry"
)

const autoStartName = "winMQ"

// legacyAutoStartName 改名前的自启项名；启用时顺手清掉，避免留下指向已删除程序的残留项
const legacyAutoStartName = "MNSQueue"

func autoStartRegPath() registry.Key {
	key, err := registry.OpenKey(registry.CURRENT_USER,
		`Software\Microsoft\Windows\CurrentVersion\Run`, registry.QUERY_VALUE|registry.SET_VALUE)
	if err != nil {
		return 0
	}
	return key
}

// IsAutoStart 查询是否已设置"登录后自启"
func IsAutoStart() bool {
	key := autoStartRegPath()
	if key == 0 {
		return false
	}
	defer key.Close()
	v, _, err := key.GetStringValue(autoStartName)
	return err == nil && v != ""
}

// SetAutoStart 设置/取消"登录后自启"
func SetAutoStart(on bool) error {
	key := autoStartRegPath()
	if key == 0 {
		return fmt.Errorf("打开注册表失败")
	}
	defer key.Close()
	if on {
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		key.DeleteValue(legacyAutoStartName) // 清理改名前的残留项
		return key.SetStringValue(autoStartName, `"`+filepath.Clean(exe)+`"`)
	}
	if err := key.DeleteValue(autoStartName); err != nil && err != registry.ErrNotExist {
		return err
	}
	if err := key.DeleteValue(legacyAutoStartName); err != nil && err != registry.ErrNotExist {
		return err
	}
	return nil
}
