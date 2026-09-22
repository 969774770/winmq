package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
)

// Config 服务配置（config.json）
type Config struct {
	HTTPAddr            string `json:"httpAddr"`            // HTTP 监听地址
	DataDir             string `json:"dataDir"`             // 数据目录
	AccessKeyID         string `json:"accessKeyId"`         // API 访问 ID
	AccessKeySecret     string `json:"accessKeySecret"`     // API 访问密钥（也是管理页登录口令）
	SnapshotIntervalSec int    `json:"snapshotIntervalSec"` // 快照间隔（秒）
}

// Load 读取配置；文件不存在则生成默认配置（随机 secret）
func Load(path string) (*Config, error) {
	c := &Config{}
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, c)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}

	// 生成默认配置
	buf := make([]byte, 16)
	rand.Read(buf)
	c = &Config{
		HTTPAddr:            ":6166",
		DataDir:             "data",
		AccessKeyID:         "winmq",
		AccessKeySecret:     hex.EncodeToString(buf),
		SnapshotIntervalSec: 30,
	}
	// 数据目录默认相对可执行文件所在目录
	exe, _ := os.Executable()
	c.DataDir = filepath.Join(filepath.Dir(exe), "data")

	Save(path, c)
	return c, nil
}

// Save 原子写入配置文件
func Save(path string, c *Config) error {
	data, _ := json.MarshalIndent(c, "", "  ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// AbsDataDir 返回绝对数据目录
func (c *Config) AbsDataDir() string {
	if filepath.IsAbs(c.DataDir) {
		return c.DataDir
	}
	exe, _ := os.Executable()
	return filepath.Join(filepath.Dir(exe), c.DataDir)
}
