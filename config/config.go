package config

import (
	"os"
	"io/ioutil"
	"encoding/json"
	"log"
)

type ServerMeta struct {
	ServerId  int     `json:"server_id"`
	Endpoint  string  `json:"endpoint"`
}

type Config struct {
	Local  ServerMeta    `json:"local"`
	Peers  []ServerMeta  `json:"peers"`
	DbPath string        `json:"dbpath"`
}

func ParseEndpointsConfig(filename string) *Config {
	fd, err := os.Open(filename)
	CheckError("open config file error:", err)
	
	buff, err := ioutil.ReadAll(fd)
	CheckError("read config file error:", err)
	
	cfg := &Config{}
	err = json.Unmarshal(buff, cfg)
	CheckError("config Unmarshal error:", err)
	
	return cfg
}

func CheckError(prompt string, err error) {
	if err != nil {
		log.Fatal(prompt, err)
	}
}