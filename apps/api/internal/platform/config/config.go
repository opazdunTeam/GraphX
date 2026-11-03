package config

import (
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   server
	Postgres postgres
	Process  process
}

type server struct {
	Host                string `yaml:"host"`
	Log_Level           string `yaml:"log_level"`
	Read_header_timeout int    `yaml:"read_header_timeout"`
	Read_timeout        int    `yaml:"read_timeout"`
	Write_timeout       int    `yaml:"write_timeout"`
	Idle_timeout        int    `yaml:"idle_timeout"`
	Retries             int    `yaml:"retries"`
}

type postgres struct {
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Name     string `yaml:"name"`
	Password string `yaml:"password"`
	Port     string `yaml:"port"`
	Sslmode  string `yaml:"sslmode"`
}

type process struct {
	Env      string     `yaml:"env"`
	LogLevel slog.Level `yaml:"log_level"`
}

func InitConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)

	if err != nil {
		return Config{}, err
	}

	var cfg Config

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}

	cfg.Postgres.Password = os.Getenv("DB_PASSWORD")

	return cfg, nil
}
