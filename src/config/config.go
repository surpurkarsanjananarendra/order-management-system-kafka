package config

import (
	"fmt"
	"sync"

	"github.com/spf13/viper"
)

type providers struct {
	providers map[string]*viper.Viper
	mu        sync.Mutex
}

var baseConfigPaths []string
var p *providers

func Init(paths []string) {
	baseConfigPaths = paths

	p = &providers{
		providers: make(map[string]*viper.Viper),
	}
}

func Get(name string) (*viper.Viper, error) {

	p.mu.Lock()
	defer p.mu.Unlock()

	if provider, ok := p.providers[name]; ok {
		return provider, nil
	}

	provider := viper.New()

	provider.SetConfigName(name)
	provider.SetConfigType("env")

	for _, path := range baseConfigPaths {
		provider.AddConfigPath(path)
	}

	err := provider.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read config %s : %w",
			name,
			err,
		)
	}

	p.providers[name] = provider

	return provider, nil
}
