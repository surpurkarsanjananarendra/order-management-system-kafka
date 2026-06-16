package configs

import (
	"fmt"
	"sync"

	"github.com/spf13/viper"
)

type providers struct {
	providers map[string]*viper.Viper //this is the cache where the configuration sread only once anre cahced into so that later they might be used again and again
	mu        sync.Mutex              // makes sure that no two go routine modify the mapp at the same time
}

var baseConfigPaths []string
var p *providers

func Init(paths []string) { //lazy loading   (nothing is loaded yet during the first call)
	baseConfigPaths = paths // = "....."

	p = &providers{
		providers: make(map[string]*viper.Viper), // = {}
	}
}

func Get(name string) (*viper.Viper, error) {

	p.mu.Lock()
	defer p.mu.Unlock() // locks provider

	if provider, ok := p.providers[name]; ok { // if instance is found then provider = pointer to Viper(.env), no need to read the file further, can return
		return provider, nil
	}

	provider := viper.New() // if not then the instance is cereated

	provider.SetConfigName(name)
	provider.SetConfigType("yaml") // this tells viper, configurations uses yaml
	// 
	//  format

	for _, path := range baseConfigPaths { // adds search paths
		provider.AddConfigPath(path)
	}

	err := provider.ReadInConfig() //starts file reading
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read config %s : %w",
			name,
			err,
		)
	}

	p.providers[name] = provider // read information is stored in cache, hence when the call is made again it first checksif there is an instance, if exists, it returns, without reading file again (once)

	return provider, nil
}
