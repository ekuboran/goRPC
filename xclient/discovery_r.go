package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type GoRegistryDiscovery struct {
	*MultiServersDiscovery
	registryAddr string        // 注册中心地址
	timeout      time.Duration // 服务列表的过期时间
	lastUpdate   time.Time     // 最后从注册中心更新服务列表的时间
}

const defaultUpdateTimeout = time.Second * 10

func NewGoRegistryDiscovery(registerAddr string, timeout time.Duration) *GoRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &GoRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registryAddr:          registerAddr,
		timeout:               timeout,
	}
	return d
}

// 从注册中心更新服务列表
func (grd *GoRegistryDiscovery) Refresh() error {
	grd.mu.Lock()
	defer grd.mu.Unlock()
	if grd.lastUpdate.Add(grd.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", grd.registryAddr)
	resp, err := http.Get(grd.registryAddr)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-Gorpc-Servers"), ",")
	grd.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			grd.servers = append(grd.servers, strings.TrimSpace(server))
		}
	}
	grd.lastUpdate = time.Now()
	return nil
}

// 手动更新服务列表
func (grd *GoRegistryDiscovery) Update(servers []string) error {
	grd.mu.Lock()
	defer grd.mu.Unlock()
	grd.servers = servers
	grd.lastUpdate = time.Now()
	return nil
}

// 根据负载均衡策略，选择一个服务实例

func (grd *GoRegistryDiscovery) Get(mode SelectMode) (string, error) {
	if err := grd.Refresh(); err != nil {
		return "", err
	}
	return grd.MultiServersDiscovery.Get(mode)
}

// 返回所有的服务实例
func (grd *GoRegistryDiscovery) GetAll() ([]string, error) {
	if err := grd.Refresh(); err != nil {
		return nil, err
	}
	return grd.MultiServersDiscovery.GetAll()
}
