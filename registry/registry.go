package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type GoRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_gorpc_/registry"
	defaultTimeout = time.Minute * 5 // 超时时间设置为 5 min，也就是说，任何注册的服务超过5min，即视为不可用状态。
)

func NewRegistry(timeout time.Duration) *GoRegistry {
	return &GoRegistry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultGoRegister = NewRegistry(defaultTimeout)

// 添加服务器并接收心跳以使其保持活动状态。
func (gr *GoRegistry) putServer(addr string) {
	gr.mu.Lock()
	defer gr.mu.Unlock()
	server := gr.servers[addr]
	if server == nil {
		gr.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		server.start = time.Now() // if exists, update start time to keep alive
	}
}

// 返回所有活动服务器并同时删除已死服务器。
func (gr *GoRegistry) aliveServers() []string {
	gr.mu.Lock()
	defer gr.mu.Unlock()
	var alive []string
	for addr, s := range gr.servers {
		if gr.timeout == 0 || s.start.Add(gr.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(gr.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// GoRegistry 采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中。
func (gr *GoRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-Gorpc-Servers", strings.Join(gr.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-Gorpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		gr.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP registers an HTTP handler for GoRegistry messages on registryPath
func (gr *GoRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, gr)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultGoRegister.HandleHTTP(defaultPath)
}

// 定时向注册中心发送心跳，默认周期比注册中心设置的过期时间少 1 min。
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 设置默认心跳周期
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	err := sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// rpc服务端向注册中心发送http心跳
func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Gorpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
