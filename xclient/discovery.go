package xclient

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect     SelectMode = iota // 随机选择
	RoundRobinSelect                   // 轮询算法
)

type Discovery interface {
	// 从注册中心更新服务列表
	Refresh() error
	// 手动更新服务列表
	Update(servers []string) error
	// 根据负载均衡策略，选择一个服务实例
	Get(mode SelectMode) (string, error)
	// 返回所有的服务实例
	GetAll() ([]string, error)
}

// 不需要注册中心，服务列表由手工维护的服务发现的结构体
// 由用户显式地提供服务器地址
type MultiServersDiscovery struct {
	rd      *rand.Rand   // 生成随机数
	mu      sync.RWMutex // 保护以下内容
	servers []string
	index   int // 记录Round Robin算法已经轮询到的位置，为了避免每次从 0 开始，初始化时随机设定一个值。
}

var _ Discovery = (*MultiServersDiscovery)(nil)

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	md := &MultiServersDiscovery{
		// 创建一个新的随机数生成器（rand.Rand），并使用当前时间（以纳秒为单位）作为种子值来初始化它，确保在程序每次运行时不会重复相同的随机数序列
		rd:      rand.New(rand.NewSource(time.Now().UnixNano())),
		servers: servers,
	}
	// 生成一个在 [0, math.MaxInt32-1] 范围内的随机整数
	md.index = md.rd.Intn(math.MaxInt32 - 1)
	return md
}

// Refresh对MultiServersDiscovery没有意义，忽略它
func (md *MultiServersDiscovery) Refresh() error {
	return nil
}

// 在需要时更新发现服务器
func (md *MultiServersDiscovery) Update(servers []string) error {
	md.mu.Lock()
	defer md.mu.Unlock()
	md.servers = servers
	return nil
}

// 根据负载均衡策略，选择一个服务实例
func (md *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	md.mu.Lock()
	defer md.mu.Unlock()
	n := len(md.servers)
	fmt.Println(md.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}
	switch mode {
	case RandomSelect:
		return md.servers[md.rd.Intn(n)], nil
	case RoundRobinSelect:
		i := md.index % n
		md.index = (md.index + 1) % n // 往后轮询
		return md.servers[i], nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}
}

// 返回所有的服务实例
func (md *MultiServersDiscovery) GetAll() ([]string, error) {
	md.mu.RLock()
	defer md.mu.RUnlock()
	servers := make([]string, len(md.servers))
	copy(servers, md.servers)
	return servers, nil
}
