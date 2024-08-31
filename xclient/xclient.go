/*
支持负载均衡的客户端
*/

package xclient

import (
	"context"
	. "gorpc"
	"io"
	"reflect"
	"sync"
)

type XClient struct {
	d       Discovery
	mode    SelectMode
	opt     *Option
	mu      sync.Mutex         // protect following
	clients map[string]*Client // 为了尽量地复用已经创建好的 Socket 连接，使用clients保存创建成功的Client实例，key为服务端ip
}

var _ io.Closer = (*XClient)(nil)

// 三个参数：服务发现实例、负载均衡模式、协议选项
func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client),
	}
}

// 提供 Close 方法，在结束后关闭已经建立的连接。
func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

// 将复用Client的能力封装在方法dial中，如果有缓存且可用的Client，就直接返回，没有就创建添加到缓存并返回。
func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	client, ok := xc.clients[rpcAddr]
	// 如果客户端不可用，从缓存中删除
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	// 如果没有返回缓存的Client，则说明需要创建新的Client，缓存并返回
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// Call调用服务函数，等待它完成，并返回它的错误状态。xc将选择一个合适的服务器。
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	// 获取服务端地址
	rpcAddr, err := xc.d.Get(xc.mode)
	// fmt.Println(rpcAddr)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast为Discovery中注册的每个服务器调用服务函数，即将请求广播到所有的服务实例
// 如果任意一个实例发生错误，则返回其中一个错误；如果调用成功，则返回其中一个的结果。
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex         // 保护共享资源 e and replyDone
	var e error               // 用于记录第一个出现的错误。
	replyDone := reply == nil // 标记是否已经设置了 reply 的值。
	// 创建一个可以取消的上下文，允许在发现错误时取消所有 goroutine 的执行。
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			//  如果 reply 不为 nil，则创建一个 reply类型的新的值，用于存储调用的结果。
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			// 如果遇到错误且之前没有记录错误
			if err != nil && e == nil {
				e = err
				cancel() // 任意一个调用失败了就cancel()取消所有未完成的调用。
			}
			//  如果没有错误且还没有设置 reply 的值，将 clonedReply 的值设置到 reply 中。
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
