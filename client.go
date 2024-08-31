package gorpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gorpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call表示一个存活的RPC.
type Call struct {
	Seq           uint64
	ServiceMethod string // 格式为 "<service>.<method>"
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call // Receives *Call when Go is complete
}

func (call *Call) done() {
	call.Done <- call
}

// 一个客户端可能有多个未完成的调用，并且一个客户端可能同时被多个 goroutine 使用。
type Client struct {
	codec   codec.Codec
	opt     *Option
	sending sync.Mutex   //  是一个互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆
	header  codec.Header // 请求的消息头，每个客户端只需要一个（因为请求时互斥的），声明在 Client 结构体中可以复用。
	mu      sync.Mutex   // 共享资源的互斥锁
	seq     uint64
	pending map[uint64]*Call // 存储未处理完的请求，键是编号，值是 Call 实例。
	// 下面任何一个变为true，client都会变成不可用的状态
	closing  bool // 用户是否已经调用Close（用户主动关闭）
	shutdown bool // 服务端是否已经告知我们停止服务（一般有错误发生，服务端停止）
}

type clientResult struct {
	client *Client
	err    error
}

// 函数类型
type newClientFunc func(conn io.ReadWriteCloser, opt *Option) (*Client, error)

var ErrShutdown = errors.New("connection is shut down")

// 关闭连接
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrShutdown
	}
	c.closing = true
	return c.codec.Close()
}

// 判断客户端是否还在工作
func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.shutdown && !c.closing
}

// 接收rpc响应，三种情况
func (c *Client) receive() {
	var err error
	var header codec.Header
	for err == nil {
		if err := c.codec.ReadHeader(&header); err != nil {
			break
		}
		call := c.removeCall(header.Seq)
		switch {
		// 三种情况：
		case call == nil:
			// call 不存在，即没有待处理的调用。这通常意味着 Write部分失败，并且调用已被删除；
			// 服务端仍然处理了这个错误的请求并返回了一个响应
			err = c.codec.ReadBody(nil)
		case header.Err != "":
			// call 存在，但服务端处理出错，即 h.Error 不为空。
			call.Error = fmt.Errorf(header.Err)
			err = c.codec.ReadBody(nil)
			call.done()
		default:
			// 正确情况：call存在，服务端处理正常，于是从body中读取Reply的值。
			err = c.codec.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// 发生了错误，于是终止代办的call
	c.terminateCalls(err)
}

// 发送rpc请求
func (c *Client) send(call *Call) {
	c.sending.Lock()
	defer c.sending.Unlock()

	// 注册call
	seq, err := c.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	// 构造header
	c.header.Seq = seq
	c.header.ServiceMethod = call.ServiceMethod
	c.header.Err = ""
	// 编码并发送header
	if err := c.codec.Write(&c.header, call.Args); err != nil {
		// 如果发送出错了，就从待办请求中移除
		call := c.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// 将参数 call 添加到 client.pending 中，并更新 client.seq。
func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

// 根据 seq，从 client.pending 中移除对应的 call，并返回。
func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用，将shutdown设置为true，且将错误信息通知所有pending状态的call。
func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

// 创建Client实例，需要完成一开始的协议交换，即发送Option信息给服务端,协商好消息的编解码方式之后，再创建一个子协程调用receive()接收响应。
func NewClient(conn io.ReadWriteCloser, opt *Option) (*Client, error) {
	// 获取编解码器的构造方法
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	// 发送option给服务端
	if err := json.NewEncoder(conn).Encode(&opt); err != nil {
		log.Println("rpc client: options error: ", err)
		conn.Close()
		return nil, err
	}
	// 返回Client实例（传入编解码器和option）
	return NewClientWithCodec(f(conn), opt), nil
}

func NewClientWithCodec(codec codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,
		codec:   codec,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	// 创建一个子协程调用receive()接收响应
	go client.receive()
	return client
}

// 解析options，是一个容错的机制，处理客户端传入的不合规的option
func parseOptions(opts ...*Option) (*Option, error) {
	// 如果客户端传入的option为空，就返回默认的Option（gob编解码）
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 创建网络连接
	conn, err := net.DialTimeout(network, address, opt.ConnectionTimeout)
	if err != nil {
		return nil, err
	}
	// 如果没创建成功Client实例，就将连接关闭
	defer func() {
		if client == nil {
			conn.Close()
		}
	}()
	// 用于阻塞等待客户端创建好
	ch := make(chan clientResult)
	go func() {
		// 子协程创建客户端
		client, err := f(conn, opt)
		ch <- clientResult{client, err}
	}()
	// 如果没有设置连接超时限制，就直接返回客户端实例
	if opt.ConnectionTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectionTimeout):
		// 超时就返回错误
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectionTimeout)
	case result := <-ch:
		// 非超时情况就返回客户端实例和信息
		return result.client, result.err
	}
}

// Dial connects to an RPC server at the specified network address
func Dial(network string, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

/*
Call() 和 Go() 函数是客户端暴露给用户的两个 RPC 服务调用接口，分别用于同步调用和异步调用。
Call() 函数用于同步地进行 RPC 调用。它会阻塞当前的调用者，直到远程函数执行完成并返回结果。
Go() 函数用于异步地进行 RPC 调用。它会立即返回一个 Call 对象，调用者可以通过 Call.Done 通道来监控调用的完成情况。
*/
// 异步调用该函数。它返回表示调用的[Call]结构。done通道将在调用完成时通过返回相同的call对象发出信号。
// 如果done为nil, Go将分配一个新通道。如果非nil, done必须被缓冲，否则Go会故意崩溃。
func (c *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	return call
}

// 调用命名函数，等待它完成，并返回它的错误状态。Call 是对 Go 的封装，阻塞 call.Done，等待响应返回
func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := c.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		// context承载超时信息，超时了就会执行Done()。于是从待办calls中移除该call，返回错误信息
		c.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case doneCall := <-call.Done:
		// 非超时情况接收到call.Done，返回信息
		return doneCall.Error
	}
}

// 通过HTTP作为传输协议创建一个客户端实例
func NewHTTPClient(conn io.ReadWriteCloser, opt *Option) (*Client, error) {
	// 发送CONNECT请求
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))
	// 在交换RPC协议之前需要获取成功的HTTP响应（第二个参数用于指定与此响应对应的请求，如果为nil，则默认为GET请求）
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		// HTTP CONNECT请求建立连接之后，后续的通信过程就交给 NewClient了。
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP连接到指定网络地址的HTTP RPC服务器，监听默认的HTTP RPC路径
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// 为了简化使用，提供同一入口XDial
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opts...)
	}
}
