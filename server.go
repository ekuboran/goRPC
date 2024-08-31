package gorpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"gorpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

const (
	connected        = "200 Connected to goRPC"
	defaultRPCPath   = "/_goprc_"
	defaultDebugPath = "/debug/gorpc"
)

type Option struct {
	MagicNumber       int           // MagicNumber标记这是一个gorpc请求
	CodecType         codec.Type    // 客户端可能选择不同的序列化/反序列化方式
	ConnectionTimeout time.Duration // 连接超时限制，默认设置5s
	HandleTimeout     time.Duration // 服务端处理请求超时限制，默认为0，即没有限制
}

// RPC 请求
type request struct {
	h            *codec.Header // header of request
	argv, replyv reflect.Value // argv and replyv of request
	svc          *service
	mtype        *methodType
}

// 为了实现简单GoRPC客户端固定采用Gob编码Option，后续的header和body的编码方式由Option中的CodeType指定
// 服务端首先使用JSON解码Option然后通过 Option 的 CodeType 解码剩余的内容。
var DefaultOption = &Option{
	MagicNumber:       MagicNumber,
	CodecType:         codec.GobType,
	ConnectionTimeout: time.Second * 5,
}

// RPC 服务的Server
type Server struct {
	// 一种并发安全的映射类型，专门为并发环境设计。与常规的map不同，其可以在多个goroutine中安全地读写，而无需显式使用互斥锁（sync.Mutex）来保护数据。
	serviceMap sync.Map // 见Register方法中，key为service.name, value为service实例
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{}
}

// DefaultServer 是一个默认的 Server 实例，主要为了用户使用方便。
var DefaultServer = NewServer()

// Accept接受侦听器上的连接，并为每个传入的连接提供请求。
func (s *Server) Accept(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go s.ServeConn(conn)
	}
}

func Accept(l net.Listener) { DefaultServer.Accept(l) }

// ServeConn在单个连接上运行服务器。ServeConn阻塞，为连接提供服务，直到客户端挂起。
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	// defer func() {
	// 	conn.Close()
	// }()
	var opt Option
	// 解码最前面的Option的json数据
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	// 判断是不是rpc请求
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	// 获取新建编解码对象的方法
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	s.serveCodec(f(conn), &opt)
}

// invalidRequest是错误发生时响应argv的占位符
var invalidRequest = struct{}{}

// serveCodec在单个连接上运行服务器。serveCodec阻塞，为连接提供服务，直到客户端挂起。
func (s *Server) serveCodec(cc codec.Codec, opt *Option) {
	// 确保返回一个完整的响应
	sending := new(sync.Mutex)
	// 等待直到所有的请求都被处理完成
	wg := new(sync.WaitGroup)
	// 循环等待请求的到来，直到发生错误（例如连接被关闭，接收到的报文有问题）等
	for {
		// 读请求
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				break // 不可能恢复，所以关闭连接
			}
			req.h.Err = err.Error()
			// 回复请求
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// 处理请求,处理请求是并发的，但是回复请求的报文必须是逐个发送的，并发容易导致多个回复报文交织在一起，客户端无法解析。在这里使用锁(sending)保证。
		go s.handleRequest(cc, req, sending, wg, opt.ConnectionTimeout)
	}
	wg.Wait()
	cc.Close()
}

// 从编解码器中读取请求头
func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// 从编解码器中读取请求，返回Request实例
func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	// 生成Request实例
	req := &request{h: h}
	req.svc, req.mtype, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv, req.replyv = req.mtype.newArgv(), req.mtype.newReplyv()
	// 确保argvi是一个指针（因为req.argv可能是个值类型），ReadBody()需要一个指针作为参数
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		// 如果 req.argv 不是指针类型，使用 Addr() 方法获取它的地址，这将返回一个指向 req.argv 的指针类型的 reflect.Value。
		argvi = req.argv.Addr().Interface()
	}
	// 反序列化读取body到argvi中
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

// 用给定的header和body发送响应
func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// 处理请求
func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{}) // 接收调用是否完成的信息
	send := make(chan struct{})   // 接收返回响应是否完成的信息
	go func() {
		// 调用服务方法
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			// 如果出错返回出错响应
			req.h.Err = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			send <- struct{}{}
			return
		}
		//返回正确响应(.Interface()方法来获取reflect.Value这个类型持有的具体值)
		s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		send <- struct{}{}
	}()
	// 若没有处理超时限制，就直接接收调用和响应操作的完成信息后返回
	if timeout == 0 {
		<-called
		<-send
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Err = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-send
	}
}

// 在rpc服务端上注册满足以下条件的接收器值的方法集，即注册服务
func (s *Server) Register(rcvr interface{}) error {
	service := newService(rcvr)
	// LoadOrStore()用于检查给定的键（service.name）是否已经存在。
	if _, dup := s.serviceMap.LoadOrStore(service.name, service); dup {
		return errors.New("rpc: service already defined: " + service.name)
	}
	return nil
}

// 在默认的rpc服务端上注册满足以下条件的接收器值的方法集，即在默认的rpc服务器上注册服务
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// 根据"Service.Method"这样的字符串从serviceMap中找到服务
func (s *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	// LastIndex(str, substr)获取substr在str中最后一次出现的位置(不存在则返回-1)，这里即获取.的位置
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	// 获取服务名和方法名
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	// Load()返回的是接口类型
	svci, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	// 类型断言 .(*service) 的意思是：“我期望svci实际上是一个*service类型的值” 如果类型断言成功，则svci将被转换为 *service类型并赋值给svc。
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// ServeHTTP implements an http.Handler that answers RPC requests.
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		// 不是CONNECT请求就返回错误信息
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		// 用于向指定的io.Writer接口写入字符串数据，避免了在写入字符串时需要将其转换为字节切片。返回写入的字节数和错误信息
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	// 劫持（hijack）一个HTTP连接,这个操作允许你在处理HTTP请求时绕过HTTP标准协议，直接获取底层的TCP连接 (conn)，从而实现自定义的数据传输逻辑。
	// 调用Hijack()之后，http服务器将不再管理此连接，调用者可以自由使用它。可以再其至上实现websocket协议、自定义协议等
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	// 响应成功信息
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.ServeConn(conn)
}

// HandleHTTP在rpcPath上为RPC消息注册一个HTTP处理程序。
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

// 暴露出来方便用户使用
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
