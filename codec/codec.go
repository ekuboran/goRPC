package codec

import "io"

// 请求和响应中的参数和返回值抽象为body，剩余的信息放在header中
type Header struct {
	ServiceMethod string // 格式如"Service.Method"
	Seq           uint64 // 请求ID
	Err           string
}

/*
编解码器
Codec 为 RPC 会话的客户端实现 RPC 请求的写入和 RPC 响应的读取。
客户端调用 [Codec.WriteRequest] 将请求写入连接，
并成对调用 [Codec.ReadResponseHeader] 和 [Codec.ReadResponseBody] 读取响应。
客户端在完成连接后调用 [Codec.Close]。可以使用 nil 参数调用 ReadResponseBody 以强制读取响应主体，然后将其丢弃。
*/
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// Codec 的构造函数
type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
