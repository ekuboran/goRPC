package codec

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

// 确保GobCodec实现了Codec接口
var _ Codec = (*GobCodec)(nil)

type GobCodec struct {
	conn io.ReadWriteCloser // 由构建函数传入，通常是通过 TCP 或者 Unix 建立 socket 时得到的链接实例
	buf  *bufio.Writer      // 为了防止阻塞而创建的带缓冲的 `Writer`，一般这么做能提升性能。
	dec  *gob.Decoder       // 对应 gob 的Decoder
	enc  *gob.Encoder       // 对应 gob 的Encoder
}

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	// bufio.NewWriter()用于创建一个带缓冲的写入器，它在内存中暂存写入的数据，
	// 直到缓冲区满或显式调用Flush()方法时，才会将数据实际写入底层 io.Writer。
	// 这种机制减少了频繁的小块写入操作对性能的影响，适用于需要高效写入数据的场景。
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(buf),
	}
}

func (c *GobCodec) Close() error {
	return c.conn.Close()
}

// 读header，将其存储在h中
func (c *GobCodec) ReadHeader(h *Header) error {
	// Decode从输入流中读取下一个值，并将其存储在由空接口值表示的数据中。
	return c.dec.Decode(h)
}

// 读body，将其存储在body中
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// 将数据编码并写到连接上
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		c.buf.Flush()
		if err != nil {
			c.Close()
		}
	}()
	if err := c.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := c.enc.Encode(body); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}
