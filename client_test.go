package gorpc

import (
	"context"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// 用例1：用于测试连接超时。NewClient函数耗时 2s，ConnectionTimeout分别设置为 1s 和 0s 两种场景。
func TestClient_dialTimeout(t *testing.T) {
	// Parallel表示此测试将与(且仅与)其他并行测试并行运行。当一个测试由于使用-test.count或-test.cpu而多次运行时，单个测试的多个实例永远不会彼此并行运行。
	t.Parallel()
	l, _ := net.Listen("tcp", ":0")

	f := func(conn io.ReadWriteCloser, opt *Option) (client *Client, err error) {
		_ = conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}
	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectionTimeout: time.Second})
		_assert(err != nil && strings.Contains(err.Error(), "connect timeout"), "expect a timeout error")
	})
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &Option{ConnectionTimeout: 0})
		_assert(err == nil, "0 means no limit")
	})
}

type Bar int

// 服务方法，睡两秒
func (b Bar) Timeout(argv int, reply *int) error {
	time.Sleep(time.Second * 2)
	return nil
}

func startServer(addr chan string) {
	var b Bar
	_ = Register(&b)
	// pick a free port
	l, _ := net.Listen("tcp", ":0")
	addr <- l.Addr().String()
	Accept(l)
}

// 用例2：用于测试处理超时。Bar.Timeout 耗时 2s，场景一：客户端设置超时时间为 1s，服务端无限制；场景二，服务端设置超时时间为1s，客户端无限制。
func TestClient_Call(t *testing.T) {
	t.Parallel()
	addrCh := make(chan string)
	go startServer(addrCh)
	addr := <-addrCh
	// time.Sleep(time.Second)
	t.Run("client timeout", func(t *testing.T) {
		client, _ := Dial("tcp", addr)
		// 服务端超时时间设为1s
		ctx, _ := context.WithTimeout(context.Background(), time.Second)
		var reply int
		err := client.Call(ctx, "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), ctx.Err().Error()), "expect a timeout error")
	})
	t.Run("server handle timeout", func(t *testing.T) {
		client, _ := Dial("tcp", addr, &Option{
			// 客户端建立连接超时时间设置为1s
			HandleTimeout: time.Second,
		})
		var reply int
		err := client.Call(context.Background(), "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), "handle timeout"), "expect a timeout error")
	})
}

func TestXDial(t *testing.T) {
	if runtime.GOOS == "linux" {
		ch := make(chan struct{})
		addr := "/tmp/gorpc.sock"
		go func() {
			_ = os.Remove(addr)
			l, err := net.Listen("unix", addr)
			if err != nil {
				t.Fatal("failed to listen unix socket")
			}
			ch <- struct{}{}
			Accept(l)
		}()
		<-ch
		_, err := XDial("unix@" + addr)
		_assert(err == nil, "failed to connect unix socket")
	}
}
