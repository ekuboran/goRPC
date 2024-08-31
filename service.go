package gorpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync"
)

// methodType实例包含一个方法的完整信息。
type methodType struct {
	sync.Mutex                // 用于保护numCalls计数
	method     reflect.Method // 方法本身
	ArgType    reflect.Type
	ReplyType  reflect.Type
	numCalls   uint
}

// 将一个结构体映射为一个服务
type service struct {
	name   string                 // 映射的结构体的名称
	typ    reflect.Type           // 映射的结构体的类型
	rcvr   reflect.Value          // 映射的结构体的实例本身
	method map[string]*methodType // 存储映射的结构体的所有符合条件的方法。
}

func (m *methodType) NumCalls() uint {
	m.Lock()
	n := m.numCalls
	m.Unlock()
	return n
}

// 新建对应输入参数类型的实例
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// 参数可能是指针类型或值类型
	if m.ArgType.Kind() == reflect.Ptr {
		// 如果是指针类型，创建一个指向该指针所指类型的新值。
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// 如果是值类型，创建一个新的指向该类型的指针，并取出指针所指的值。
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// 新建对应返回参数类型的实例
func (m *methodType) newReplyv() reflect.Value {
	// reply必须是指针类型
	replyv := reflect.New(m.ReplyType.Elem())
	// 特别处理了 map 和 slice 类型，确保这些复杂类型在创建时被正确地初始化为一个空的 map 或 slice
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType, 0, 0))
	}
	return replyv
}

// 入参为任意需要映射为服务的结构体实例
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	// Indirect(v)返回指针v所指向的值（如果v不是指针，就返回v）
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)
	// 确保服务名称是一个有效的、可导出的（即可被外部包访问的）名称。
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	// 构建s.method
	s.registerMethods()
	return s
}

// 过滤出符合条件的方法，并不是所有的方法都能被映射为服务
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		// 条件1：两个导出或内置类型的入参（反射时为 3 个，第 0 个是自身，类似于 python 的 self，java 中的 this）
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		// 条件2：返回值有且只有 1 个，类型为 error
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		// 入参和返回参数都得是外部可访问的类型
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

// 判断该类型是否被暴露出来
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// call()能够通过反射值调用方法
func (s *service) call(mType *methodType, argv reflect.Value, replyv reflect.Value) error {
	mType.Lock()
	mType.numCalls++
	mType.Unlock()
	f := mType.method.Func
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}
