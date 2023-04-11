package waCache

// ByteView 用于表示缓存值
type ByteView struct {
	bytes []byte
}

// Len 实现Value接口,获取数据长度
func (v ByteView) Len() int {
	return len(v.bytes)
}

// ByteSlice 实现只读功能，返回拷贝的切片防篡改
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.bytes)
}

func cloneBytes(bytes []byte) []byte {
	c := make([]byte, len(bytes))
	copy(c, bytes)
	return c
}

func (v ByteView) String() string {
	return string(v.bytes)
}
