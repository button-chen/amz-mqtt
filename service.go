package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

var (
	ErrInvalidConnectionType  error = errors.New("service: Invalid connection type")
	ErrInvalidSubscriber      error = errors.New("service: Invalid subscriber")
	ErrBufferNotReady         error = errors.New("service: buffer is not ready")
	ErrBufferInsufficientData error = errors.New("service: buffer has insufficient data.")
)

func getMsgBuff(c io.Closer) ([]byte, error) {
	if c == nil {
		return nil, ErrInvalidConnectionType
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return nil, ErrInvalidConnectionType
	}

	var (
		buf []byte

		// 临时缓冲，读取单个字节
		b = make([]byte, 1)

		// 读取字节总长度
		l int
	)

	// 读取消息类型 1 字节， 与最多 4 字节的剩余长度
	for {
		// 如果读完第5个字节还没有break 是错误的
		if l > 5 {
			return nil, fmt.Errorf("connect/getMessage: 4th byte of remaining length has continuation bit set")
		}

		n, err := conn.Read(b[0:])
		if err != nil {
			return nil, err
		}

		buf = append(buf, b...)
		l += n

		// 见文档剩余长度的表示，最高位设置表示还有后续，反之说明剩余长度的大小读取完毕
		if l > 1 && b[0] < 0x80 {
			break
		}
	}

	// 解析消息剩余长度的大小（注： 此时buf中最多有5字节）
	remlen, _ := binary.Uvarint(buf[1:])
	buf = append(buf, make([]byte, remlen)...)

	for l < len(buf) {
		n, err := conn.Read(buf[l:])
		if err != nil {
			return nil, err
		}
		l += n
	}

	return buf, nil
}

func sendMsgBuff(c io.Closer, buf []byte) error {
	if c == nil {
		return ErrInvalidConnectionType
	}

	conn, ok := c.(net.Conn)
	if !ok {
		return ErrInvalidConnectionType
	}
	n, err := conn.Write(buf)
	if err != nil {

	}
	// TODO
	fmt.Println("send byte ", n)
	return nil
}

func HandConn(c io.Closer) {
	defer func() {
		c.Close()
	}()
	buf, err := getMsgBuff(c)
	if err != nil {
		return
	}
	mqtt, err := Decode(buf)
	if err != nil {
		return
	}
	fmt.Printf("connect info: %v", mqtt)
	// 回复确认连接请求
	repconn := &Mqtt{}
	connfg := &ConnectFlags{}
	h := &Header{}
	h.MessageType = CONNACK

	connfg.CleanSession = true
	repconn.ReturnCode = ACCEPTED
	repconn.ConnectFlags = connfg
	repconn.Header = h

	repbuf, err := Encode(repconn)
	if err != nil {
		return
	}
	sendMsgBuff(c, repbuf)

	rbuf := make([]byte, 1024)
	conn, _ := c.(net.Conn)
	for {
		n, err := conn.Read(rbuf)
		if err != nil {
			break
		}
		fmt.Println("recv: ", n)
	}
}
