// Package input creates a benthos input
package input

import (
	"context"
	"fmt"
	"github.com/Jeffail/benthos/v3/public/service"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
)

var inputConfigSpec = service.NewConfigSpec().
	Summary("This input starts a UDP server and pipes the incoming packets to upstream pipeline").
	Field(service.NewStringField(addressParam)).
	Field(service.NewIntField(maxBufferSizeParam).Default(defaultMaxBufferSize)).
	Field(service.NewIntField(maxInFlightParam).Default("1"))

func init() {
	err := service.RegisterInput("udp", inputConfigSpec, newUDPInput)
	if err != nil {
		panic(err)
	}
}

type udpInput struct {
	messageChan chan *service.Message
	done        chan struct{}
	count       uint64
	conf        UDPServerConfig
	conn        *net.UDPConn
	connLock    sync.RWMutex
	logger      *service.Logger
}

func newUDPInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	address, err := conf.FieldString(addressParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", addressParam, err)
	}
	maxBufferSize, err := conf.FieldInt(maxBufferSizeParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", maxBufferSizeParam, err)
	}
	maxInfFlight, err := conf.FieldInt(maxInFlightParam)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", maxInFlightParam, err)
	}

	a, _ := net.ResolveUDPAddr("udp", address)
	conn, err := net.ListenUDP("udp", a)
	if err != nil {
		return nil, err
	}

	config := UDPServerConfig{
		maxBufferSize: maxBufferSize,
		maxInFlight:   maxInfFlight,
	}

	input := udpInput{
		conf:        config,
		conn:        conn,
		messageChan: make(chan *service.Message, maxInfFlight),
		done:        make(chan struct{}),
		logger:      mgr.Logger(),
	}

	return &input, nil
}

//------------------------------------------------------------------------------

// UDPServerConfig contains configuration fields for the UDP inpout type.
type UDPServerConfig struct {
	maxBufferSize int
	maxInFlight   int
}

//------------------------------------------------------------------------------

func (c *udpInput) listenUDPPackets() {
	for {
		buf := make([]byte, c.conf.maxBufferSize)
		n, sa, err := c.conn.ReadFrom(buf)
		if err != nil {
			c.logger.Errorf("error reading UDP packet: %v", err)
		}
		msg := service.NewMessage(buf[:n])
		msg.MetaSet("source_address", sa.String())
		c.messageChan <- msg
	}
}

func (c *udpInput) Connect(ctx context.Context) error {
	go c.listenUDPPackets()
	return nil
}

func (c *udpInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case message := <-c.messageChan:
		atomic.AddUint64(&c.count, 1)
		message.MetaSet("count", strconv.FormatUint(c.count, 10))
		return message, func(ctx context.Context, err error) error {
			return nil
		}, nil
	case <-c.done:
		return nil, nil, service.ErrEndOfInput
	}
}

func (c *udpInput) Close(ctx context.Context) error {
	return nil
}
