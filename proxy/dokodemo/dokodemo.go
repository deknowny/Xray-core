package dokodemo

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pires/go-proxyproto"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		d := new(DokodemoDoor)
		err := core.RequireFeatures(ctx, func(pm policy.Manager) error {
			return d.Init(config.(*Config), pm, session.SockoptFromContext(ctx))
		})
		return d, err
	}))
}

type DokodemoDoor struct {
	policyManager policy.Manager
	config        *Config
	address       net.Address
	port          net.Port
	portMap       map[string]string
	sockopt       *session.Sockopt
}

// Init initializes the DokodemoDoor instance with necessary parameters.
func (d *DokodemoDoor) Init(config *Config, pm policy.Manager, sockopt *session.Sockopt) error {
	if len(config.Networks) == 0 {
		return errors.New("no network specified")
	}
	d.config = config
	d.address = config.GetPredefinedAddress()
	d.port = net.Port(config.Port)
	d.portMap = config.PortMap
	d.policyManager = pm
	d.sockopt = sockopt

	return nil
}

// Network implements proxy.Inbound.
func (d *DokodemoDoor) Network() []net.Network {
	return d.config.Networks
}

func (d *DokodemoDoor) policy() policy.Session {
	config := d.config
	p := d.policyManager.ForLevel(config.UserLevel)
	return p
}

// Process implements proxy.Inbound.
func (d *DokodemoDoor) Process(ctx context.Context, network net.Network, conn stat.Connection, dispatcher routing.Dispatcher) error {
	errors.LogDebug(ctx, "processing connection from: ", conn.RemoteAddr())
	dest := net.Destination{
		Network: network,
		Address: d.address,
		Port:    d.port,
	}

	if !d.config.FollowRedirect {
		host, port, err := net.SplitHostPort(conn.LocalAddr().String())
		if dest.Address == nil {
			if err != nil {
				dest.Address = net.DomainAddress("localhost")
			} else {
				if strings.Contains(host, ".") {
					dest.Address = net.LocalHostIP
				} else {
					dest.Address = net.LocalHostIPv6
				}
			}
		}
		if dest.Port == 0 {
			dest.Port = net.Port(common.Must2(strconv.Atoi(port)))
		}
		if d.portMap != nil && d.portMap[port] != "" {
			h, p, _ := net.SplitHostPort(d.portMap[port])
			if len(h) > 0 {
				dest.Address = net.ParseAddress(h)
			}
			if len(p) > 0 {
				dest.Port = net.Port(common.Must2(strconv.Atoi(p)))
			}
		}
	}

	destinationOverridden := false
	if d.config.FollowRedirect {
		outbounds := session.OutboundsFromContext(ctx)
		if len(outbounds) > 0 {
			ob := outbounds[len(outbounds)-1]
			if ob.Target.IsValid() {
				dest = ob.Target
				destinationOverridden = true
			}
		}
		iConn := stat.TryUnwrapStatsConn(conn)
		if tlsConn, ok := iConn.(tls.Interface); ok && !destinationOverridden {
			if serverName := tlsConn.HandshakeContextServerName(ctx); serverName != "" {
				dest.Address = net.DomainAddress(serverName)
				destinationOverridden = true
				ctx = session.ContextWithMitmServerName(ctx, serverName)
			}
			if tlsConn.NegotiatedProtocol() != "h2" {
				ctx = session.ContextWithMitmAlpn11(ctx, true)
			}
		}
	}
	if !dest.IsValid() || dest.Address == nil {
		return errors.New("unable to get destination")
	}

	inbound := session.InboundFromContext(ctx)
	inbound.Name = "dokodemo-door"
	inbound.CanSpliceCopy = 1
	inbound.User = &protocol.MemoryUser{
		Level: d.config.UserLevel,
	}

	ctx = log.ContextWithAccessMessage(ctx, &log.AccessMessage{
		From:   conn.RemoteAddr(),
		To:     dest,
		Status: log.AccessAccepted,
		Reason: "",
	})
	errors.LogInfo(ctx, "received request for ", conn.RemoteAddr())

	var reader buf.Reader
	if dest.Network == net.Network_TCP {
		reader = buf.NewReader(conn)
		reader = maybePrependProxyProtocol(ctx, reader, conn)
	} else {
		reader = buf.NewPacketReader(conn)
	}

	var writer buf.Writer
	if network == net.Network_TCP {
		writer = buf.NewWriter(conn)
	} else {
		// if we are in TPROXY mode, use linux's udp forging functionality
		if !destinationOverridden {
			writer = &buf.SequentialWriter{Writer: conn}
		} else {
			back := conn.RemoteAddr().(*net.UDPAddr)
			if !dest.Address.Family().IsIP() {
				if len(back.IP) == 4 {
					dest.Address = net.AnyIP
				} else {
					dest.Address = net.AnyIPv6
				}
			}
			addr := &net.UDPAddr{
				IP:   dest.Address.IP(),
				Port: int(dest.Port),
			}
			var mark int
			if d.sockopt != nil {
				mark = int(d.sockopt.Mark)
			}
			pConn, err := FakeUDP(addr, mark)
			if err != nil {
				return err
			}
			writer = NewPacketWriter(pConn, &dest, mark, back)
			defer writer.(*PacketWriter).Close() // close fake UDP conns
		}
	}

	if err := dispatcher.DispatchLink(ctx, dest, &transport.Link{
		Reader: reader,
		Writer: writer},
	); err != nil {
		return errors.New("failed to dispatch request").Base(err)
	}
	return nil // Unlike Dispatch(), DispatchLink() will not return until the outbound finishes Process()
}

type proxyProtocolReader struct {
	header []byte
	reader buf.Reader
}

func (r *proxyProtocolReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if len(r.header) > 0 {
		header := r.header
		r.header = nil
		return buf.MultiBuffer{buf.FromBytes(header)}, nil
	}

	return r.reader.ReadMultiBuffer()
}

func (r *proxyProtocolReader) ReadMultiBufferTimeout(duration time.Duration) (buf.MultiBuffer, error) {
	if len(r.header) > 0 {
		return r.ReadMultiBuffer()
	}

	if timeoutReader, ok := r.reader.(buf.TimeoutReader); ok {
		return timeoutReader.ReadMultiBufferTimeout(duration)
	}

	return r.reader.ReadMultiBuffer()
}

func maybePrependProxyProtocol(ctx context.Context, reader buf.Reader, conn stat.Connection) buf.Reader {
	version := prependProxyProtocolVersion(ctx)
	if version == 0 {
		return reader
	}

	header := proxyproto.HeaderProxyFromAddrs(version, conn.RemoteAddr(), conn.LocalAddr())
	var payload bytes.Buffer
	if _, err := header.WriteTo(&payload); err != nil {
		errors.LogInfoInner(ctx, err, "failed to build prepend PROXY protocol header")
		return reader
	}
	if payload.Len() == 0 {
		return reader
	}

	return &proxyProtocolReader{
		header: payload.Bytes(),
		reader: reader,
	}
}

func prependProxyProtocolVersion(ctx context.Context) byte {
	raw := strings.TrimSpace(os.Getenv("XRAY_PREPEND_PROXY_PROTOCOL"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("MTCNF_XRAY_PREPEND_PROXY_PROTOCOL"))
	}
	if raw == "" || raw == "0" {
		return 0
	}

	inbound := session.InboundFromContext(ctx)
	if inbound == nil {
		return 0
	}

	tag := strings.TrimSpace(os.Getenv("XRAY_PREPEND_PROXY_PROTOCOL_INBOUND_TAG"))
	if tag == "" {
		tag = "public-in"
	}
	if tag != "*" && inbound.Tag != tag {
		return 0
	}

	version, err := strconv.ParseUint(raw, 10, 8)
	if err != nil || version < 1 || version > 2 {
		errors.LogWarning(ctx, "invalid XRAY_PREPEND_PROXY_PROTOCOL=", raw, "; expected 1 or 2")
		return 0
	}

	return byte(version)
}

func NewPacketWriter(conn net.PacketConn, d *net.Destination, mark int, back *net.UDPAddr) buf.Writer {
	writer := &PacketWriter{
		conn:  conn,
		conns: make(map[net.Destination]net.PacketConn),
		mark:  mark,
		back:  back,
	}
	writer.conns[*d] = conn
	return writer
}

type PacketWriter struct {
	conn  net.PacketConn
	conns map[net.Destination]net.PacketConn
	mark  int
	back  *net.UDPAddr
}

func (w *PacketWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	for {
		mb2, b := buf.SplitFirst(mb)
		mb = mb2
		if b == nil {
			break
		}
		var err error
		if b.UDP != nil && b.UDP.Address.Family().IsIP() {
			conn := w.conns[*b.UDP]
			if conn == nil {
				conn, err = FakeUDP(
					&net.UDPAddr{
						IP:   b.UDP.Address.IP(),
						Port: int(b.UDP.Port),
					},
					w.mark,
				)
				if err != nil {
					errors.LogInfo(context.Background(), err.Error())
					b.Release()
					continue
				}
				w.conns[*b.UDP] = conn
			}
			_, err = conn.WriteTo(b.Bytes(), w.back)
			if err != nil {
				errors.LogInfo(context.Background(), err.Error())
				w.conns[*b.UDP] = nil
				conn.Close()
			}
			b.Release()
		} else {
			_, err = w.conn.WriteTo(b.Bytes(), w.back)
			b.Release()
			if err != nil {
				buf.ReleaseMulti(mb)
				return err
			}
		}
	}
	return nil
}

func (w *PacketWriter) Close() error {
	for _, conn := range w.conns {
		if conn != nil {
			conn.Close()
		}
	}
	return nil
}
