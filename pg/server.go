package pg

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"net/url"
)

type PGProtoServer struct {
	bindAddr net.Addr
}

func (s *PGProtoServer) handleConnection(clientConn net.Conn) {
	clientLogger := log.WithFields(log.Fields{
		"client": clientConn.RemoteAddr(),
	})

	clientLogger.Info("Client connected")
	pg := &PGProtocol{clientConn, clientLogger}
	if err := pg.Loop(); err != nil {
		clientLogger.Error(err)
	}
	clientLogger.Info("Client disconnected")
}

func (s *PGProtoServer) Listen() error {
	l, err := net.Listen(s.bindAddr.Network(), s.bindAddr.String())
	if err != nil {
		return err
	}
	defer l.Close()

	log.Infof("PGProtoServer listening on %s", s.bindAddr)

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go s.handleConnection(conn)
	}
}

func parseAddr(addr string) (net.Addr, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	var bindAddr net.Addr
	switch u.Scheme {
	case "tcp":
		bindAddr, err = net.ResolveTCPAddr("tcp", u.Host)
	case "unix":
		bindAddr, err = net.ResolveUnixAddr("unix", u.Path)
	default:
		err = fmt.Errorf("Invalid network protocol: %s", u.Scheme)
	}
	if err != nil {
		return nil, err
	}
	return bindAddr, nil
}

func NewPGProtoServer(addr string) (*PGProtoServer, error) {
	bindAddr, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	return &PGProtoServer{bindAddr}, nil
}
