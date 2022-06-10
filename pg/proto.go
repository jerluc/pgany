package pg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
)

const (
	StartupMessage = 196608
	SSLRequest     = 80877103
)

var (
	TypeLen = map[string]int16{
		"bool": 1,
		"text": -1,
	}
	Disconnect = fmt.Errorf("Client disconnected")
)

func WriteMessage(conn io.Writer, parts ...any) (int, error) {
	contentsBuf := bytes.NewBuffer([]byte{})
	for i, part := range parts {
		if i == 0 {
			continue
		}
		binary.Write(contentsBuf, binary.BigEndian, part)
	}

	msgType := parts[0].(byte)
	contents := contentsBuf.Bytes()
	msgBuf := bytes.NewBuffer([]byte{})
	msgBuf.WriteByte(msgType)
	binary.Write(msgBuf, binary.BigEndian, int32(len(contents)+4))
	msgBuf.Write(contents)

	return conn.Write(msgBuf.Bytes())
}

func AuthenticationOk(conn io.Writer) (int, error) {
	return WriteMessage(conn, byte('R'), int32(0))
}

func ReadyForQuery(conn io.Writer) (int, error) {
	return WriteMessage(conn, byte('Z'), byte('I'))
}

func RowDescription(conn io.Writer, data []map[string]any) (int, error) {
	numFields := len(data[0])
	var parts []any = []any{
		byte('T'), int16(numFields)}
	for k := range data[0] {
		// Field name
		name := bytes.NewBufferString(k).Bytes()
		for _, b := range name {
			parts = append(parts,
				b,
			)
		}
		parts = append(parts, byte(0))

		parts = append(parts,
			// Table object ID
			int32(0),
			// Attr number
			int16(0),
			// Data type object ID
			int32(0),
			// Data type size (pg_type.typlen)
			int16(-1),
			// Type modifier (pg_attribute.atttypmod
			int32(0),
			// Format code (0 or 1 for text or binary)
			int16(0),
		)
	}
	return WriteMessage(conn, parts...)
}

func DataRow(conn io.Writer, row map[string]any) (int, error) {
	numFields := len(row)
	var parts []any = []any{
		byte('D'), int16(numFields)}
	for _, v := range row {
		// Value length
		vb := bytes.NewBuffer([]byte{})
		if s, isString := v.(string); isString {
			sb := bytes.NewBufferString(s)
			vb.ReadFrom(sb)
			vb.WriteByte(0)
		} else {
			binary.Write(vb, binary.BigEndian, v)
		}
		parts = append(parts, int32(vb.Len()))
		for _, b := range vb.Bytes() {
			parts = append(parts,
				b,
			)
		}
	}
	return WriteMessage(conn, parts...)
}

func CommandComplete(conn io.Writer, tag string) (int, error) {
	var parts []any = []any{byte('C')}
	tagb := bytes.NewBufferString(tag).Bytes()
	for _, b := range tagb {
		parts = append(parts,
			b,
		)
	}
	parts = append(parts, byte(0))
	return WriteMessage(conn, parts...)
}

type PGProtocol struct {
	clientConn io.ReadWriteCloser
	log        log.Ext1FieldLogger
}

func (pg *PGProtocol) Write(b []byte) (int, error) {
	pg.log.Tracef("[WRITE] %v\n", b)
	n, err := pg.clientConn.Write(b)
	return n, err
}

func (pg *PGProtocol) Startup() (bool, error) {
	msgLength, err := pg.ReadInt32()
	if err != nil {
		return false, err
	}
	contentLength := int64(msgLength - 4)
	buf := bytes.NewBuffer([]byte{})
	n, err := io.CopyN(buf, pg.clientConn, contentLength)
	if err != nil {
		return false, err
	}
	if n != contentLength {
		return false, fmt.Errorf("Buffer underflow, only read %d bytes (expected %d)", n, contentLength)
	}
	var protocolVersion uint32
	err = binary.Read(buf, binary.BigEndian, &protocolVersion)
	if err != nil {
		return false, err
	}

	if protocolVersion == SSLRequest {
		// TODO: Add support for SSL connections
		if err := binary.Write(pg, binary.BigEndian, byte('N')); err != nil {
			return false, err
		}
		// We still need to wait for the StartupMessage before "ready"
		return false, nil
	} else if protocolVersion == StartupMessage {
		for {
			_, err := buf.ReadString(0)
			if err == io.EOF {
				break
			}
			if err != nil {
				return false, err
			}
			// TODO: Authentication loop?
		}
		return true, nil
	}
	return false, fmt.Errorf("Unknown protocol version: %d", protocolVersion)
}

func (pg *PGProtocol) ReadQuery() (string, error) {
	var msgType byte
	err := binary.Read(pg.clientConn, binary.BigEndian, &msgType)
	if err != nil {
		return "", err
	}
	if msgType == 'X' {
		return "", Disconnect
	}
	if msgType != 'Q' {
		return "", fmt.Errorf("Expected 'Q', but got '%s'", string(msgType))
	}
	msgLength, err := pg.ReadInt32()
	if err != nil {
		return "", err
	}
	contentLength := int64(msgLength - 4)
	buf := bytes.NewBuffer([]byte{})
	n, err := io.CopyN(buf, pg.clientConn, contentLength)
	if n != contentLength {
		return "", fmt.Errorf("Buffer underflow, only read %d bytes (expected %d)", n, contentLength)
	}
	return buf.String(), nil
}

func (pg *PGProtocol) ReadInt32() (int32, error) {
	var v int32
	if err := binary.Read(pg.clientConn, binary.BigEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

func (pg *PGProtocol) Loop() error {
	for {
		ready, err := pg.Startup()
		if err != nil {
			return err
		}
		if ready {
			break
		}
	}

	_, err := AuthenticationOk(pg)
	if err != nil {
		return err
	}

	for {
		_, err = ReadyForQuery(pg)
		if err != nil {
			return err
		}

		q, err := pg.ReadQuery()
		if err == Disconnect {
			break
		}
		if err != nil {
			return err
		}

		queryLogger := pg.log.WithField("query", q)
		queryLogger.Debug("Received query")
		// TODO: Actually run the query
		data := []map[string]any{
			{
				"a": 1,
				"b": "B1",
				"c": "C1",
			},
			{
				"a": 2,
				"b": "B2",
				"c": "C2",
			},
		}
		_, err = RowDescription(pg, data)
		if err != nil {
			return err
		}
		for _, row := range data {
			_, err = DataRow(pg, row)
			if err != nil {
				return err
			}
		}
		_, err = CommandComplete(pg, "what")
		if err != nil {
			return err
		}
	}
	return nil
}
