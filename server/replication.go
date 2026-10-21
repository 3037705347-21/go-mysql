package server

import (
	"encoding/binary"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ParseBinlogDumpRequest parses the payload of COM_BINLOG_DUMP and returns the
// requesting replica's server id together with the requested start position.
//
// Payload layout: binlog-pos (4 bytes), flags (2 bytes), server-id (4 bytes),
// binlog-filename (rest of the packet).
func ParseBinlogDumpRequest(data []byte) (uint32, mysql.Position, error) {
	if len(data) < 10 {
		return 0, mysql.Position{}, mysql.ErrMalformPacket
	}
	serverID := binary.LittleEndian.Uint32(data[4:8])
	p := mysql.Position{
		Pos:  binary.LittleEndian.Uint32(data[0:4]),
		Name: string(data[10:]),
	}

	return serverID, p, nil
}

func parseBinlogDump(data []byte) (mysql.Position, error) {
	_, p, err := ParseBinlogDumpRequest(data)
	return p, err
}

// ParseBinlogDumpGTIDRequest parses the payload of COM_BINLOG_DUMP_GTID and
// returns the requesting replica's server id together with the requested GTID
// set.
func ParseBinlogDumpGTIDRequest(data []byte) (uint32, *mysql.MysqlGTIDSet, error) {
	if len(data) < 22 {
		return 0, nil, mysql.ErrMalformPacket
	}

	serverID := binary.LittleEndian.Uint32(data[2:6])

	pos := 0
	pos += 2 // flags
	pos += 4 // server_id

	// binlog filename length (at offset 6)
	nameSize := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if len(data) < pos+nameSize+8+4 {
		return 0, nil, mysql.ErrMalformPacket
	}

	pos += nameSize // binlog filename
	pos += 8        // binlog position

	// GTID data length
	dataSize := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if len(data) < pos+dataSize {
		return 0, nil, mysql.ErrMalformPacket
	}

	// parse GTID set
	gtidSet, err := mysql.DecodeMysqlGTIDSet(data[pos : pos+dataSize])
	if err != nil {
		return 0, nil, err
	}
	return serverID, gtidSet, nil
}

func parseBinlogDumpGTID(data []byte) (*mysql.MysqlGTIDSet, error) {
	_, gtidSet, err := ParseBinlogDumpGTIDRequest(data)
	return gtidSet, err
}
