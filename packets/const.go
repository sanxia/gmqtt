package packets

import (
	"errors"
)

//PacketNames maps the constants for each of the MQTT packet types
//to a string representation of their name.
var PacketNames = map[uint8]string{
	1:  "CONNECT",
	2:  "CONNACK",
	3:  "PUBLISH",
	4:  "PUBACK",
	5:  "PUBREC",
	6:  "PUBREL",
	7:  "PUBCOMP",
	8:  "SUBSCRIBE",
	9:  "SUBACK",
	10: "UNSUBSCRIBE",
	11: "UNSUBACK",
	12: "PINGREQ",
	13: "PINGRESP",
	14: "DISCONNECT",
}

//Below are the constants assigned to each of the MQTT packet types
const (
	Connect     = 1
	Connack     = 2
	Publish     = 3
	Puback      = 4
	Pubrec      = 5
	Pubrel      = 6
	Pubcomp     = 7
	Subscribe   = 8
	Suback      = 9
	Unsubscribe = 10
	Unsuback    = 11
	Pingreq     = 12
	Pingresp    = 13
	Disconnect  = 14
)

//Below are the const definitions for error codes returned by
//Connect()
const (
	Accepted                        = 0x00
	ErrRefusedBadProtocolVersion    = 0x01
	ErrRefusedIDRejected            = 0x02
	ErrRefusedServerUnavailable     = 0x03
	ErrRefusedBadUsernameOrPassword = 0x04
	ErrRefusedNotAuthorised         = 0x05
	ErrNetworkError                 = 0xFE
	ErrProtocolViolation            = 0xFF
)

//ConnackReturnCodes is a map of the error codes constants for Connect()
//to a string representation of the error
var ConnackReturnCodes = map[uint8]string{
	0:   "Connection Accepted",
	1:   "Connection Refused: Bad Protocol Version",
	2:   "Connection Refused: Client Identifier Rejected",
	3:   "Connection Refused: Server Unavailable",
	4:   "Connection Refused: Username or Password in unknown format",
	5:   "Connection Refused: Not Authorised",
	254: "Connection Error",
	255: "Connection Refused: Protocol Violation",
}

//ConnErrors is a map of the errors codes constants for Connect()
//to a Go error
var ConnErrors = map[byte]error{
	Accepted:                        nil,
	ErrRefusedBadProtocolVersion:    errors.New("Unnacceptable protocol version"),
	ErrRefusedIDRejected:            errors.New("Identifier rejected"),
	ErrRefusedServerUnavailable:     errors.New("Server Unavailable"),
	ErrRefusedBadUsernameOrPassword: errors.New("Bad user name or password"),
	ErrRefusedNotAuthorised:         errors.New("Not Authorized"),
	ErrNetworkError:                 errors.New("Network Error"),
	ErrProtocolViolation:            errors.New("Protocol Violation"),
}
