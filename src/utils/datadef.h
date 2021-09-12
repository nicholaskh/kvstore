#ifndef __DATA_DEF_H__
#define __DATA_DEF_H__

#include <assert.h>
#include <string>

using namespace std;

typedef unsigned int uint32_t;
typedef int int32_t;

#if __WORDSIZE == 64
typedef unsigned long uint64_t;
#else
typedef unsigned long long uint64_t;
#endif

typedef unsigned char uint8_t;
typedef unsigned short uint16_t;

typedef uint32_t user_id_t;

#define U64_MAX 0xffffffffffffffffULL
#define U64_ZERO 0ULL
#define U32_MAX 0xffffffff

typedef enum
{
	S_OK = 0x00,

	S_INVALID_PARA = 0x10,
	S_INVALID_PACK = 0x11,
	S_READ_ERROR = 0x12,
	S_BAD_REQ = 0x13,

	S_END = 0x30,
	S_NOT_FOUND = 0x31,
	S_OUT_OF_SCOPE = 0x32,
	S_MEM_OUT = 0x33,
	S_NOT_SUPPORT = 0x34,

	S_IGNORE = 0x35,

	S_TIMEOUT = 0x41,
	S_BUSY = 0x42,

	S_NONEED_REWRITE = 0x50,

	S_FATAL_RUNTIME = 0xF0,

	S_NULL = 0xFE, // empty
	S_FAIL = 0xFF
} ret_t;

typedef enum
{
	SORT_TYPE_DOWN = 0x00,
	SORT_TYPE_UP = 0x01
} sort_type_t;

typedef enum
{
	CONN_LONG = 0x00,
	CONN_SHORT = 0x01
} conn_type_t;

typedef enum
{
	PTYPE_UNKNOWN = 0x00,
	PTYPE_REQ_PUT_DATA = 0x01,
	PTYPE_REQ_GET_DATA = 0x02,
	PTYPE_REQ_DEL_DATA = 0x03,
	PTYPE_REQ_PUT_SCHEMA = 0x10,
	PTYPE_REQ_GET_SCHEMA = 0x11,

	PTYPE_REQ_PUT_DATA_START = 0x21,
	PTYPE_REQ_PUT_DATA_END = 0x22,
	PTYPE_ACK_PUT_DATA = 0x23,
	PTYPE_ACK_GET_DATA = 0x24,
	PTYPE_ACK_PUT_DATA_START = 0x25,

	PTYPE_HTTP_REQ = 0x70,
	PTYPE_ERRORINFO = 0x71,

	PTYPE_HTTP_RESULT = 0x81,
	PTYPE_RESPONSE = 0xa0,

	PTYPE_REQ_SESSION_END = 0xfe,
	PTYPE_OTHER = 0xff
} pack_type_t;

typedef enum
{
	PF_JSON = 0x00,
	PF_PROTOBUF = 0x01,
	PF_TEXT = 0x02
} pack_format_t;

typedef enum
{
	LOG_L_DEBUG = 0x00,
	LOG_L_INFO = 0x01,
	LOG_L_NOTICE = 0x02,
	LOG_L_WARNING = 0x03,
	LOG_L_ERROR = 0x04,
	LOG_L_FATAL = 0x05,
	LOG_L_ALERT = 0x06
} log_level_t;

#endif
