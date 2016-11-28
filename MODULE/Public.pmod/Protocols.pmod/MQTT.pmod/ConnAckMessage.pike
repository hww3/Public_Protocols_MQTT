inherit .Message;

constant MESSAGE_TYPE = "CONNACK";
constant MESSAGE_NUMBER = 2;

constant has_variable_header = 1;
constant has_payload = 0;

int compression_flags = 0;
int response_code = 0;
string response_text = "Unknown";

protected void create() {}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  compression_flags = read_byte(body); // reserved
  response_code = read_byte(body);
  response_text = response_texts[response_code] || "Unknown";
  
}

void encode_variable_header(Stdio.Buffer buf) { 
  encode_byte(buf, compression_flags);
  encode_byte(buf, response_code);
}

protected mapping response_texts = ([
0x00: "Connection Accepted",
0x01: "Connection Refused: unacceptable protocol version",
0x02: "Connection Refused: identifier rejected",
0x03: "Connection Refused: server unavailable",
0x04: "Connection Refused: bad user name or password",
0x05: "Connection Refused: not authorized",
]);
/* 6-255 		Reserved for future user */

