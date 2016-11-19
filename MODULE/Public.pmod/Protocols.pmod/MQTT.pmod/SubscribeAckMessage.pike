inherit .Message;

constant MESSAGE_TYPE = "SUBACK";
constant MESSAGE_NUMBER = 9;

constant has_variable_header = 1;
constant has_payload = 1;

int packet_identifier;
int response_code; // maximum qos or 0x80 for failure

protected void create() { 
	qos_level = 1; // mandated by spec 
}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  packet_identifier = read_word(body);
  // then the body
  
  response_code = read_byte(body);
}

void encode_variable_header(Stdio.Buffer body) {
	encode_word(body, packet_identifier);
}

void encode_payload(Stdio.Buffer body) {
	encode_byte(body, response_code);
}