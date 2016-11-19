inherit .Message;

constant MESSAGE_TYPE = "UNSUBACK";
constant MESSAGE_NUMBER = 11;

constant has_variable_header = 1;
constant has_payload = 0;

int packet_identifier;

protected void create() { 
}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  packet_identifier = read_word(body);
  // then the body
}

void encode_variable_header(Stdio.Buffer body) {
	encode_word(body, packet_identifier);
}
