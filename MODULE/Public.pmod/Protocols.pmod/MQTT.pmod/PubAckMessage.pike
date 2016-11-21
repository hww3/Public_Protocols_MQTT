inherit .Message;

constant MESSAGE_TYPE = "PUBACK";
constant MESSAGE_NUMBER = 4;

constant has_variable_header = 1;
constant has_payload = 0;

int message_identifier;

protected void create() { 
}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  message_identifier = read_word(body);
  // then the body
}

void encode_variable_header(Stdio.Buffer body) {
	encode_word(body, message_identifier);
}
