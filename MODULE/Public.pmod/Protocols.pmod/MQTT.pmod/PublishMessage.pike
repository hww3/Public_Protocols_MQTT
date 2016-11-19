inherit .Message;

constant MESSAGE_TYPE = "PUBLISH";
constant MESSAGE_NUMBER = 3;

constant has_variable_header = 1;
constant has_payload = 1;

string topic;
int message_identifier;
string body;

protected void create() {}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  topic = read_string(body);
  message_identifier = read_word(body);
  this.body = read_string(body);
}

void encode_variable_header(Stdio.Buffer body) {
	encode_string(body, topic);
	encode_word(body, message_identifier);
}

void encode_payload(Stdio.Buffer body) {
	encode_string(body, this.body);
}