inherit .Message;

constant MESSAGE_TYPE = "PUBLISH";
constant MESSAGE_NUMBER = 3;

constant has_variable_header = 1;
constant has_payload = 1;

string topic;
int message_identifier;
string body;

protected void create() {}

void set_qos_level(int(0..3) qos) { qos_level = qos; }

void decode_body(Stdio.Buffer body) {
  // first the variable header
  topic = read_string(body);
  if(qos_level > 0)
    message_identifier = read_word(body);
  this.body = read_string(body);
}

void encode_variable_header(Stdio.Buffer body) {
	encode_string(body, topic);
	if(qos_level > 0)
  	  encode_word(body, message_identifier);
}

void encode_payload(Stdio.Buffer body) {
	encode_string(body, this.body);
}