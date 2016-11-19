inherit .Message;

constant MESSAGE_TYPE = "SUBSCRIBE";
constant MESSAGE_NUMBER = 8;

constant has_variable_header = 1;
constant has_payload = 1;

int packet_identifier;
array(array) topics = ({});

protected void create() { 
	qos_level = 1; // mandated by spec 
}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  packet_identifier = read_word(body);
  // then the body
  string topic = "";
  int qos;
  
  while((topic = read_string(body)) != "") {
	  qos = read_byte(body);
	  topics += ({ ({topic, qos}) });
  }
}

void encode_variable_header(Stdio.Buffer body) {
	encode_word(body, packet_identifier);
}

void encode_payload(Stdio.Buffer body) {
	foreach(topics;; array topic) {
		encode_string(body, topic[0]);
		encode_byte(body, topic[1]);
	}
}