inherit .Message;

constant MESSAGE_TYPE = "UNSUBSCRIBE";
constant MESSAGE_NUMBER = 10;

constant has_variable_header = 1;
constant has_payload = 1;

int packet_identifier;
array(string) topics = ({});

protected void create() { 
	qos_level = 1; // mandated by spec 
}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  packet_identifier = read_word(body);
  // then the body
  string topic = "";
  
  while((topic = read_string(body)) != "") {
	  topics += ({ topic });
  }
}

void encode_variable_header(Stdio.Buffer body) {
	encode_word(body, packet_identifier);
}

void encode_payload(Stdio.Buffer body) {
	foreach(topics;; string topic) {
		encode_string(body, topic);
	}
}