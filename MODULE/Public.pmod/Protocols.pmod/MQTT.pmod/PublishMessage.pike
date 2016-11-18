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
  body = read_string(body);
}