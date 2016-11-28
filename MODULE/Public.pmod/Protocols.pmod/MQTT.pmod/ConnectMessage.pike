inherit .Message;

constant MESSAGE_TYPE = "CONNECT";
constant MESSAGE_NUMBER = 1;

constant has_variable_header = 1;
constant has_payload = 1;

int protocol_version;
int has_username;
int has_password;
int will_retain;
int will_qos;
int will_flag;
int clean_session;
int keep_alive;

string client_identifier;
string will_topic;
string/* 7bit */ will_message;
string username;
string password;

protected void create() {}

void decode_body(Stdio.Buffer body) {
  // first the variable header
  string cookie = read_string(body);
  if(!cookie || cookie != "MQIsdp")
    throw(Error.Generic("Bad protocol identifier: " + cookie + ".\n"));
  
  protocol_version = read_byte(body);
  int flags = read_byte(body);
  keep_alive = read_word(body);
  clean_session = (flags >> 1) & 1;
  will_flag = (flags >> 2) & 1;
  will_qos = (flags >> 4) & 3;
  will_retain = (flags >> 5) & 1;
  has_password = (flags >> 6) & 1;
  has_username = (flags >> 7) & 1;
  
  // now the body
  client_identifier = read_string(body);
  if(will_flag) {
    will_topic = read_string(body);
    will_message = read_string(body);
  }
  
  if(has_username)
    username = read_string(body);
  if(has_password)
    password = read_string(body);
}

void encode_variable_header(Stdio.Buffer buf) { 
   
   encode_string(buf, "MQIsdp");
   encode_byte(buf, 3);
   
   int flags = 0;
   
   flags += has_username;
   flags <<= 1;
   flags += has_password;
   flags <<= 1;
   flags += will_retain;
   flags <<= 1;
   flags += will_qos;
   flags <<= 2;
   flags += will_flag;
   flags <<= 1;
   flags += clean_session;
   flags <<= 1;
   
   encode_byte(buf, flags);
   encode_word(buf, keep_alive);
}

void encode_payload(Stdio.Buffer buf) {
  encode_string(buf, client_identifier);
  if(will_flag) {
    encode_string(buf, will_topic);
    encode_string(buf, will_message);
  }
  
  if(has_username)
    encode_string(buf, username);

  if(has_password)
    encode_string(buf, password);
}