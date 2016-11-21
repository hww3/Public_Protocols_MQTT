constant MESSAGE_TYPE = "RESERVED";
constant MESSAGE_NUMBER = 0;

constant has_payload = 0;
constant has_variable_header = 0;

protected int(0..2) qos_level = 0;
protected int(0..1) dup_flag = 0;
protected int(0..1) retain_flag = 0;

#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */


protected void create() { throw(Error.Generic("Creation not allowed\n")); }

int get_qos_level() { return qos_level; }

void decode(int header, Stdio.Buffer body) {

  retain_flag = header & 0x01;
  qos_level = (header >> 1) & 0x03;
  dup_flag = (header >> 3) & 0x01;
  
  if(has_payload || has_variable_header)
    decode_body(body);
  else if(sizeof(body))
    throw(Error.Generic("Body provided but message type does not permit one.\n"));
}

string encode() {
  string output;
  int header1;

  header1 = MESSAGE_NUMBER;
  header1 <<= 1;
  header1 += dup_flag;
  header1 <<= 2;
  header1 += qos_level;
  header1 <<= 1;
  header1 += retain_flag;

  output = sprintf("%c", header1);
  
  Stdio.Buffer buf = Stdio.Buffer();
  
  if(has_variable_header) encode_variable_header(buf);
      if(has_payload) encode_payload(buf);
  
  string body = buf->read();
  
  int remaining_length = sizeof(body);
  DEBUG("remaining length: %d %O\n", remaining_length, sprintf("%c", remaining_length));
  do {
    int digit = remaining_length % 128;
    remaining_length = remaining_length / 128;
    // if there are more digits to encode, set the top bit of this digit
    if ( remaining_length > 0 )
      digit = digit | 0x80;
  
     output += sprintf("%c", digit);
  }
  while ( remaining_length > 0 );

  return output + body;
}

// encodes the payload, if any
void encode_payload(Stdio.Buffer buf) { }

void encode_variable_header(Stdio.Buffer buf) { }

//! decode both the variable_header (if any) and payload (if any)
void decode_body(Stdio.Buffer body) { } 

void encode_string(Stdio.Buffer buf, string str) {
  buf->add_hstring(string_to_utf8(str), 2);
}

void encode_byte(Stdio.Buffer buf, int byte) {
  buf->add_int8(byte);
}

void encode_word(Stdio.Buffer buf, int word) {
  buf->add_int16(word);
}

int read_byte(Stdio.Buffer buf) {
  return buf->read_int8();
}

int read_word(Stdio.Buffer buf) {
  return buf->read_int16();
}

string read_string(Stdio.Buffer buf) {
  return utf8_to_string(buf->read_hstring(2) || "");
}