
Stdio.File conn;
string host;
int port;
Stdio.Buffer buf;
Stdio.Buffer outbuf;

constant NOT_CONNECTED = 0;
constant CONNECTING = 1;
constant CONNECTED = 2;

int connection_state = NOT_CONNECTED;

protected void create(string _host, int _port) {
   host = _host;
   port = _port;
}


void connect() {
   connection_state = CONNECTING;
   
   conn = Stdio.File();
   if(!conn->connect(host, port))  {
     connection_state = NOT_CONNECTED;
     throw(Error.Generic("Unable to connect.\n"));
   }

   buf = Stdio.Buffer();
   outbuf = Stdio.Buffer();
   conn->set_buffer_mode(buf, outbuf);
   conn->set_write_callback(write_cb);
   conn->set_close_callback(close_cb);
   conn->set_read_callback(read_cb);
   conn->set_nonblocking_keep_callbacks();
   
   .ConnectMessage m = .ConnectMessage();
   m->keep_alive = 20;
   m->client_identifier = "hww3";
   
   send_message(m);
}

void send_message(.Message m) {
   string msg = m->encode();  
   werror("Adding outbound message to queue: %O => %O\n", m, msg);
   outbuf->add(msg);
}

void disconnect() {
  connection_state = NOT_CONNECTED;
  conn->close();
  reset_state();
}

int packet_started = 0;
int have_length = 0;
int current_header;
int current_length;
array(int) length_bytes = ({});

int get_digit(Stdio.Buffer buf, int digit) {
  if(sizeof(length_bytes) > digit)
    return length_bytes[digit];
  else {
    if(!sizeof(buf)) return -1;
    int val = buf->read_int8();
    length_bytes += ({ val });
    return val;
  }
}

void reset_state() {
// reset everything
  packet_started = 0;
  have_length = 0;
  current_header = 0;
  current_length = 0;
  length_bytes = ({});
}

int wrote_connect;

void write_cb(mixed id) {

  werror("write_cb %O\n", id);
}

void close_cb(mixed id) {
  werror("close_cb %O\n", id);
  reset_state();
}

void read_cb(mixed id, object data) {
  werror("read_cb: %O %O\n", id, data);
  mixed buf = data;

  if(!sizeof(buf)) return 0;
  
  if(!packet_started)
  {
     packet_started = 1;
     current_header = buf->read_int8();
     werror("header: %d\n", current_header);     
     if(!sizeof(buf)) return 0;
  }
  
  if(!have_length) {
     int t = 0;
     int multiplier = 1;
     int value = 0;
     int digit;
     
     do {
        digit = get_digit(buf, t);
        t++;
        if(digit == -1) return;
        value += (digit & 127) * multiplier;
        multiplier *= 128;
      }
      while ((digit & 128) != 0);
      
      have_length = 1;
      current_length = value;
  }
  
  int len = sizeof(buf);
  if(!len || len < current_length) return 0;
  
  string body = buf->read(current_length);
  int h = current_header;
  
  reset_state();
  
  int message_type = (h >> 4) & 15;  

  program mt = Public.Protocols.MQTT.message_registry[message_type];
  if(!mt) throw(Error.Generic("No message type registred for " + message_type + ".\n"));
  
  .Message message = mt();
  message->decode(h, Stdio.Buffer(body));
  
  process_message(message);
}

void process_message(.Message message) {
  werror("got message: %O\n", message);
  if(object_program(message) == .ConnAckMessage) {
    if(connection_state != CONNECTING) 
      throw(Error.Generic("Received CONNACK at invalid point, disconnecting.\n"));
    if(message->response_code != 0) {
      disconnect();
      throw(Error.Generic(sprintf("Server reports connection failed with code %d: %s.\n", message->response_code, message->response_text)));
    }
    werror("response code: %O\n", message->response_code);
    connection_state = CONNECTED;
  }
}