#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

protected Stdio.File|SSL.File conn;
protected string host;
protected int port;
protected string username;
protected string password;

protected Stdio.Buffer buf;
protected Stdio.Buffer outbuf;
protected Pike.Backend backend;

constant NOT_CONNECTED = 0;
constant CONNECTING = 1;
constant CONNECTED = 2;

constant CONNECT_STATES = ([0: "NOT_CONNECTED", 1: "CONNECTING", 2: "CONNECTED"]);

protected int connection_state = NOT_CONNECTED;

protected Standards.URI connect_url;
protected string client_identifier;
protected int timeout = 10;
protected function(.client:void) connect_cb;
protected function(.client:void) disconnect_cb;
protected mixed timeout_callout_id;

protected int response_timeout = 10;
protected int max_retries = 1;

mapping(string:multiset) publish_callbacks = ([]);

// packet parsing state
protected int packet_started = 0;
protected int have_length = 0;
protected int current_header;
protected int current_length;
protected array(int) length_bytes = ({});

// protocol state
protected int wrote_connect;
protected int message_identifier = 0;
protected int packet_identifier = 0;

protected mapping(int:.Message) pending_responses = ([]);

//! MQTT client
//!
//! Limitations:
//! 
//! currently does not support QOS levels greater than zero
//! error checking is less than ideal

//!
protected variant void create(string _host, int _port) {
   create("mqtt://" + host + ":" + port);
}

//!
protected variant void create(string _connect_url) {
    connect_url = Standards.URI(_connect_url);
	if(!(<"mqtt", "mqtts">)[connect_url->scheme]) throw(Error.Generic("Connect url must be of type mqtt or mqtts.\n"));
	
	host = connect_url->host;
	port = connect_url->port;
	username = connect_url->user;
	password = connect_url->password;
	
	if(!port) {
		if(connect_url->scheme == "mqtts") port = 8883;
		else port = 1883;
	}
	
	backend = Pike.DefaultBackend;
}

//!
int is_connected() { return connection_state == CONNECTED; }

//! the seconds to wait for a response from the server for acknowledged requests
void set_response_timeout(int secs) {
  response_timeout = secs;
}

//! number of times to retry transmission for requests whose acknowledgements have timed out
void set_retry_attempts(int count) {
  max_retries = count;
}

//!
variant void connect(function(.client:void) _connect_cb) {
	connect_cb = _connect_cb;
	connect();
}

//!
variant void connect() {
   if(connection_state != NOT_CONNECTED) throw(Error.Generic("Connection already in progress.\n"));
	
   connection_state = CONNECTING;
   
   conn = Stdio.File();
   conn->set_blocking();
   DEBUG("connecting to %s, %d.\n", host, port);
   if(!conn->connect(host, port))  {
     connection_state = NOT_CONNECTED;
     throw(Error.Generic("Unable to connect to MQTT server.\n"));
   }

   if(connect_url->scheme == "mqtts") {
	   DEBUG("Starting SSL/TLS\n");
       conn = SSL.File(conn, SSL.Context());
	   conn->set_blocking();
	   if(!conn->connect(host))
	     throw(Error.Generic("Unable to start TLS session with MQTT server.\n"));
	   conn->write("");
   }

   buf = Stdio.Buffer();
   outbuf = Stdio.Buffer();
   if(connect_url->scheme != "mqtts")
     conn->set_buffer_mode(buf, outbuf);
   conn->set_write_callback(write_cb);
   conn->set_close_callback(close_cb);
   conn->set_read_callback(read_cb);
   conn->set_nonblocking_keep_callbacks();
   
   .ConnectMessage m = .ConnectMessage();
   m->keep_alive = timeout;
   m->client_identifier = get_client_identifier();
   if(username)
   {
       m->has_username = 1;
	   m->username = username;
	   if(password)
	   {
  	     m->has_password = 1;
  	     m->password = password;
	   }
   }
   
   send_message(m);
}

//!
void set_disconnect_callback(function(.client:void) cb) {
	disconnect_cb = cb;
}

//!
string get_client_identifier() {
	if(!client_identifier) { 
		client_identifier = gethostname();
	    if(sizeof(client_identifier) > 23) client_identifier = client_identifier[0..22]; 
	}	
	return client_identifier;
 }

 //!
void set_client_identifier(string id) {
	if(sizeof(id) > 23) throw(Error.Generic("Client identifier must be less than 24 bytes in length.\n"));
	client_identifier = id;
}

//!
void disconnect() {
  check_connected();
  low_disconnect();
}

protected void low_disconnect() {
  if(conn->is_open()) {
    .DisconnectMessage message = .DisconnectMessage();
    send_message_sync(message);
    conn->close();
  }
  reset_connection();
  reset_state();
}

//!
void publish(string topic, string msg) {
  check_connected();  
  
  .PublishMessage message = .PublishMessage();
  message->topic = topic;
  message->message_identifier = get_message_identifier();
  message->body = msg;
  
  send_message(message);
}

//!
void subscribe(string topic, function(.client,string,string:void) publish_cb) {
    check_connected();  
  
    .SubscribeMessage message = .SubscribeMessage();
    message->packet_identifier = get_packet_identifier();
    message->topics += ({ ({topic, 0}) }); // basic level of QOS

    if(!publish_callbacks[topic])
  	  publish_callbacks[topic] = (<>);

    publish_callbacks[topic] += (<publish_cb>);		  
    // should be send_message_await_response()  
    .Message response = send_message_await_response(message);
    if(!response) throw(Error.Generic("Subscribe was not acknowledged.\n"));

}

//!
void unsubscribe(string topic, function(.client,string,string:void) publish_cb) {
    check_connected();  
    multiset cbs = publish_callbacks[topic];
    if(!cbs) return 0;
	if(cbs[publish_cb]) cbs[publish_cb] = 0;
	if(!sizeof(cbs)) {
	    .UnsubscribeMessage message = .UnsubscribeMessage();
	    message->packet_identifier = get_packet_identifier();
	    message->topics += ({topic});
	    // should be send_message_await_response()  
	    .Message response = send_message_await_response(message);
	    if(!response) throw(Error.Generic("Unsubscribe was not acknowledged.\n"));
		m_delete(publish_callbacks, topic);
	}
}

protected void check_connected() {
	if(connection_state != CONNECTED)
	throw(Error.Generic("Not connected to MQTT server.\n"));
}

protected int get_message_identifier() {
	return ++message_identifier;
}

protected int get_packet_identifier() {
	return ++packet_identifier;
}

protected void send_message(.Message m) {
   string msg = m->encode();  
   DEBUG("Adding outbound message to queue: %O => %O\n", m, msg);
   outbuf->add(msg);
   if(connect_url->scheme == "mqtts") {
	   conn->write("");
   }
   
   if(timeout_callout_id) remove_call_out(timeout_callout_id);
   timeout_callout_id = call_out(send_ping, (timeout > 1? timeout - 1: 0.5));
}

protected void send_message_sync(.Message m) {
   string msg = m->encode();  
   DEBUG("Sending outbound message synchronously: %O => %O\n", m, msg);
   conn->set_blocking_keep_callbacks();
   conn->write(msg);
   conn->set_nonblocking_keep_callbacks();
   if(timeout_callout_id) remove_call_out(timeout_callout_id);
   timeout_callout_id = call_out(send_ping, (timeout > 1? timeout - 1: 0.5));
}

protected .Message send_message_await_response(.Message m) {
  .Message r = 0;
  int packet_identifier = m->packet_identifier;

  if(!packet_identifier) throw(Error.Generic("No packet identifier specified. Cannot receive a response.\n"));
  int attempts = 0;

  register_pending(packet_identifier);
  do {
    send_message(m);
    r = await_response(packet_identifier, response_timeout);
    if(r) break;
    m->dup_flag = 1;
  }  while(attempts++ < max_retries);

  unregister_pending(packet_identifier);

  return r;
}

Thread.Mutex await_mutex = Thread.Mutex();

protected .Message await_response(int packet_identifier, int timeout) {
  .Message m = 0;
  float f = (float)timeout;
  
  DEBUG("await_response %d\n", packet_identifier);
  if((m = pending_responses[packet_identifier])) 
    return m;

  object key = await_mutex->lock();
  Pike.Backend orig = conn->query_backend();
  Pike.Backend b = Pike.Backend();
  conn->set_backend(b);
  while(f > 0.0) {
    DEBUG("waiting for message %d\n", packet_identifier);
    f = f - b(f);
    if((m = pending_responses[packet_identifier])) 
      break;
  }

  conn->set_backend(orig);
  key = 0;
  return m; // timeout
}

protected void register_pending(int packet_identifier) {
  pending_responses[packet_identifier] = 0;
}

protected void unregister_pending(int packet_identifier) {
  if(has_index(pending_responses, packet_identifier)) {
    DEBUG("clearing pending response marker for %d\n", packet_identifier);
    m_delete(pending_responses, packet_identifier);
  }
}

protected int get_digit(Stdio.Buffer buf, int digit) {
  if(sizeof(length_bytes) > digit)
    return length_bytes[digit];
  else {
    if(!sizeof(buf)) return -1;
    int val = buf->read_int8();
    length_bytes += ({ val });
    return val;
  }
}

protected void send_ping() {
	.PingMessage message = .PingMessage();
	
	send_message(message);
}

protected void reset_state() {
// reset everything
  packet_started = 0;
  have_length = 0;
  current_header = 0;
  current_length = 0;
  length_bytes = ({});
}

protected void reset_connection() {
    connection_state = NOT_CONNECTED;
    if(timeout_callout_id)
      remove_call_out(timeout_callout_id);
    if(disconnect_cb)
	  disconnect_cb(this);
}

protected int write_cb(mixed id) {
  DEBUG("write_cb %O\n", id);
  if(connect_url->scheme == "mqtts" && sizeof(outbuf))
  {
	  string s = outbuf->read();
	  int tosend = sizeof(s);
	  int tot = 0;
	  DEBUG("writing %d\n", sizeof(s));
	  while(tot < tosend) {
       int sent = conn->write(s);
	   tot += sent;
	   if(tot < tosend && sent > 0) s = s[sent..];
    }
	  DEBUG("wrote %d\n", tot);
	  return tot;
  
  }
  return 0;
}

protected void close_cb(mixed id) {
  DEBUG("close_cb %O\n", id);
  reset_connection();
  reset_state();
}

protected void read_cb(mixed id, object data) {
  DEBUG("read_cb: %O %O\n", id, data);
  if(connect_url->scheme == "mqtts")
  {
    buf->add(data);
	data = buf;
  }
  
  mixed buf = data;

  while(sizeof(buf)) {
  
  if(!packet_started)
  {
     packet_started = 1;
     current_header = buf->read_int8();
     DEBUG("header: %d\n", current_header);     
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
        if(digit == -1) return 0;
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
}

protected void process_message(.Message message) {
  DEBUG("got message: %O\n", message);
  int packet_identifier = message->packet_identifier;
  if(packet_identifier) {
    DEBUG("Got a message with a packet_identifier: %d\n", packet_identifier);
    if(has_index(pending_responses, packet_identifier)) pending_responses[packet_identifier] = message;
  }
  if(object_program(message) == .ConnAckMessage) {
    if(connection_state != CONNECTING) 
      throw(Error.Generic("Received CONNACK at invalid point, disconnecting.\n"));
    if(message->response_code != 0) {
      low_disconnect();
      throw(Error.Generic(sprintf("Server reports connection failed with code %d: %s.\n", message->response_code, message->response_text)));
    }
    connection_state = CONNECTED;
    DEBUG("%O got connect response code: %O\n", this, message->response_code);
	if(connect_cb) connect_cb(this);
  }
  
  else if(object_program(message) == .PublishMessage) {
	  multiset cbs; 
	  if((cbs = publish_callbacks[message->topic])) {
		  foreach(cbs; function callback;) {
			  DEBUG("Scheduling delivery of message from %s to %O\n", message->topic, callback);
			  call_out(callback, 0, this, message->topic, message->body);
		  }
	  }
	  else {
   	    DEBUG("WARNING: got publish message for something we have no record of subscribing to: " + message->topic + "\n");
	  }
  }
  
  else if(object_program(message) == .SubscribeAckMessage) {
      DEBUG("%O subscribe got response code: %O\n", this, message->response_code);
  }
  else if(object_program(message) == .UnsubscribeAckMessage) {
      DEBUG("%O unsubscribe got response\n", this);
  }

}
  
protected string _sprintf(mixed t) {
	  return "MQTT.client(" + (string)connect_url + "=>" + CONNECT_STATES[connection_state] + ")";
}