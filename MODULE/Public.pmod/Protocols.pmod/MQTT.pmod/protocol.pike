#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

protected Stdio.File|SSL.File conn;

protected Stdio.Buffer buffer;
protected Stdio.Buffer outbuf;
protected Pike.Backend backend;
protected Pike.Backend await_backend = Pike.Backend();
protected ADT.Queue ping_timeout_callout_ids = ADT.Queue();

constant NOT_CONNECTED = 0;
constant CONNECTING = 1;
constant CONNECTED = 2;

constant CONNECT_STATES = ([0: "NOT_CONNECTED", 1: "CONNECTING", 2: "CONNECTED"]);

protected int connection_state = NOT_CONNECTED;

protected SSL.Context ssl_context;
protected string client_identifier;
protected int timeout = 10;
protected function(.client:void) connect_cb;
protected function(.client,.Reason:void) disconnect_cb;
protected mixed timeout_callout_id;

protected int(0..2) qos_level = 0;
protected int response_timeout = 30;
protected int publish_response_timeout = 30;
protected int max_retries = 3;

// packet parsing state
protected int packet_started = 0;
protected int have_length = 0;
protected int current_header;
protected int current_length;
protected array(int) length_bytes = ({});

Thread.Mutex await_mutex = Thread.Mutex();

// protocol state
protected int wrote_connect;
protected int message_identifier = 0;

protected mapping(int:.Message) pending_responses = ([]);

//!
int is_connected() { return connection_state == CONNECTED; }

//! sets the QOS level for all subscriptions on this client
void set_qos_level(int(0..2) level) { qos_level = level; }

//! the seconds to wait for a response from the server for acknowledged publish requests
//! 
//! as published messages could be larger and require more time to transmit, this timeout
//! is configurable seprately from the acknowledged request timeout used by subscribe and 
//! unsubscribe messages.
void set_publish_response_timeout(int secs) {
  response_timeout = secs;
}

//! the seconds to wait for a response from the server for acknowledged requests
void set_response_timeout(int secs) {
  response_timeout = secs;
}

//! number of times to retry transmission for requests whose acknowledgements have timed out
void set_retry_attempts(int count) {
  max_retries = count;
}


//!
void set_disconnect_callback(function(.client,.Reason:void) cb) {
	disconnect_cb = cb;
}

//! 
void set_ssl_context(SSL.Context context) {
  ssl_context = context;
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

protected void check_connected() {
	if(connection_state != CONNECTED)
	throw(Error.Generic("No MQTT connection.\n"));
}

//!
void disconnect() {
  check_connected();
  low_disconnect(1);
}

protected void low_disconnect(int _local, mixed|void backtrace) {
  if(conn->is_open()) {
    .DisconnectMessage message = .DisconnectMessage();
    send_message_sync(message);
    conn->close();
  }
  reset_connection(_local, backtrace);
  reset_state();
}

protected int get_message_identifier() {
	return ++message_identifier;
}

void send_message(.Message m) {
   string msg = m->encode();  
   DEBUG("Adding outbound message to queue: %O => %O\n", m, msg);
   outbuf->add(msg);
   
   if(conn->query_application_protocol)
	   conn->write("");
   
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

protected void async_await_response(int message_identifier, .Message message, int response_timeout, 
    int max_retries, function success, function failure, mixed data) {
    register_pending(message_identifier, message, response_timeout, max_retries, success, failure, data);
}

protected .Message send_message_await_response(.Message m, int timeout) {
  .Message r = 0;
  int message_identifier = m->message_identifier;

  if(!message_identifier) throw(Error.Generic("No message identifier specified. Cannot receive a response.\n"));
  int attempts = 0;

  register_pending(message_identifier, m);
  do {
    send_message(m);
    r = await_response(message_identifier, timeout);
    if(r) break;
    m->set_dup_flag();
  }  while(attempts++ < max_retries);

  unregister_pending(message_identifier);

  return r;
}

protected .Message await_response(int message_identifier, int timeout) {
  .Message m = 0;
  Pike.Backend orig;

  float f = (float)timeout;
  
  DEBUG("await_response %d\n", message_identifier);

  if((m = pending_responses[message_identifier])  && m->message)
    return m->message;

// TODO
// there is a theoretical race condition here
// or at least a possible performance gap that could occur if a
// client is waiting and the lock is held while messages are not 
// delivered. we should 
  while(f > 0.0) {
    
    object key = await_mutex->trylock();

    if(!key) {
      key = await_mutex->lock();
      if((m = pending_responses[message_identifier]) && m->message) { 
        key = 0;
        return m->message;
      }
    }

    if(!orig) orig = conn->query_backend();

    // DEBUG("waiting %f seconds for message %d\n", f, message_identifier);
    conn->set_backend(await_backend);
    f = f - await_backend(f);
    key = 0;
    if((m = pending_responses[message_identifier]) && m->message) 
      break;
  }

  conn->set_backend(orig);
  return m?m->message:m; // timeout
}

protected variant void register_pending(int message_identifier, .Message message) {
  .PendingResponse pr = .PendingResponse(this, message_identifier, message, 0, 0);
  pending_responses[message_identifier] = pr;
}

protected variant void register_pending(int message_identifier, .Message message,  int timeout, int max_retries, function success, function failure, mixed data) {
  .PendingResponse pr = .PendingResponse(this, message_identifier, message, timeout, max_retries);
  if(data) pr->data = data;
  if(success) pr->success = success;
  if(failure) pr->failure = failure;
    else pr->failure = report_timeout;
  pending_responses[message_identifier] = pr;
}

void unregister_pending(int message_identifier) {
  if(has_index(pending_responses, message_identifier)) {
    DEBUG("clearing pending response marker for %d\n", message_identifier);
    m_delete(pending_responses, message_identifier);
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
	ping_timeout_callout_ids->put(call_out(ping_timeout, timeout));
	send_message(message);
}

protected void ping_timeout() {
  // no ping response received before timeout.
  DEBUG("No ping response received before timeout, disconnecting.\n");
  disconnect();
}

protected int write_cb(mixed id) {
//  DEBUG("write_cb %O\n", id);
  if(conn->query_application_protocol && sizeof(outbuf))
  {
	  string s = outbuf->read();
	  int tosend = sizeof(s);
	  int tot = 0;
//	  DEBUG("writing %d\n", sizeof(s));
	  while(tot < tosend) {
       int sent = conn->write(s);
	   tot += sent;
	   if(tot < tosend && sent > 0) s = s[sent..];
    }
//	  DEBUG("wrote %d\n", tot);
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
  mixed buf = buffer;

  if(conn->query_application_protocol)
  {
    buf->add(data);
	  data = buf;
  }
  
  if(!packet_started)
  {
     if(!sizeof(buf)) return 0;
     packet_started = 1;
     current_header = buf->read_int8();
 //    werror("header: %O\n", current_header);     
     if(!sizeof(buf)) {// werror("No more data after header byte\n"); 
     return 0; }
  }
  
  if(!have_length) {
     int t = 0;
     int multiplier = 1;
     int value = 0;
     int digit;
     
  //   werror("getting length\n");
     
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
  
  if(have_length)
    DEBUG("Expecting packet length of %d\n", current_length);
  else DEBUG("Didn't get full length from packet. Will wait for more data.");

    int h = current_header;
  
  int len = sizeof(buf);
  int message_type = (h >> 4) & 15;  
  
  if(len && !have_length) { throw(Error.Generic("Didn't calculate length but have data in buffer. Algorithm bug?\n")); }
  
  if(len < current_length) return 0;
  
  string body = buf->read(current_length);

  werror("Data? header: %O, message_type: %O, length: %O body: %O, remaining: %O\n", h, message_type, current_length, body, sizeof(buf));
  reset_state();
  

  program mt = Public.Protocols.MQTT.message_registry[message_type];
  if(!mt){ 
    werror("Bad data? header: %O, length: %O body: %O, remaining: %O\n", h, current_length, body, sizeof(buf));
    throw(Error.Generic("No message type registered for " + message_type + ".\n"));
  }
  .Message message = mt();
  message->decode(h, Stdio.Buffer(body));
  
  process_message(message);
  
  if(sizeof(buf)) {// data left, so queue up another round.
  DEBUG("have %d in buffer\n", sizeof(buf));
    call_out(read_cb, 0, id, "");
    }
    else DEBUG("buffer empty.\n");
}

 void report_timeout(.PendingResponse response) {
   DEBUG("A timed out waiting for response after %d attempts.\n", response->attempts);
 }

protected void process_message(.Message message);

protected void reset_state() {
// reset everything
  packet_started = 0;
  have_length = 0;
  current_header = 0;
  current_length = 0;
  length_bytes = ({});
}

protected void reset_connection(void|int _local, mixed|void backtrace) {
    connection_state = NOT_CONNECTED;
    if(timeout_callout_id)
      remove_call_out(timeout_callout_id);
    if(disconnect_cb)
	    disconnect_cb(this, .Reason(_local, backtrace));
}

protected void report_error(mixed error) {
  werror(master()->describe_backtrace(error));
}