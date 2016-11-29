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

constant MQTT_PORT = 1883;
constant MQTTS_PORT = 8883;

protected string will_topic;
protected string will_message;
protected int will_qos;

protected int connection_state = NOT_CONNECTED;

protected SSL.Context ssl_context;
protected string client_identifier;
protected int timeout = 10;

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
void set_ssl_context(SSL.Context context) {
  ssl_context = context;
}

//!
string get_will_topic() {
 return will_topic;
}

//!
string(7bit) get_will_message() {
  return will_message;
}  

//!
int(0..2) get_will_qos() {
  return will_qos;
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
  //check_connected();
  low_disconnect(1);
}

//! publish a message and return immediately. 
//!
//! @note
//!   all I/O performed by this message happens in the backend, so any code that calls 
//!   this method must return to the backend before any work is done.
void publish(string topic, string msg, int|void qos_level, function failure_cb) {
  check_connected();  
  
  .PublishMessage message = .PublishMessage();
  message->topic = topic;
  message->body = msg;
  message->set_qos_level(qos_level);
  
  switch(qos_level) {
    case .QOS_AT_MOST_ONCE:
      send_message(message);
      break;
    case .QOS_AT_LEAST_ONCE:
      message->message_identifier = get_message_identifier();
      
      send_message(message);
      async_await_response(message->message_identifier, message, publish_response_timeout, max_retries,
        publish_ack_cb, publish_ack_timeout_cb, (["failure": failure_cb]));
      
      break;
    case .QOS_EXACTLY_ONCE:  
      mapping data = (["failure": failure_cb]); // for storing callbacks later.
      message->message_identifier = get_message_identifier();
      send_message(message);
      async_await_response(message->message_identifier, message, publish_response_timeout, max_retries, 
        publish_rec_cb, publish_rec_timeout_cb, data);
      break;
    default:
      throw(Error.Generic("Unsupported QOS Level: " + qos_level + "\n"));
  }
}

protected void publish_ack_cb(.PendingResponse r) { 
          DEBUG("Got response for publish of message id %d\n", r->original_message->message_identifier);
          if(r->data->success) r->data->success(r->original_message);
}

protected void publish_ack_timeout_cb(.PendingResponse r) { 
          throw(Error.Generic(sprintf("Publish message %d (QOS=AT_LEAST_ONCE) was not acknowledged by server.\n", r->original_message->message_identifier)));
          if(r->data->failure) r->data->failure(r->original_message);
}

protected void publish_rec_cb(.PendingResponse r) { 
          DEBUG("Got response for publish (phase 1) of message id %d\n", r->original_message->message_identifier);
          if(object_program(r->message) != .PubRecMessage) {
            DEBUG("Got invalid response for publish (phase 1) of message id %d\n", r->original_message->message_identifier);
            // TODO failure callback
            if(r->data->failure) r->data->failure(r->original_message);
          }
          else {
            .Message message2 = .PubRelMessage();
            message2->message_identifier = r->message_identifier;
            send_message(message2);
            async_await_response(message2->message_identifier, message2, publish_response_timeout, max_retries,
              publish_comp_cb, publish_comp_timeout_cb, (["failure": r->data->failure, "success": r->data->success, "original_message": r->original_message]));
          }
}

protected void publish_rec_timeout_cb(.PendingResponse r) { 
          throw(Error.Generic(sprintf("Publish message %d (QOS=AT_EXACTLY_ONCE, phase 1) was not acknowledged by server.\n", r->original_message->message_identifier)));
            // TODO failure callback
            if(r->data->failure) r->data->failure(r->original_message);
}

protected void publish_comp_cb(.PendingResponse r) { 
          DEBUG("Got response for publish (phase 2) of message id %d\n", r->original_message->message_identifier);
          if(object_program(r->message) != .PubCompMessage) {
            DEBUG("Got invalid response for publish (phase 2) of message id %d\n", r->original_message->message_identifier);
            // TODO failure callback
            if(r->data->failure) r->data->failure(r->data->original_message);
          }
          else {
            DEBUG("Got response for publish of message id %d\n", r->original_message->message_identifier);
            if(r->data->success) r->data->success(r->data->original_message);
          }
}

protected void publish_comp_timeout_cb(.PendingResponse r) { 
          throw(Error.Generic(sprintf("Publish message %d (QOS=AT_EXACTLY_ONCE, phase 2) was not acknowledged by server.\n", r->original_message->message_identifier)));
            // TODO failure callback
            if(r->data->failure) r->data->failure(r->data->original_message);
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
}

protected void send_message_sync(.Message m) {
   string msg = m->encode();  
   DEBUG("Sending outbound message synchronously: %O => %O\n", m, msg);
   conn->set_blocking_keep_callbacks();
   conn->write(msg);
   conn->set_nonblocking_keep_callbacks();
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

  DEBUG("Data? header: %O, message_type: %O, length: %O body: %O, remaining: %O\n", h, message_type, current_length, body, sizeof(buf));
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

void acknowledge_pub(.Message message) {
   .Message response = .PubAckMessage();
   response->message_identifier = message->message_identifier;
   send_message(response);
}

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
}

protected void report_error(mixed error) {
  werror(master()->describe_backtrace(error));
}

protected void destroy() {
  foreach(pending_responses; int key; object pending_response) {
    m_delete(pending_responses, key);
    destruct(pending_response);
  }
}