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

protected Standards.URI connect_url;
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

mapping(string:multiset) publish_callbacks = ([]);

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

//! MQTT client
//!
//! Limitations:
//! 
//! error checking is less than ideal


//!
protected variant void create(string _host, int _port) {
   create("mqtt://" + host + ":" + port);
}

//! create a client 
//!
//! @param _connect_url
//!   A url in the form of mqtt://username:password@server:port or mqtts://username:password@server:port
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
       conn = SSL.File(conn, ssl_context || SSL.Context());
	   conn->set_blocking();
	   if(!conn->connect(host))
	     throw(Error.Generic("Unable to start TLS session with MQTT server.\n"));
	   //conn->write("");
   }

   buffer = Stdio.Buffer();
   outbuf = Stdio.Buffer();
   if(connect_url->scheme != "mqtts")
     conn->set_buffer_mode(buffer, outbuf);
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


//! publish a message synchronously and if QOS level requires a response, wait before returning.
//! 
//! @note
//!   synchronous publishing is far slower than asynchnronous publishing using @[publish]. 
void publish_sync(string topic, string msg, int|void qos_level) {
  check_connected();  
  
  .PublishMessage message = .PublishMessage();
  message->topic = topic;
  message->body = msg;
  message->set_qos_level(qos_level);
  
  switch(qos_level) {
    case .QOS_AT_MOST_ONCE:
      send_message_sync(message);
      break;
    case .QOS_AT_LEAST_ONCE:
      message->message_identifier = get_message_identifier();
      .Message response = send_message_await_response(message, publish_response_timeout);
      if(!response || object_program(response) != .PubAckMessage)
        throw(Error.Generic("Publish message (QOS=AT_LEAST_ONCE) was not acknowledged by server.\n"));
      else { DEBUG("Got response for publish of message id %d\n", message->message_identifier); }
      break;
    case .QOS_EXACTLY_ONCE:  
      message->message_identifier = get_message_identifier();
      response = send_message_await_response(message, publish_response_timeout);
      if(!response || object_program(response) != .PubRecMessage)
        throw(Error.Generic("Publish message (QOS=EXACTLY_ONCE) was not acknowledged by server (phase 1).\n"));
      .Message message2 = .PubRelMessage();
      message2->message_identifier = response->message_identifier;
      .Message response2 = send_message_await_response(message2, publish_response_timeout);
      if(!response2 || object_program(response2) != .PubCompMessage)
        throw(Error.Generic("Publish message (QOS=EXACTLY_ONCE) was not acknowledged by server (phase 2).\n"));
      break;
    default:
      throw(Error.Generic("Unsupported QOS Level: " + qos_level + "\n"));
  }
}

//!
void subscribe(string topic, function(.client,string,string:void) publish_cb) {
    check_connected();  
  
    .SubscribeMessage message = .SubscribeMessage();
    message->message_identifier = get_message_identifier();
    message->topics += ({ ({topic, qos_level}) });

    if(!publish_callbacks[topic])
  	  publish_callbacks[topic] = (<>);

    publish_callbacks[topic] += (<publish_cb>);		  
    .Message response = send_message_await_response(message, response_timeout);
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
	    message->message_identifier = get_message_identifier();
	    message->topics += ({topic});
	    .Message response = send_message_await_response(message, response_timeout);
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

void send_message(.Message m) {
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
int d = _get_digit(buf, digit);
  //werror("digit: %O\n", d);
  return d;
}
protected int _get_digit(Stdio.Buffer buf, int digit) {
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

protected int write_cb(mixed id) {
//  DEBUG("write_cb %O\n", id);
  if(connect_url->scheme == "mqtts" && sizeof(outbuf))
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

  if(connect_url->scheme == "mqtts")
  {
    buf->add(data);
	  data = buf;
  }
  
  if(!packet_started)
  {
     if(!sizeof(buf)) return;
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

protected void process_message(.Message message) {
  werror("got message: %O\n", message);
  int message_identifier = message->message_identifier;

  
  if(object_program(message) == .PingResponseMessage) {
    // TODO what if we have multiple outstanding ping responses? Should we keep a stack of timeouts?
    DEBUG("Got PingResponse\n");
    if(sizeof(ping_timeout_callout_ids)) {
      DEBUG("removing ping timeout callout\n");
      remove_call_out(ping_timeout_callout_ids->get());
    }
  }
  
  else if(object_program(message) == .ConnAckMessage) {
    if(connection_state != CONNECTING) 
      throw(Error.Generic("Received CONNACK at invalid point, disconnecting.\n"));
    if(message->response_code != 0) {
      low_disconnect(1);
      throw(Error.Generic(sprintf("Server reports connection failed with code %d: %s.\n", message->response_code, message->response_text)));
    }
    connection_state = CONNECTED;
    DEBUG("%O got connect response code: %O\n", this, message->response_code);
  	if(connect_cb) connect_cb(this);
  }
  
  else if(object_program(message) == .PublishMessage) {
	  multiset cbs; 
	  
    int qos = message->get_qos_level();
	  DEBUG("PublishMessage topic: %s (qos=%d), %O\n", message->topic, qos, message->body);
	  if((cbs = publish_callbacks[message->topic])) {
		  switch(qos) {
		    case .QOS_AT_MOST_ONCE:
    		  foreach(cbs; function callback;) {
    		    DEBUG("Scheduling delivery of message from %s to %O\n", message->topic, callback);
		  	    call_out(callback, 0, this, message->topic, message->body);
		  	  }
		      break;
		    case .QOS_AT_LEAST_ONCE:
	        int have_errors = 0;
				  foreach(cbs; function callback;) {
    		    DEBUG("Performing delivery of message from %s to %O\n", message->topic, callback);
		          mixed e = catch(callback(this, message->topic, message->body));
		        if(e) { 
		          have_errors++; 
		          report_error(e);
		        }
		      }
		      if(!have_errors) acknowledge_pub(message);
		      else
		      {
		        werror("One or more callbacks to an AT_LEAST_ONCE publish message failed. Not acknowledging, duplicates may occur.\n");
		      }
	        break;
	      case .QOS_EXACTLY_ONCE:
          int message_identifier = message->message_identifier;
          .Message message2 = .PubRecMessage();
          message2->message_identifier = message_identifier;
          send_message(message2);
          mapping data = (["message": message]); // for storing callbacks later.
          async_await_response(message2->message_identifier, message2, publish_response_timeout, max_retries, 
            publish_rel_cb, publish_rel_timeout_cb, data);
	    }
	  }
	  else {
   	    DEBUG("WARNING: got publish message for something we have no record of subscribing to: " + message->topic + "\n");
	  }
  }
  else if(message_identifier) {
    DEBUG("Got a message with a message_identifier: %d %O\n", message_identifier, message);
    if(has_index(pending_responses, message_identifier)) {
      object pending_response = pending_responses[message_identifier];
      pending_response->received_message(message);
    } else  {
      DEBUG("Not waiting for an answer for id=%d\n", message_identifier);
      if(object_program(message) == .PubRelMessage) {
        // could be a resend. since we got here, we have already processed it, so we just need to keep replying back.
        .Message reply = .PubCompMessage();
        reply->message_identifier = message_identifier;
        send_message(reply);
      }
      else if(object_program(message) == .SubscribeAckMessage) {
        DEBUG("%O unhandled subscribe ack with response code: %O\n", this, message->response_code);
      }
      else if(object_program(message) == .UnsubscribeAckMessage) {
        DEBUG("%O unhandled unsubscribe ack response\n", this);
      }
    }
  }
  else {
     DEBUG("%O unhandled message %O\n", this, message);
  }

}

protected void publish_rel_cb(.PendingResponse r) { 
          DEBUG("Got response for publish (phase 2) of message id %d: %O\n", r->original_message->message_identifier, r->message);
          if(object_program(r->message) != .PubRelMessage) {
            DEBUG("Got invalid response for publish (phase 2) of message id %d\n", r->original_message->message_identifier);
            // TODO failure callback
            if(r->data->failure) r->data->failure(r->original_message);
          }
          else {
            .Message message2 = .PubCompMessage();
            message2->message_identifier = r->message_identifier;
            send_message(message2);
            .Message message = r->data->message;
            mixed cbs = publish_callbacks[message->topic];
            foreach(cbs; function callback;) {
      		    DEBUG("Scheduling delivery of message from %s to %O\n", message->topic, callback);
	  	  	    call_out(callback, 0, this, message->topic, message->body);
  		  	  }

          }
}

protected void publish_rel_timeout_cb(.PendingResponse r) { 
          throw(Error.Generic(sprintf("Publish message %d (QOS=AT_EXACTLY_ONCE, phase 2) was not acknowledged by server.\n", r->original_message->message_identifier)));
            // TODO failure callback
            if(r->data->failure) r->data->failure(r->original_message);
}

protected void report_error(mixed error) {
  werror(master()->describe_backtrace(error));
}

void acknowledge_pub(.Message message) {
   .Message response = .PubAckMessage();
   response->message_identifier = message->message_identifier;
   send_message(response);
}
  
protected string _sprintf(mixed t) {
	  return "MQTT.client(" + (string)connect_url + "=>" + CONNECT_STATES[connection_state] + ")";
}