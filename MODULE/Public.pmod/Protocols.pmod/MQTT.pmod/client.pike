#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

inherit .protocol;

protected string host;
protected int port;
protected string username;
protected string password;

protected Standards.URI connect_url;

protected function(.client:void) connect_cb;
protected function(.client,.Reason:void) disconnect_cb;
mapping(string:multiset) publish_callbacks = ([]);
mapping(.Matcher:multiset) publish_callback_matchers = ([]);

//! MQTT client

//! create a client which will connect to an mqtt server on the specified server and port.
protected variant void create(string _host, int _port) {
   create("mqtt://" + host + ":" + port);
}

//! create a client 
//!
//! @param _connect_url
//!   A url in the form of  @tt{mqtt://[user[:password]@@][hostname][:port]@} or  @tt{mqtts://[user[:password]@@][hostname][:port]@}
//!
protected variant void create(string _connect_url) {
    connect_url = Standards.URI(_connect_url);
	if(!(<"mqtt", "mqtts">)[connect_url->scheme]) throw(Error.Generic("Connect url must be of type mqtt or mqtts.\n"));
	
	host = connect_url->host;
	port = connect_url->port;
	username = connect_url->user;
	password = connect_url->password;
	
	if(!port) {
		if(connect_url->scheme == "mqtts") port = MQTTS_PORT;
		else port = MQTT_PORT;
	}
	
	backend = Pike.DefaultBackend;
}

//! specify the will and testament message that will be sent by the server when
//! this client disconnects.
void set_will_and_testament(string topic, string(7bit) message, int(0..2) qos) {
  if(is_connected()) throw(Error.Generic("will and testament must be provided before connecting.\n"));
  if(!(topic && message)) throw(Error.Generic("Both will topic and message must be provided.\n"));
   will_topic = topic;
   will_message = message;
   will_qos = qos;
}

//! connect and specify a method to be called when the connection successfully completes.
variant void connect(function(.client:void) _connect_cb) {
	connect_cb = _connect_cb;
	connect();
}

//! connect to the server.
//!
//! @note
//!  this method may return before the connection has succeeded or failed. 
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
   if(will_topic) {
     m->will_flag = 1;
     m->will_topic = will_topic;
     m->will_message = will_message;
   }
   
   send_message(m);
}

//! specify a callback to be run when a client is disconnected.
void set_disconnect_callback(function(.client,.Reason:void) cb) {
	disconnect_cb = cb;
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

//! subscribe to a topic at the qos level specified by @[set_qos_level]().
//!
//! @param publish_cb
//!  a method which will be called for each message received. parameters include
//!  the MQTT client, the topic and the message body. 
//!
//! @note
//!  multiple subscriptions to a given topic may be made by calling this method
//!  repeatedly with different callback methods.
void subscribe(string topic, function(.client,string,string:void) publish_cb) {
    check_connected();  
  
    .SubscribeMessage message = .SubscribeMessage();
    message->message_identifier = get_message_identifier();
    message->topics += ({ ({topic, qos_level}) });
    
    if(search(topic, "#") != -1) { // multi level wildcard
      .Matcher matcher;
      if(topic == "#") {
         matcher = make_matcher(.MatchAllWithoutDollar, topic);
      } else {
         matcher = make_matcher(.MatchWithPrefix, topic);
      } 
      
      if(!publish_callback_matchers[matcher])                                                                                        
        publish_callback_matchers[matcher] = (<>);                                                                                                  
     
DEBUG("adding matcher %O for %s\n", matcher, topic);                                                                                  
      publish_callback_matchers[matcher] += (<publish_cb>);  
    } else if(search(topic, "+") != -1) { // single level wildcard
    } else {
      if(!publish_callbacks[topic])
  	  publish_callbacks[topic] = (<>);

      publish_callbacks[topic] += (<publish_cb>);		  
    }
    .Message response = send_message_await_response(message, response_timeout);
    if(!response) throw(Error.Generic("Subscribe was not acknowledged.\n"));

}

//! unsubscribe the specified publish callback from a topic. If all callbacks
//! registered for a topic have been removed, the client will unsubscribe from the topic 
void unsubscribe(string topic, function(.client,string,string:void) publish_cb) {
    check_connected();  
    if(has_wildcard(topic)) {
      foreach(publish_callback_matchers; .Matcher m; multiset cbs) {
         if(m->topic == topic) {
          if(cbs[publish_cb]) cbs[publish_cb] = 0;
 if(!sizeof(cbs)) {                                           
            .UnsubscribeMessage message = .UnsubscribeMessage();   
            message->message_identifier = get_message_identifier();             
            message->topics += ({m->topic});                                              
            .Message response = send_message_await_response(message, response_timeout);                        
            if(!response) throw(Error.Generic("Unsubscribe was not acknowledged.\n"));
                m_delete(publish_callback_matchers, m); 
          }
        }
      }
    } else {
    multiset cbs = publish_callbacks[topic];
    if(cbs) {
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
  }
}

protected int(0..1) has_wildcard(string topic) {
  return (search(topic, "#") != -1 || search(topic, "+") != -1);
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

//! method used internally by the MQTT client
void send_message(.Message m) {
   ::send_message(m);
   if(timeout_callout_id) remove_call_out(timeout_callout_id);
   timeout_callout_id = call_out(send_ping, (timeout > 1? timeout - 1: 0.5));
}

protected void send_message_sync(.Message m) {
   ::send_message_sync(m);
   if(timeout_callout_id) remove_call_out(timeout_callout_id);
   timeout_callout_id = call_out(send_ping, (timeout > 1? timeout - 1: 0.5));
}

protected void process_message(.Message message) {
  DEBUG("got message: %O\n", message);
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
		  switch(qos) {
		    case .QOS_AT_MOST_ONCE:
if((cbs = publish_callbacks[message->topic])) {
    		  foreach(cbs; function callback;) {
    		    DEBUG("Scheduling delivery of message from %s to %O\n", message->topic, callback);
		  	    call_out(callback, 0, this, message->topic, message->body);
		  	  }
}
// delivery to wildcards                                                                                                                          
            foreach(publish_callback_matchers; .Matcher m; multiset cbs) {                                                                      
              if(m->match(message->topic))                                                                                               
                foreach(cbs; function callback;) {                                                                                             
                    DEBUG("Scheduling delivery of wildcard matched message from %s to %O\n", message->topic, callback);                        
                    call_out(callback, 0, this, message->topic, message->body);                                                                
                }                                                                                                                                             
            } 
		      break;
		    case .QOS_AT_LEAST_ONCE:
	        int have_errors = 0;
if((cbs = publish_callbacks[message->topic])) { 
		foreach(cbs; function callback;) {
    		  DEBUG("Performing delivery of message from %s to %O\n", message->topic, callback);
		  mixed e = catch(callback(this, message->topic, message->body));
		  if(e) { 
                    have_errors++; 
                    report_error(e);
	          }
	       }
}
// delivery to wildcards                                                                                                                          
            foreach(publish_callback_matchers; .Matcher m; multiset cbs) {                                                                      
              if(m->match(message->topic))                                                                                               
                foreach(cbs; function callback;) {                                                                                             
                    DEBUG("Performing delivery of wildcard matched message from %s to %O\n", message->topic, callback);                        
                    mixed e = catch(callback(this, message->topic, message->body));
                    if(e) {
                     have_errors++;
                     report_error(e);
                    }                                    
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
            // delivery to wildcards
            foreach(publish_callback_matchers; .Matcher m; multiset cbs) {
              if(m->match(message->topic))
                foreach(cbs; function callback;) {
                    DEBUG("Scheduling delivery of wildcard matched message from %s to %O\n", message->topic, callback);                                                        
                    call_out(callback, 0, this, message->topic, message->body); 
                }
            }
          }
}

protected void publish_rel_timeout_cb(.PendingResponse r) { 
          throw(Error.Generic(sprintf("Publish message %d (QOS=AT_EXACTLY_ONCE, phase 2) was not acknowledged by server.\n", r->original_message->message_identifier)));
            // TODO failure callback
            if(r->data->failure) r->data->failure(r->original_message);
}
  
  protected void reset_connection(void|int _local, mixed|void backtrace) {
    ::reset_connection();
    if(disconnect_cb)
	    disconnect_cb(this, .Reason(_local, backtrace));
}

mapping(string:.Matcher) matchers = ([]);

.Matcher make_matcher(program(.Matcher) prog, string topic) {
  .Matcher m;
  if(m = matchers[topic])
    return m;
   else matchers[topic] = (m = prog(topic));
   return m;
}

protected string _sprintf(mixed t) {
	  return "MQTT.client(" + (string)connect_url + "=>" + CONNECT_STATES[connection_state] + ")";
}
