inherit .protocol;

#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

object server;

mixed packet_timeout_callout_id;

protected void create (Stdio.File|SSL.File connection, object servero) {
  backend = Pike.DefaultBackend;
  conn = connection;
  server = servero;
  connection_state = CONNECTING;
  timeout = 5; // default timeout
  conn->set_blocking();
   DEBUG("connection from %s.\n", conn->query_address());

   buffer = Stdio.Buffer();
   outbuf = Stdio.Buffer();
   if(conn->set_buffer_mode) 
     conn->set_buffer_mode(buffer, outbuf);
   conn->set_write_callback(write_cb);
   conn->set_close_callback(close_cb);
   conn->set_read_callback(read_cb);
   conn->set_nonblocking_keep_callbacks();

    reset_packet_timeout();
}

protected void reset_packet_timeout() {
  DEBUG("setting packet timeout to %d seconds.\n", timeout);
  if(packet_timeout_callout_id) remove_call_out(packet_timeout_callout_id);
  if(timeout >= 0) // timeout of zero means no disconnect.
  	packet_timeout_callout_id = call_out(packet_timeout, (int)(timeout * 1.5)); // technically we should round up, but we are lazy.
}

protected void packet_timeout() {
  DEBUG("packet timeout reached. disconnecting.\n");
  disconnect();
}

protected void process_message(.Message message) {
  DEBUG("Server connection from %s got message %O\n", conn->query_address(), message);
    reset_packet_timeout();
    
    if(connection_state == CONNECTING) { // only ConnectMessage is valid.
    if(object_program(message) == .ConnectMessage) {

      .Message m = .ConnAckMessage();
      client_identifier = message->client_identifier;
      timeout = message->keep_alive;
      
      if(message->protocol_version != 3)
      {
        m->response_code = 1;
        send_message_sync(m);
        disconnect();
        return 0;
      }
      if(timeout > (3600 * 18))
      {
        DEBUG("Client requests timeout of %d seconds. Unacceptable!\n", timeout);
        m->response_code = 3; // no real response code suitable so we fudge it.
        send_message_sync(m);
        disconnect();
        return 0;
      }
      
      if(message->has_username) {
        if(!server->authenticate(message->username, message->has_password?message->password:0)) {
          m->response_code = 4;
          send_message_sync(m);
          disconnect();
          return 0;
        }
      }
      
      if(message->will_flag) {
        will_topic = message->will_topic;
        will_message = message->will_message;
      }
      
      if(!message->client_identifier) {
        m->response_code = 2;
        send_message_sync(m);
        disconnect();
        return 0;
      }
      
      if(!server->new_session(this, client_identifier)) {
        m->response_code = 3;
        send_message(m);
        disconnect();
        return 0;
      }

      send_message(m);
      connection_state = CONNECTED;
      return 0;
    }
    else {
      DEBUG("Got unexpected message %O in connection state %d.\n", message, connection_state);
      disconnect();
      return;
    }
  }
  else if(connection_state == CONNECTED) {
    if(object_program(message) == .PingMessage) {
      .Message m = .PingResponseMessage();
      send_message(m);
      return 0;
    }
    else if(object_program(message) == .SubscribeMessage) {
      int max_qos;
      foreach(message->topics;; array topic) {
        DEBUG("Subscription to %s requested with QOS %d\n", topic[0], topic[1]);
        if(topic[1] > max_qos) max_qos = topic[1];
        // failed subscribe
        if(!server->subscribe(this, topic[0], topic[1])) max_qos = 0x80;
      }
      .Message m = .SubscribeAckMessage();
      m->message_identifier = message->message_identifier;
      m->response_code = max_qos;
      send_message(m);
      return 0;
    }
    else if(object_program(message) == .UnsubscribeMessage) {
      foreach(message->topics;; array topic) {
        DEBUG("Unsubscription to %s requested with QOS %d\n", topic[0], topic[1]);
        server->unsubscribe(this, topic);
      }
      .Message m = .UnsubscribeAckMessage();
      m->message_identifier = message->message_identifier;
      send_message(m);
      return 0;
    }
    else if(object_program(message) == .DisconnectMessage) {
      disconnect();
      return 0;
    }
    else if(object_program(message) == .PublishMessage) {
      multiset cbs; 
	  
    int qos = message->get_qos_level();
	  DEBUG("PublishMessage topic: %s (qos=%d), %O\n", message->topic, qos, message->body);
	  switch(qos) {
		  case .QOS_AT_MOST_ONCE:
    	  foreach(cbs; function callback;) {
    	  DEBUG("Scheduling delivery of message from %s to %O\n", message->topic, callback);
		    call_out(server->publish, 0, this, message->topic, message->body, qos);
		  }
		  break;
		  case .QOS_AT_LEAST_ONCE:
	      int have_errors = 0;
		    if(server->publish(this, message->topic, message->body, qos))
		      acknowledge_pub(message);
		    else
	      {
	        werror("Publish of an AT_LEAST_ONCE message failed. Not acknowledging, duplicates may occur.\n");
	      }
        break;
      case .QOS_EXACTLY_ONCE:
        int message_identifier = message->message_identifier;
        .Message message2 = .PubRecMessage();
        DEBUG("sending PubRec for message identifier %d\n", message_identifier);
        message2->message_identifier = message_identifier;
        send_message(message2);
        mapping data = (["message": message]); // for storing callbacks later.
        async_await_response(message2->message_identifier, message2, publish_response_timeout, max_retries, 
          publish_rel_cb, publish_rel_timeout_cb, data);
        break;  
	    }
	    
	    return;
    }

    if(message->message_identifier) {
      int message_identifier = message->message_identifier;
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
      }
      return;
    }
    else {
      DEBUG("Got an unexpected message %O in connection state %d.\n", message, connection_state);
      disconnect();
    }
  }
  else {
    DEBUG("Got unexpected message %O in connection state %d.\n", message, connection_state);
    disconnect();
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
    DEBUG("Scheduling delivery of message from %s to %O\n", message->topic, callback);
    call_out(server->publish, 0, this, message->topic, message->body, message->qos_level);
  }
}

protected void publish_rel_timeout_cb(.PendingResponse r) { 
  throw(Error.Generic(sprintf("Publish message %d (QOS=AT_EXACTLY_ONCE, phase 2) was not acknowledged by client.\n", r->original_message->message_identifier)));
  // TODO failure callback
  if(r->data->failure) r->data->failure(r->original_message);
}

void disconnect() {
  //throw(Error.Generic("Disconnect called\n"));
  server->client_disconnected(this);
  conn->close();
 :: disconnect();
 destruct(this);
}