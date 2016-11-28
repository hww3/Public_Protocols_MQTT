inherit .protocol;

#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

object server;

mixed packet_timeout_callout_id;

protected void create (Stdio.File|SSL.File connection, object server) {
  backend = Pike.DefaultBackend;
  conn = connection;
  server = server;
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
	packet_timeout_callout_id = call_out(packet_timeout, timeout);
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
     
      // TODO check keepalive for validity.
     
      
      if(message->protocol_version != 3)
      {
        m->response_code = 1;
        send_message_sync(m);
        disconnect();
        return 0;
      }
      if(timeout > 600)
      {
        DEBUG("Client requests timeout of %d seconds. Unacceptable!\n", timeout);
        m->response_code = 3; // no real response code suitable so we fudge it.
        send_message_sync(m);
        disconnect();
        return 0;
      }
      
      if(message->has_username) {
        /*
        server->authenticate(message->username, message->has_password?message->password:0);
        m->response_code = 4;
        send_message_sync(m);
        disconnect();
        return 0;
        */
      }
      
      if(!message->client_identifier) {
        m->response_code = 2;
        send_message_sync(m);
        disconnect();
        return 0;
      }
      
      m->response_code = 0;
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
        // if(!server->subscribe(topic[0], topic[1])) max_qos = 0x80;
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
        // server->unsubscribe(topic);
      }
      .Message m = .UnsubscribeAckMessage();
      m->message_identifier = message->message_identifier;
      send_message(m);
      return 0;
    }
    
    DEBUG("Got unexpected message %O in connection state %d.\n", message, connection_state);
    disconnect();
  }
  else {
    DEBUG("Got unexpected message %O in connection state %d.\n", message, connection_state);
    disconnect();
  }
}

void disconnect() {
  // server->client_disconnected(this);
  conn->close();
 :: disconnect();
 destruct(this);
}