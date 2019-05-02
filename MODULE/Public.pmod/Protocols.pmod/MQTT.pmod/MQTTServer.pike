#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT.MQTTServer: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

protected int bind_port;
protected string bind_address;

protected mapping clients = ([]);
protected mapping subscriptions = ([]);

protected int started;
protected Stdio.Port port; 
protected function(void:Stdio.Port)|program(Stdio.Port) port_program = Stdio.Port;
protected program(.ServerConnection) connection_program = .ServerConnection;
protected server_delegate delegate; 
protected variant void create() {
  create(.protocol.MQTT_PORT);
}

protected mapping(.Matcher:mapping) subscription_matchers = ([]);

//!
protected variant void create(int port) {
  bind_port = port;
  create_delegate();
}

//!
protected variant void create(int port, string address) {
  bind_port = port;
  bind_address = address;
  create_delegate();
}

protected void create_delegate() {
  delegate = server_delegate();
  delegate->authenticate = authenticate;
  delegate->new_session = new_session;
  delegate->subscribe = subscribe;
  delegate->unsubscribe = unsubscribe;
  delegate->client_disconnected = client_disconnected;
  delegate->publish = publish;
}

//! specify a port program to be used when starting the server.
void set_port_program(function(void:Stdio.Port)|program(Stdio.Port) port_factory) {
  if(started) throw(Error.Generic("Cannot specify port object: MQTTServer is already started.\n"));
  port_program = port_factory;
}

//! bind to the specified port and enable the server
void start() {
  if(started) throw(Error.Generic("MQTTServer is already started.\n"));
  started = 1;
  if(!port)
    port = port_program();
  port->bind(bind_port, accept_cb, bind_address);
}

//! specify the server connection program to be used when accepting new connections.
void set_connection_program(program(.ServerConnection) conn_program) {
  connection_program = conn_program;
}

protected void accept_cb(mixed id) {
  DEBUG("Accepting connection\n");
  Stdio.File conn = port->accept();
  .ServerConnection connection = connection_program(conn, delegate);  
  DEBUG("Done\n");
}

protected class server_delegate {
  function(string, string: int) authenticate;
  function(object, string: int) new_session;
  function(object, string, int: int) subscribe;
  function(object, string: void) unsubscribe;
  function(object: void) client_disconnected;
  function(object, string, string, int: int) publish;
};

protected int authenticate(string username, string password) {
  DEBUG("SERVER: authenticate(%O, %O)\n", username, password);
  return 1;
}

protected int new_session(object session, string identifier) {
  DEBUG("SERVER: new_session(%O, %O)\n", session, identifier);
  object o = clients[identifier];
  if(o) o->disconnect(); // only one connection per client identifier, please.
  clients[identifier] = session;
  return 1;
}

protected int subscribe(object session, string topic, int qos) {
  DEBUG("SERVER: subscribe(%O, %O, %O)\n", session, topic, qos);
  
  
    if(search(topic, "#") != -1) { // multi level wildcard
      .Matcher matcher;
      if(topic == "#") {
         matcher = make_matcher(.MatchAllWithoutDollar, topic);
      } else {
         matcher = make_matcher(.MatchWithPrefix, topic);
      } 
      
      if(!subscription_matchers[matcher])                                                                                        
        subscription_matchers[matcher] = ([]);                                                                                                  
     
DEBUG("adding matcher %O for %s\n", matcher, topic);                                                                                  
      subscription_matchers[matcher][session->get_client_identifier()] = qos;
    } else if(search(topic, "+") != -1) { // single level wildcard
        .Matcher matcher;
        matcher = make_matcher(.SingleLevelMatcher, topic);
      
        if(!subscription_matchers[matcher])                                                                                        
          subscription_matchers[matcher] = ([]);  
		  
	      subscription_matchers[matcher][session->get_client_identifier()] = qos;
    } else {
	    if(!subscriptions[topic]) subscriptions[topic] = ([]);
	    subscriptions[topic][session->get_client_identifier()] = qos;		  
    }
  
  return 1;
}

protected void unsubscribe(object session, string topic) {
  DEBUG("SERVER: unsubscribe(%O, %O)\n", session, topic);

  if(has_wildcard(topic)) {
    foreach(subscription_matchers; .Matcher m; mapping cbs) {
       if(m->topic == topic) {
        if(cbs[session->get_client_identifier()]) m_delete(cbs, session->get_client_identifier());

  	  mapping s = subscriptions[topic];
  		if(!s) return;
  	  string ci = session->get_client_identifier();
  
 	 if(s[ci]) m_delete(s, ci);
  
  	if(!sizeof(s)) m_delete(subscriptions, topic);
  
  	return;
  	}
	}
	}
}  

protected void client_disconnected(object session) {
  DEBUG("SERVER: client_disconnected(%O)\n", session);
  foreach(subscriptions; string topic;)
    unsubscribe(session, topic);

  string will_topic = session->get_will_topic();
  
  if(will_topic) {
    publish(session, will_topic, session->get_will_message(), session->get_will_qos());
  }
}

protected int publish(object session, string topic, string message, int qos) {
  DEBUG("SERVER: publish(%O, %O, %O, %O)\n", session, topic, message, qos);
  int i;
  mapping destinations = subscriptions[topic];
  if(destinations)
  foreach(destinations; string identifier; int cqos) {
    object s = clients[identifier];
    if(!s) continue;
	i++;
    s->publish(topic, message, qos&cqos);
  }
  
  foreach(subscription_matchers; .Matcher m; mapping cbs) {
      if(m->match(topic))                                                                                               
        foreach(cbs; mixed identifier; int cqos) {                                                                                             
            DEBUG("Performing delivery of wildcard matched message from %s to %O\n", message->topic, identifier);                        
            
			object s = clients[identifier];
			if(!s) continue;
			i++;
			s->publish(topic, message, qos&cqos);
        }                                     
  }
  
  return !i;
}

mapping(string:.Matcher) matchers = ([]);

.Matcher make_matcher(program(.Matcher) prog, string topic) {
  .Matcher m;
  if(m = matchers[topic])
    return m;
   else matchers[topic] = (m = prog(topic));
   return m;
}


protected int(0..1) has_wildcard(string topic) {
  return (search(topic, "#") != -1 || search(topic, "+") != -1);
}
