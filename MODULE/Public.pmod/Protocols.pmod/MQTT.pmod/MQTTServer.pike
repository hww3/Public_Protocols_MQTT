#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT.MQTTServer: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

protected int bind_port;
protected string bind_address;

protected mapping clients = ([]);
protected mapping subscriptions = ([]);

protected Stdio.Port port; 
protected program(.ServerConnection) connection_program = .ServerConnection;
protected server_delegate delegate; 
protected variant void create() {
  create(.protocol.MQTT_PORT);
}

protected variant void create(int port) {
  bind_port = port;
  create_delegate();
}

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

//!
void set_port_object(Stdio.Port port_object) {
  port = port_object;
}

//!
void start() {
  if(!port)
    port = Stdio.Port();
  port->bind(bind_port, accept_cb, bind_address);
}

//!
Stdio.Port get_port() {
  return port;
}

//!
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
  if(!subscriptions[topic]) subscriptions[topic] = ([]);
  subscriptions[topic][session->get_client_identifier()] = qos;
  return 1;
}

protected void unsubscribe(object session, string topic) {
  DEBUG("SERVER: unsubscribe(%O, %O)\n", session, topic);
  mapping s = subscriptions[topic];
  if(!s) return;
  string ci = session->get_client_identifier();
  
  if(s[ci]) m_delete(s, ci);
  if(!sizeof(s)) m_delete(subscriptions, topic);
  
  return;
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
  mapping destinations = subscriptions[topic];
  if(!destinations) return 1;
  foreach(destinations; string identifier; int cqos) {
    object s = clients[identifier];
    if(!s) continue;
    s->publish(topic, message, qos&cqos);
  }
  return 1;
}