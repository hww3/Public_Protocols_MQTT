import Public.Protocols.MQTT;

Stdio.Port port;

int main() {
  port = Stdio.Port();
  port->bind(1883, accept_cb);
  return -1;
}


void accept_cb(mixed id) {
  werror("Accepting connection\n");
  Stdio.File conn = port->accept();
  ServerConnection connection = ServerConnection(conn, this);  
}