
object p;

int main() {

  p = Public.Protocols.MQTT.client("mqtt://localhost");
//  p = Public.Protocols.MQTT.client("mqtts://37.187.106.16");
  p->set_client_identifier("hww3-" + time());
  p->set_disconnect_callback(dis_cb);
  werror("Client: %O\n", p);
  p->connect(has_connected);
  werror("Client: %O\n", p);
 return -1;

}

void has_connected(object client) {
	werror("Client connected: %O\n", client);
	client->set_qos_level(2);
	// client->subscribe("hww3/test", pub_cb);
	for(int i = 0; i < 200; i++) call_out(client->publish, 0, "hww3/test", "client_connect" + i, 1);
//	call_out(has_connected, 0, client);
}

int c = 0;
void pub_cb(object client, string topic, string body) {
	c ++;
	werror("Received message: %O, %s -> %O\n", client, topic, body);

	if(c > 2) {
  	client->unsubscribe(topic, pub_cb);
	client->disconnect();
  }
}

void dis_cb(object client, object reason) {
	werror("Client disconnected: %O=>%O\n", client, reason);
}
