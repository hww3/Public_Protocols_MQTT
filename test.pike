
object p;


int main() {

  p = Public.Protocols.MQTT.client("mqtts://37.187.106.16");
  p->set_client_identifier("hww3-" + time());
  p->set_disconnect_callback(dis_cb);
  werror("client: %O\n", p);
  p->connect(has_connected);
  werror("client: %O\n", p);
 return -1;

}


void has_connected(object client) {
	werror("Client connected: %O\n", client);
	client->subscribe("hww3/test", pub_cb);
	client->publish("hww3/test", "client_connect");
}

int c = 0;
void pub_cb(object client, string topic, string body) {
	c ++;
	werror("received message: %O, %s -> %O\n", client, topic, body);
	
	if(c > 2) {
  	client->unsubscribe(topic, pub_cb);
	client->disconnect();
  }
}

void dis_cb(object client) {
	werror("client disconnected: %O\n", client);
}