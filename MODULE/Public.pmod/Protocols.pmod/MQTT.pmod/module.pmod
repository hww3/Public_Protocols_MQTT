
constant QOS_AT_MOST_ONCE = 0;
constant QOS_AT_LEAST_ONCE = 1;
constant QOS_EXACTLY_ONCE = 2;

mapping message_registry = ([]);

#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

protected void create() {
  message_registry[.ConnectMessage.MESSAGE_NUMBER] = .ConnectMessage; 
  message_registry[.ConnAckMessage.MESSAGE_NUMBER] = .ConnAckMessage; 
  message_registry[.PublishMessage.MESSAGE_NUMBER] = .PublishMessage;  
  message_registry[.PubAckMessage.MESSAGE_NUMBER] = .PubAckMessage;
  message_registry[.SubscribeMessage.MESSAGE_NUMBER] = .SubscribeMessage;
  message_registry[.SubscribeAckMessage.MESSAGE_NUMBER] = .SubscribeAckMessage;
  message_registry[.UnsubscribeMessage.MESSAGE_NUMBER] = .UnsubscribeMessage;
  message_registry[.UnsubscribeAckMessage.MESSAGE_NUMBER] = .UnsubscribeAckMessage;
  message_registry[.PingMessage.MESSAGE_NUMBER] = .PingMessage;
  message_registry[.PingResponseMessage.MESSAGE_NUMBER] = .PingResponseMessage;
    
  DEBUG("Message Registry: %O\n", message_registry);
}