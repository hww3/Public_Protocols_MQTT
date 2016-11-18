
constant QOS_AT_MOST_ONCE = 0;
constant QOS_AT_LEAST_ONCE = 1;
constant QOS_EXACTLY_ONCE = 2;

mapping message_registry = ([]);


protected void create() {
  message_registry[.ConnectMessage.MESSAGE_NUMBER] = .ConnectMessage; 
  message_registry[.ConnAckMessage.MESSAGE_NUMBER] = .ConnAckMessage; 
  message_registry[.PublishMessage.MESSAGE_NUMBER] = .PublishMessage;  
  werror("Message Registry: %O\n", message_registry);
}