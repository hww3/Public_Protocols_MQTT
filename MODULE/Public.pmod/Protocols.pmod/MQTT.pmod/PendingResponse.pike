#ifdef MQTT_DEBUG
#define DEBUG(X ...) werror("MQTT.PendingResponse: " + X)
#else
#define DEBUG(X ...)
#endif /* MQTT_DEBUG */

int message_identifier;
.Message message;
.Message original_message;
mixed timeout_callout_id;
function(.PendingResponse:void) success;
function(.PendingResponse:void) failure;
int timeout;
int timeout_timestamp;
int attempts;
int max_retries;
object client;
mixed data;

protected void create(object client, int message_identifier, .Message orig, int timeout, int max_retries) {
  this.client = client;
  this.message_identifier = message_identifier;
  this.original_message = orig;
  this.timeout = timeout;
  this.max_retries = max_retries;

  if(timeout)
    configure();
}

protected void configure() {
  timeout_timestamp = timeout + time();
  timeout_callout_id = call_out(failure_callout, timeout, this);
}

protected void failure_callout(.PendingResponse response) {
  if(max_retries >= attempts) {
werror("Timed out waiting for response to message_identifier=%d, resending\n", message_identifier);
    attempts ++;
    original_message->set_dup_flag();
    client->send_message(original_message);
    configure();
  }
  else {
  DEBUG("Timed out waiting for response to message_identifier=%d, max_retries reached.\n", message_identifier);

    client->unregister_pending(message_identifier);
    if(failure) failure(this);
  }
  
}

void received_message(.Message message) {
  this.message = message;
  if(timeout_callout_id) remove_call_out(timeout_callout_id);
  if(timeout) client->unregister_pending(message_identifier); // messages with no timeout will be handled synchronously.
  if(success) success(this);
}