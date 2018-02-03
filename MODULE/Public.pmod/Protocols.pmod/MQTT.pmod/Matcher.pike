
// prototype for all wildcard matchers

public string topic = "";

protected void create(string topic) {
  this->topic = topic;
  validate();
}

void validate();

int(0..1) match(string destination);
