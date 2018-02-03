inherit .Matcher;

string prefix;

void validate() {
  if(!has_suffix(topic, "/#")) throw(Error.Generic("Multi-level wild card must be last element of filter.\n"));
  prefix = topic[0..sizeof(topic) - 3];
werror("prefix: %O\n", prefix);
}

int(0..1) match(string destination) {
  if(destination == prefix || has_prefix(destination, prefix + "/")) return 1;
  else return 0;  
}
