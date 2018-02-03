inherit .Matcher;

int(0..1) match(string destination) {
  if(sizeof(destination) && destination[0] == '$') return 0;
  else return 1;
}
