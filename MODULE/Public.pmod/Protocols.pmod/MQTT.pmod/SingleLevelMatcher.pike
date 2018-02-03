inherit .Matcher;

string pattern;

protected void validate() {
	pattern = replace(topic, "+", "%*s");
}

int(0..1) match(string destination) {
	
	werror("match?\n");
	return sscanf(destination, pattern) == 1;
}