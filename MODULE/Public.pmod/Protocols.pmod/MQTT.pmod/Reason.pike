int local_disconnect = 0;
string message;
mixed backtrace;

protected void create(int _local, mixed _backtrace) { local_disconnect = _local; backtrace = _backtrace; }

protected string _sprintf(int t) {
  return "Reason(" + (local_disconnect?"local":"remote") + ")";
}