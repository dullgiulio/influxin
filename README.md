# Influxin - Parent process for influxdb producers

Influnxin reads the standard output of one or more programs (as speficied as command line arguments,
separating each command with a semicolon ';'), and forwards the read lines to InfluxDB in batches.

Influxin doesn't do any data validation: make sure your programs only output valid influxdb lines
to standard output. Anything written to standard error is logged back.

Failing programs are automatically restarted if they exit cleanly (zero exit code).

Influxin can also print everything it reads to standard output again, in addition to sending to
an InfluxDB server.

Influxin respects the HTTP_PROXY environment variable.
