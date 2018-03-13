
# x-influx

x-influx (read as x-to-influx) is a simple cli tool 
for importing data into a running influx instance.

At the moment only CSV as input format is supported. There
is a roadmap for adding SQL as import format. Feel free to provide
other input formats as well.

## Arguments


    Usage: rinflux [options] <file>...
        rinflux --help

    Options:
        -h, --help           Print this help message.
        -v, --verbose        Enable verbose logging.
        -u, --user USER      Username for influxdb [default: test].
        -p, --password PASS  Password for influxdb [default: ].
        -d, --database DB    Influx database [default: test].
        -s, --server SRV     The influxdb server for import 
                             [default: http://localhost:8086].
  
        -S, --series VAL     Name of the measurement series [default: series]
        -m, --measure VAL    Name of the measurement value [default: data].
        -t, --tags VAL       Comma seperated list of tags associated to a value.
        -T, --time VAL       Name of the timestamp column [default: timestamp].
        -f, --format FMT     The timestamp format [default: %F %H:%M:%S]
                             See https://docs.rs/chrono/0.4.0/chrono/format/strftime/index.html

        -D, --delimiter DEL  Use another csv delimiter [default: ,].
        --skip-rows NUM      Remove first NUM lines from file [default: 0].


# Contribution

All contributions are very welcome. Please provide pull requests for
bug fixes, new import formats, and so on.

# License

© 2018 @ Henrik Jürges, GPLv3. See the license file for further information.
