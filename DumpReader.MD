# EXAMPLES
```bash
> ./control.sh --dump ./snapshots/dmp --print  # will print in deffault format - json

> ./control.sh --dump ./snapshots/dmp --print --format csv

> ./control.sh --dump ./snapshots/dmp --print --format xml

> ./control.sh --dump ./snapshots/dmp --restore --thin 127.0.0.1:10500

> ./control.sh --dump ./snapshots/dmp --restore --thin-config ./thin-client-config.xml

> ./control.sh --dump ./snapshots/dmp --restore --client-node ./config.xml

> ./control.sh --dump ./snapshots/dmp --restore --client-node ./config.xml --use-data-streamer

> ./control.sh --dump ./snapshots/dmp --consumer ru.nizhikov.ignite.dump.MyConsumer
```