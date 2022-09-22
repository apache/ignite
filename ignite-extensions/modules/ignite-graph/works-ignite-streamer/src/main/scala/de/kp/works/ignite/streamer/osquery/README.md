
# Osquery Streamer

The **Osquery Streamer** implements a TLS endpoint for a fleet of Osquery agents (daemons). Note, that for
each daemon only a single (remote) endpoint can be configured. Therefore, the Osquery Streamer is intended
to support lightweight *fleets* with a limited number of devices or machines.