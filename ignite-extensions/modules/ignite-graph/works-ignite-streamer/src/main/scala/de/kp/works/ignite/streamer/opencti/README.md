
# OpenCTI Streamer

The OpenCTI Ignite streamer is responsible for listening to the OpenCTI SSE endpoint
and retrieving STIX compliant threat intelligence events.

The events are pushed to a temporary Ignite (notification) cache, and the implemented 
OpenCTI processor then turns events into edges & vertices of a *threat intelligence* 
information network.