
# FIWARE Streamer

The FIWARE Ignite streamer is responsible for subscribing to FIWARE Context Broker
events. The broker responds with notifications, sent to the notification server
of the FIWARE streamer.

The notifications are pushed to a temporary Ignite (notification) cache, and the
implemented FIWARE processor then turns notifications into edges & vertices of an
*Internet of Things* information network.