# IgniteGraph

<p align="center">
  <img src="https://github.com/predictiveworks/ignite-graph/blob/main/images/ignite-graph.png" width="600" alt="IgniteGraph">
</p>


IgniteGraph is a client layer for using Apache Ignite as a graph database. It is 
an implementation of the Apache TinkerPop  interfaces.

## Streaming Ingestion

**IgniteGraph** is a contribution to the **PredictiveWorks.** initiative to leverage Cyber & IoT analytics
side by side. Graph database technology and graph analytics is an excellent fit for this use case.

Connecting IoT device readings on the one hand and e.g. information from Common Vulnerabilities & Exposures
on the other hand, generates insights about potentially malicious behavior of sensors and devices.

IgniteGraph (currently) ships with two different Ignite Data Streamer applications to ingest events from
**FIWARE** enabled IoT networks and **OpenCTI** threat intelligence.

## FIWARE

FIWARE brings a curated framework of open source software components to accelerate and ease the implementation 
of smart IoT platforms and solutions. The main components comprise and information hub, the FIWARE Context Broker, 
and a set of IoT Agents (IOTA) to interact with devices via widely used IoT protocols and bridge between multiple 
message formats and a common NGSI v2 and NGSI-LD based format.

From an analytics perspective, it is preferable to connect to a (single) Context Broker and receive device notification 
in real-time and in NGSI format, instead of interacting with plenty of individual data sources.

Just to name a few of the IoT protocols supported by the FIWARE framework:

ISOXML
LoRaWAN
LWM2M over CoaP
MQTT
OPC-UA
Sigfox

FIWARE is not restricted to these protocols, and also provides an agent library to build custom IoT Agents.

## OpenCTI

OpenCTI is a unified open source platform for all levels of Cyber Threat Intelligence. A major goal is to build 
and provide a powerful knowledge base for cyber threat intelligence and cyber operations.

OpenCTI ships with a variety of connectors to widely known threat intelligence data sources like AlienVault, 
CrowdStrike, FireEye and MISP, MITRE ATT&CK and more.


## Document vs Vertex 

Vertext: id,label,propKey1,propValue1
         id,label,propKey2,propValue2

Document:id,label,prop1,prop2,prop3

