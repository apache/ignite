<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Public API, observable behavior, and compatibility contracts

Ticket: [IGNITE-29219](https://issues.apache.org/jira/browse/IGNITE-29219)

## Public interfaces

Public interfaces are designed and supported for external use:

- **Programming APIs:** language and client APIs, plugins, and SPIs.
- **SQL:** syntax, types, functions, and behavior.
- **REST and CLI:** endpoints, commands, options, and supported machine-readable output.
- **Configuration:** user settings and default values.
- **Monitoring:** supported metrics, JMX interfaces, system views, and documented structured events.
- **Data exchange:** client protocols and serialization formats used by external systems.

An interface remains public even if its documentation is missing or we know of no external users. Experimental public interfaces follow their stated stability limits.

## Contract guarantees

The contract of each public interface covers:

- **Structure:** names, signatures, and schemas.
- **Inputs and results:** accepted inputs and results.
- **Behavior:** side effects and guarantees about transactions, ordering, retries, and cancellation.
- **Errors:** errors, including error codes and SQLState values.
- **Extension points:** guarantees about calls to overridden methods and callbacks.

## Compatibility

Check these parts of compatibility separately:

- **API compatibility:** source and binary compatibility.
- **Protocol compatibility:** client/server protocols and protocols between nodes, including communication, discovery, and internal management messages.
- **Stored data compatibility:** data, metadata, WAL, and snapshots.

Protocols between nodes and stored data formats have separate contracts from external APIs. These checks cover supported client/server combinations, upgrades, recovery, and reading stored data. This RFC does not add support for other version combinations or rolling upgrades.

## Outside the contract

- **Implementation details:** internal classes, algorithms, and undocumented implementation details.
- **Diagnostics:** stack traces, plus log wording that the interface does not guarantee.
- **Timing:** execution timing that the interface does not guarantee.

An internal class does not become public API because it has public methods or produces public error codes.
