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

## Public API and behavior

Public API includes interfaces designed and supported for external use:

- Language and client APIs, plugins, and SPIs.
- SQL syntax, types, functions, and behavior.
- REST endpoints, CLI commands and options, and supported machine-readable output.
- User configuration and default values.
- Supported metrics, JMX interfaces, system views, and documented structured events.
- Client protocols and serialization formats used by external systems.

The contract covers names, signatures, schemas, accepted inputs, results, side effects, and errors, including error codes and SQLState values. It also covers guarantees about transactions, ordering, retries, cancellation, and calls to overridden methods or callbacks.

An interface remains public even if its documentation is missing or we know of no external users. Experimental public interfaces follow their stated stability limits.

## Compatibility

Protocols between nodes and stored data formats are separate from external APIs. Their compatibility contracts cover communication, discovery, and internal management messages, plus data, metadata, WAL, and snapshots.

Check source and binary API compatibility, protocol compatibility, and stored data compatibility separately. This applies to supported client/server combinations, upgrades, recovery, and reading stored data. This RFC does not add support for other version combinations or rolling upgrades.

## Outside the contract

Internal classes and algorithms, stack traces, and undocumented implementation details are outside the contract. So are log wording and execution timing that the interface does not guarantee.

An internal class does not become public API because it has public methods or produces public error codes.
