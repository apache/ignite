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

# Public API, public behavior, and compatibility contracts

Ticket: [IGNITE-29219](https://issues.apache.org/jira/browse/IGNITE-29219)

## Public behavior

A compatible change preserves supported functionality and its guarantees. This applies to external interfaces, stored data, and supported build and deployment setups:

- **Results and effects:** accepted inputs, guarantees about results, and side effects.
- **Operations:** guarantees about transactions, ordering, retries, and cancellation.
- **Errors:** errors, including error codes and SQLState values.
- **Extensions:** guarantees about calls to overridden methods and callbacks.

When documentation is incomplete, use tests and existing usage to help identify the public behavior. A test alone does not make an implementation detail part of the public contract.

## External interfaces

These interfaces are designed and supported for external use:

- **Programming APIs:** language and client APIs, plugins, and SPIs.
- **SQL:** syntax, types, and functions.
- **REST and CLI:** endpoints, commands, options, exit codes, and supported machine-readable output.
- **Configuration:** user settings, including system properties, environment variables, and distributed properties, plus their default values.
- **Monitoring:** supported metrics, JMX interfaces, system views, and documented structured events.
- **Client protocols:** client protocols and external serialization formats.

A compatible change keeps existing supported calls, queries, commands, configurations, and integrations valid. Their names, signatures, and schemas stay compatible, and their public behavior is preserved.

- **Source compatibility:** existing application and plugin source code still compiles.
- **Binary compatibility:** compiled applications and plugins still work without rebuilding.
- **Client/server compatibility:** supported combinations of client and server versions keep working.

An interface remains public even if its documentation is missing or we know of no external users. Experimental public interfaces follow their stated stability limits.

## Stored data

This covers data, metadata, WAL, and snapshot formats used during supported upgrades and recovery.

A compatible change preserves the ability to read and recover existing data with the same meaning and durability guarantees. The physical format may change through a documented migration.

## Distribution and runtime

This covers published artifact names and coordinates, and supported JDK and operating system versions.

A compatible change keeps supported build and deployment setups working.

## Supported versions

Document supported client/server version combinations, data upgrade paths, and JDK and operating system versions in the relevant user documentation. If this information is incomplete, clarify it in the RFC. Missing documentation does not remove existing guarantees.

## Outside the contract

- **Implementation details:** classes outside the public API, internal algorithms, and undocumented implementation details.
- **Diagnostics:** stack traces, plus log wording that the interface does not guarantee.
- **Timing:** execution timing that the interface does not guarantee.

A class in an internal package can still be part of the public API. The same compatibility rules apply to it.

Public methods or public error codes alone do not make a class part of the public API.
