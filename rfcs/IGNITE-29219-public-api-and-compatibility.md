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

## Purpose

Define what users and integrations can rely on in Apache Ignite, without changing approval or deprecation rules.

## Public API

Public API comprises interfaces intended and supported for external use:

- Language and client APIs, including supported plugins and SPIs.
- SQL syntax, types, functions, and semantics.
- REST endpoints, CLI commands and options, and supported machine-readable output.
- User-facing configuration, including defaults.
- Supported metrics, JMX interfaces, and system views.
- Client protocols and externally consumed serialization formats.

These contracts include names, signatures, schemas, accepted inputs, results, error codes, and supported behavior. Missing documentation does not make an interface intended for external use internal. Conversely, a public method on an internal class does not automatically become public API.

## Observable behavior

The contract includes guarantees such as transaction semantics, ordering, retries, and cancellation. For example, a timeout contract should explain whether the operation can still complete after the caller receives a timeout. How timers are tracked internally is an implementation detail.

Externally exposed SQLState values and error codes belong to the contract even when produced by an internal exception class. The class itself remains internal unless it is part of a supported external interface.

## Compatibility contracts

Internode protocols and persistent formats are separate from external APIs, but changes can affect compatibility. This includes communication, discovery, and internal management messages, as well as stored data, metadata, WAL, and snapshots.

Compatibility is assessed for supported client/server combinations and upgrade, recovery, and data-reading paths. This does not promise interoperability between arbitrary versions or introduce rolling-upgrade support. Source and binary API compatibility, wire compatibility, and persistent-data compatibility are distinct concerns.

## Outside the contract

Internal algorithms and classes, incidental log wording, stack traces, incidental execution timings, and undocumented implementation details are not contracts. Documented structured events remain covered.

A bug fix that restores established behavior does not redefine the contract. Calling a change a bug fix does not exempt a deliberate change to supported behavior. Experimental interfaces remain public where intended for external use, with their explicitly stated stability limits.
