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

This document defines what users and integrations can rely on in Apache Ignite. It does not change the rules for approval or deprecation.

## Public API

Public API includes interfaces designed and supported for external use:

- Language and client APIs, including supported plugins and SPIs.
- SQL syntax, types, functions, and behavior.
- REST endpoints, CLI commands and options, and supported machine-readable output.
- Configuration available to users, including default values.
- Supported metrics, JMX interfaces, and system views.
- Client protocols and serialization formats used by external systems.

The contract covers names, signatures, schemas, accepted inputs, results, error codes, and supported behavior. An interface designed for external use is not internal just because its documentation is missing. A public method in an internal class is not automatically part of the public API.

## Observable behavior

Public behavior is what a supported interface guarantees: its results, side effects, errors, and when it calls overridden methods or callbacks. Changing this behavior can break compatibility even if method signatures stay the same. A contract remains public even if we know of no external users.

These guarantees include transaction behavior, ordering, retries, and cancellation. For example, the contract should say whether an operation can still finish after the caller receives a timeout. How the implementation manages timers is an internal detail.

SQLState values and error codes returned to users are part of the contract, even if they come from an internal exception class. The class itself remains internal unless it is part of a supported external interface.

## Compatibility contracts

Protocols between nodes and stored data formats are separate from external APIs. They also have compatibility requirements. These cover communication, discovery, and internal management messages, as well as stored data, metadata, WAL, and snapshots.

We check compatibility for supported combinations of clients and servers, and for supported ways to upgrade, recover, and read stored data. This does not mean that any two versions can work together or that rolling upgrades are supported. Source and binary API compatibility, protocol compatibility, and stored data compatibility are separate concerns.

## Outside the contract

The contract does not cover internal algorithms and classes, stack traces, or undocumented implementation details. It also does not cover log wording or execution timing that the interface does not guarantee. Documented structured events remain part of the contract.

A bug fix that restores previously supported behavior does not change the contract. If a fix deliberately changes supported behavior, it changes the contract too. Experimental interfaces designed for external use are still public, but their stated stability limits apply.
