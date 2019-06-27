/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.websocket;

/**
 * Contains web socket endpoint names.
 */
public interface WebSocketEvents {
    /** */
    public static final String AGENTS_PATH = "/agents";

    /** */
    public static final String BROWSERS_PATH = "/browsers";

    /** */
    public static final String ERROR = "error";

    /** */
    public static final String AGENT_HANDSHAKE = "agent:handshake";

    /** */
    public static final String AGENT_REVOKE_TOKEN = "agent:revoke:token";

    /** */
    public static final String AGENT_STATUS = "agent:status";

    /** */
    public static final String ADMIN_ANNOUNCEMENT = "admin:announcement";

    /** */
    public static final String SCHEMA_IMPORT_DRIVERS = "schemaImport:drivers";

    /** */
    public static final String SCHEMA_IMPORT_SCHEMAS = "schemaImport:schemas";

    /** */
    public static final String SCHEMA_IMPORT_METADATA = "schemaImport:metadata";

    /** */
    public static final String NODE_REST = "node:rest";

    /** */
    public static final String NODE_VISOR = "node:visor";

    /** */
    public static final String CLUSTER_TOPOLOGY = "cluster:topology";
}
