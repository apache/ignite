

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
    
    /** 启动一个集群grid */
    public static final String AGENT_START_CLUSTER = "agent:startCluster";    
    
    /** 停止一个集群grid */
    public static final String AGENT_STOP_CLUSTER = "agent:stopCluster";
    
    /** 调用集群grid的服务 */
    public static final String AGENT_CALL_CLUSTER_SERVICE = "agent:callClusterService";
    
    /** 调用集群Control Command */
    public static final String AGENT_CALL_CLUSTER_COMMAND = "agent:callClusterCommand";

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
