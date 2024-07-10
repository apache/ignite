package org.apache.ignite.internal.processors.query.h2;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.running.RunningQueryManager;
import org.apache.ignite.internal.processors.query.running.TrackableQuery;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class H2FullTrackableQuery implements TrackableQuery {
    /** Begin timestamp. */
    private final long beginTs;

    /** Query id. */
    private final long qryId;

    /** Aggregator node id. */
    private final UUID aggrNodeId;

    /** Distributed joins flag. */
    private final boolean distributedJoin;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /** Lazy mode flag. */
    private final boolean lazy;

    /** Schema name. */
    private final String schema;

    /** Sql text. */
    private final String sql;

    /** Map nodes. */
    private final Collection<ClusterNode> mapNodes;

    /**
     * @param beginTs Begin timestamp.
     * @param qryId Query id.
     * @param aggrNodeId Aggregator node id.
     * @param distributedJoin Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param lazy Lazy mode flag.
     * @param schema Schema name.
     * @param sql Sql text.
     * @param mapNodes Map nodes.
     */
    public H2FullTrackableQuery(
        long beginTs,
        long qryId,
        UUID aggrNodeId,
        boolean distributedJoin,
        boolean enforceJoinOrder,
        boolean lazy,
        String schema,
        String sql,
        @Nullable Collection<ClusterNode> mapNodes
    ) {
        this.beginTs = beginTs;
        this.qryId = qryId;
        this.aggrNodeId = aggrNodeId;
        this.distributedJoin = distributedJoin;
        this.enforceJoinOrder = enforceJoinOrder;
        this.lazy = lazy;
        this.schema = schema;
        this.sql = sql;
        this.mapNodes = mapNodes;
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return U.currentTimeMillis() - beginTs;
    }

    /** {@inheritDoc} */
    @Override public String queryInfo(@Nullable String additionalInfo) {
        StringBuilder msgSb = new StringBuilder();

        if (qryId == RunningQueryManager.UNDEFINED_QUERY_ID)
            msgSb.append(" [globalQueryId=(undefined), node=").append(aggrNodeId);
        else
            msgSb.append(" [globalQueryId=").append(QueryUtils.globalQueryId(aggrNodeId, qryId));

        if (additionalInfo != null)
            msgSb.append(", ").append(additionalInfo);

        msgSb.append(", duration=").append(time()).append("ms")
            .append(", distributedJoin=").append(distributedJoin)
            .append(", enforceJoinOrder=").append(enforceJoinOrder)
            .append(", lazy=").append(lazy)
            .append(", schema=").append(schema);

        if (mapNodes != null)
            msgSb.append(", map nodes: ").append(getMapNodes(mapNodes));

        msgSb.append(", sql='").append(sql).append("'")
            .append(']');

        return msgSb.toString();
    }

    /**
     * @param mapNodes Map nodes.
     * @return String representation of the list of all map nodes.
     */
    private String getMapNodes(Collection<ClusterNode> mapNodes) {
        StringBuilder nodesSb = new StringBuilder();

        int i = 0;

        for (ClusterNode node : mapNodes) {
            nodesSb.append(node.id());

            if (i++ != mapNodes.size() - 1)
                nodesSb.append(", ");
        }

        return nodesSb.toString();
    }
}
