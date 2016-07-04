package org.apache.ignite.internal.processors.hadoop.planner;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Map;
import java.util.UUID;

/**
 * Map-reduce plan topology.
 */
public class HadoopMapReducePlanTopology {
    /** Node ID to group map. */
    private final Map<UUID, HadoopMapReducePlanGroup> idToGrp;

    /** Host to group map. */
    private final Map<String, HadoopMapReducePlanGroup> hostToGrp;

    /**
     * Constructor.
     *
     * @param idToGrp ID to group map.
     * @param hostToGrp Host to group map.
     */
    public HadoopMapReducePlanTopology(Map<UUID, HadoopMapReducePlanGroup> idToGrp,
        Map<String, HadoopMapReducePlanGroup> hostToGrp) {
        assert idToGrp != null;
        assert hostToGrp != null;

        this.idToGrp = idToGrp;
        this.hostToGrp = hostToGrp;
    }

    /**
     * Get group for node ID.
     *
     * @param id Node ID.
     * @return Group.
     */
    public HadoopMapReducePlanGroup groupForId(UUID id) {
        return idToGrp.get(id);
    }

    /**
     * Get group for host.
     *
     * @param host Host.
     * @return Group.
     */
    public HadoopMapReducePlanGroup groupForHost(String host) {
        return hostToGrp.get(host);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopMapReducePlanTopology.class, this);
    }
}
