package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.DATA_STREAM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;

/**
 * Default IO policy resolver.
 */
class DefaultIoPolicyResolver implements IgniteClosure<ClusterNode, Byte> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Byte apply(ClusterNode gridNode) {
        assert gridNode != null;

        IgniteProductVersion version = gridNode.version();

        //TODO: change version checking before merge.
        if (gridNode.isLocal() || version.greaterThanEqual(1, 7, 3))
            return DATA_STREAM_POOL;
        else
            return PUBLIC_POOL;
    }
}
