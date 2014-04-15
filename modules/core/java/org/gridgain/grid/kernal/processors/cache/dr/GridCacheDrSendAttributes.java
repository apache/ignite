/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr;

import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * DR sender cache attributes.
 */
public class GridCacheDrSendAttributes implements Externalizable {
    private static final long serialVersionUID = 0L;

    /** Data center replication mode. */
    private GridDrSenderCacheMode mode;

    /** Replication sender hub load balancing policy. */
    private GridDrSenderHubLoadBalancingMode sndHubLoadBalancingPlc;

    /** Class name for replication cache entry filter. */
    private String entryFilterClsName;

    /**
     * {@link Externalizable} support.
     */
    public GridCacheDrSendAttributes() {
        // No-op.
    }

    /**
     * @param cfg Configuration.
     */
    public GridCacheDrSendAttributes(GridDrSenderCacheConfiguration cfg) {
        assert cfg != null;

        entryFilterClsName = className(cfg.getEntryFilter());
        mode = cfg.getMode();
        sndHubLoadBalancingPlc = cfg.getSenderHubLoadBalancingMode();
    }

    /**
     * @return Data center replication mode.
     */
    public GridDrSenderCacheMode mode() {
        return mode;
    }

    /**
     * @return Replication sender hub load balancing policy.
     */
    public GridDrSenderHubLoadBalancingMode senderHubLoadBalancingPolicy() {
        return sndHubLoadBalancingPlc;
    }

    /**
     * @return Class name for replication cache entry filter.
     */
    public String entryFilterClassName() {
        return entryFilterClsName;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, mode);
        U.writeEnum(out, sndHubLoadBalancingPlc);
        U.writeString(out, entryFilterClsName);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        mode = U.readEnum(in, GridDrSenderCacheMode.class);
        sndHubLoadBalancingPlc =U.readEnum(in, GridDrSenderHubLoadBalancingMode.class);
        entryFilterClsName = U.readString(in);
    }

    /**
     * @param obj Object to get class of.
     * @return Class name or {@code null}.
     */
    @Nullable private static String className(@Nullable Object obj) {
        return obj != null ? obj.getClass().getName() : null;
    }
}
