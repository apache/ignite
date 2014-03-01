/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.cache;

import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Adds affinity node ID to cache responses.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheRestResponse extends GridRestResponse {
    /** Affinity node ID. */
    private String affinityNodeId;

    /**
     * @return Affinity node ID.
     */
    public String getAffinityNodeId() {
        return affinityNodeId;
    }

    /**
     * @param affinityNodeId Affinity node ID.
     */
    public void setAffinityNodeId(String affinityNodeId) {
        this.affinityNodeId = affinityNodeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheRestResponse.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeString(out, affinityNodeId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        affinityNodeId = U.readString(in);
    }
}
