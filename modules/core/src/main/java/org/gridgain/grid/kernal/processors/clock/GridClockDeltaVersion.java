/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.clock;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Version for time delta snapshot.
 */
public class GridClockDeltaVersion implements Comparable<GridClockDeltaVersion>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Snapshot local version. */
    private long ver;

    /** Topology version. */
    private long topVer;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridClockDeltaVersion() {
        // No-op.
    }

    /**
     * @param ver Version.
     * @param topVer Topology version.
     */
    public GridClockDeltaVersion(long ver, long topVer) {
        this.ver = ver;
        this.topVer = topVer;
    }

    /**
     * @return Snapshot local version.
     */
    public long version() {
        return ver;
    }

    /**
     * @return Snapshot topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridClockDeltaVersion o) {
        if (topVer == o.topVer) {
            if (ver == o.ver)
                return 0;

            return ver > o.ver ? 1 : -1;
        }

        return topVer > o.topVer ? 1 : -1;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridClockDeltaVersion))
            return false;

        GridClockDeltaVersion that = (GridClockDeltaVersion)o;

        return topVer == that.topVer && ver == that.ver;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (int)(ver ^ (ver >>> 32));

        res = 31 * res + (int)(topVer ^ (topVer >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(ver);
        out.writeLong(topVer);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ver = in.readLong();
        topVer = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClockDeltaVersion.class, this);
    }
}
