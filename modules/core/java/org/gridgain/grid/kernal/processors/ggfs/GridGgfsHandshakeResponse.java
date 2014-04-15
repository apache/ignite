/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Handshake message.
 */
public class GridGgfsHandshakeResponse implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS name. */
    private String ggfsName;

    /** SECONDARY paths. */
    private GridGgfsPaths paths;

    /** Server block size. */
    private long blockSize;

    /** Whether to force sampling on client's side. */
    private Boolean sampling;

    /**
     * {@link Externalizable} support.
     */
    public GridGgfsHandshakeResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param paths Secondary paths.
     * @param blockSize Server default block size.
     */
    public GridGgfsHandshakeResponse(String ggfsName, GridGgfsPaths paths, long blockSize, Boolean sampling) {
        assert paths != null;

        this.ggfsName = ggfsName;
        this.paths = paths;
        this.blockSize = blockSize;
        this.sampling = sampling;
    }

    /**
     * @return GGFS name.
     */
    public String ggfsName() {
        return ggfsName;
    }

    /**
     * @return SECONDARY paths configured on server.
     */
    public GridGgfsPaths secondaryPaths() {
        return paths;
    }

    /**
     * @return Server default block size.
     */
    public long blockSize() {
        return blockSize;
    }

    /**
     * @return Sampling flag.
     */
    public Boolean sampling() {
        return sampling;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ggfsName);

        paths.writeExternal(out);

        out.writeLong(blockSize);

        if (sampling != null) {
            out.writeBoolean(true);
            out.writeBoolean(sampling);
        }
        else
            out.writeBoolean(false);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ggfsName = U.readString(in);

        paths = new GridGgfsPaths();

        paths.readExternal(in);

        blockSize = in.readLong();

        if (in.readBoolean())
            sampling = in.readBoolean();
    }
}
