/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link org.gridgain.grid.ggfs.IgniteFs}.
 */
public class VisorGgfs implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS instance name. */
    private final String name;

    /** GGFS instance working mode. */
    private final GridGgfsMode mode;

    /** GGFS metrics. */
    private final VisorGgfsMetrics metrics;

    /** Whether GGFS has configured secondary file system. */
    private final boolean secondaryFsConfigured;

    /**
     * Create data transfer object.
     *
     * @param name GGFS name.
     * @param mode GGFS mode.
     * @param metrics GGFS metrics.
     * @param secondaryFsConfigured Whether GGFS has configured secondary file system.
     */
    public VisorGgfs(
        String name,
        GridGgfsMode mode,
        VisorGgfsMetrics metrics,
        boolean secondaryFsConfigured
    ) {
        this.name = name;
        this.mode = mode;
        this.metrics = metrics;
        this.secondaryFsConfigured = secondaryFsConfigured;
    }

    /**
     * @param ggfs Source GGFS.
     * @return Data transfer object for given GGFS.
     * @throws GridException
     */
    public static VisorGgfs from(IgniteFs ggfs) throws GridException {
        assert ggfs != null;

        return new VisorGgfs(
            ggfs.name(),
            ggfs.configuration().getDefaultMode(),
            VisorGgfsMetrics.from(ggfs.metrics()),
            ggfs.configuration().getSecondaryFileSystem() != null
        );
    }

    /**
     * @return GGFS instance name.
     */
    public String name() {
        return name;
    }

    /**
     * @return GGFS instance working mode.
     */
    public GridGgfsMode mode() {
        return mode;
    }

    /**
     * @return GGFS metrics.
     */
    public VisorGgfsMetrics metrics() {
        return metrics;
    }

    /**
     * @return Whether GGFS has configured secondary file system.
     */
    public boolean secondaryFileSystemConfigured() {
        return secondaryFsConfigured;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGgfs.class, this);
    }
}
