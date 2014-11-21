/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * GGFS endpoint descriptor.
 */
public class VisorGgfsEndpoint implements Serializable{
    /** */
    private static final long  serialVersionUID = 0L;

    /** GGFS name. */
    private final String ggfsName;

    /** Grid name. */
    private final String gridName;

    /** Host address / name. */
    private final String hostName;

    /** Port number. */
    private final int port;

    /**
     * Create GGFS endpoint descriptor with given parameters.
     * @param ggfsName GGFS name.
     * @param gridName Grid name.
     * @param hostName Host address / name.
     * @param port Port number.
     */
    public VisorGgfsEndpoint(@Nullable String ggfsName, String gridName, @Nullable String hostName, int port) {
        this.ggfsName = ggfsName;
        this.gridName = gridName;
        this.hostName = hostName;
        this.port = port;
    }

    /**
     * @return GGFS name.
     */
    @Nullable public String ggfsName() {
        return ggfsName;
    }

    /**
     * @return Grid name.
     */
    public String gridName() {
        return gridName;
    }

    /**
     * @return Host address / name.
     */
    @Nullable public String hostName() {
        return hostName;
    }

    /**
     * @return Port number.
     */
    public int port() {
        return port;
    }

    /**
     * @return URI Authority
     */
    public String authority() {
        String addr = hostName + ":" + port;

        if (ggfsName == null && gridName == null)
            return addr;
        else if (ggfsName == null)
            return gridName + "@" + addr;
        else if (gridName == null)
            return ggfsName + "@" + addr;
        else
            return ggfsName + ":" + gridName + "@" + addr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGgfsEndpoint.class, this);
    }
}
