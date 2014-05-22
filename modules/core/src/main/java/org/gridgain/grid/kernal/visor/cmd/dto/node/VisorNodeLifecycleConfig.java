/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.*;

/**
 * Lifecycle configuration data.
 */
public class VisorNodeLifecycleConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Lifecycle beans. */
    private final String beans;

    /** Whether or not email notifications should be used on node start and stop. */
    private final boolean ntf;

    public VisorNodeLifecycleConfig(String beans, boolean ntf) {
        this.beans = beans;
        this.ntf = ntf;
    }

    /**
     * @return Lifecycle beans.
     */
    public String beans() {
        return beans;
    }

    /**
     * @return Whether or not email notifications should be used on node start and stop.
     */
    public boolean emailNotification() {
        return ntf;
    }
}
