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
 * P2P configuration data.
 */
public class VisorPeerToPeerConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether peer-to-peer class loading is enabled. */
    private final boolean p2pEnabled;

    /** Missed resource cache size. */
    private final int p2pMissedResCacheSize;

    /** List of packages from the system classpath that need to be loaded from task originating node. */
    private final String p2pLocClsPathExcl;

    public VisorPeerToPeerConfig(boolean p2pEnabled, int p2pMissedResCacheSize, String p2pLocClsPathExcl) {
        this.p2pEnabled = p2pEnabled;
        this.p2pMissedResCacheSize = p2pMissedResCacheSize;
        this.p2pLocClsPathExcl = p2pLocClsPathExcl;
    }

    /**
     * @return Whether peer-to-peer class loading is enabled.
     */
    public boolean p2pEnabled() {
        return p2pEnabled;
    }

    /**
     * @return Missed resource cache size.
     */
    public int p2pMissedResponseCacheSize() {
        return p2pMissedResCacheSize;
    }

    /**
     * @return List of packages from the system classpath that need to be loaded from task originating node.
     */
    public String p2pLocaleClassPathExcl() {
        return p2pLocClsPathExcl;
    }
}
