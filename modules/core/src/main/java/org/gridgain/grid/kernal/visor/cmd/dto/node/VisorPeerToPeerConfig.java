/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils.*;

/**
 * Data transfer object for node P2P configuration properties.
 */
public class VisorPeerToPeerConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether peer-to-peer class loading is enabled. */
    private boolean p2pEnabled;

    /** Missed resource cache size. */
    private int p2pMissedResCacheSize;

    /** List of packages from the system classpath that need to be loaded from task originating node. */
    private String p2pLocClsPathExcl;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node P2P configuration properties.
     */
    public static VisorPeerToPeerConfig from(GridConfiguration c) {
        VisorPeerToPeerConfig cfg = new VisorPeerToPeerConfig();

        cfg.p2pEnabled(c.isPeerClassLoadingEnabled());
        cfg.p2pMissedResponseCacheSize(c.getPeerClassLoadingMissedResourcesCacheSize());
        cfg.p2pLocalClassPathExclude(compactArray(c.getPeerClassLoadingLocalClassPathExclude()));

        return cfg;
    }

    /**
     * @return Whether peer-to-peer class loading is enabled.
     */
    public boolean p2pEnabled() {
        return p2pEnabled;
    }

    /**
     * @param p2pEnabled New whether peer-to-peer class loading is enabled.
     */
    public void p2pEnabled(boolean p2pEnabled) {
        this.p2pEnabled = p2pEnabled;
    }

    /**
     * @return Missed resource cache size.
     */
    public int p2pMissedResponseCacheSize() {
        return p2pMissedResCacheSize;
    }

    /**
     * @param p2pMissedResCacheSize New missed resource cache size.
     */
    public void p2pMissedResponseCacheSize(int p2pMissedResCacheSize) {
        this.p2pMissedResCacheSize = p2pMissedResCacheSize;
    }

    /**
     * @return List of packages from the system classpath that need to be loaded from task originating node.
     */
    @Nullable public String p2pLocalClassPathExclude() {
        return p2pLocClsPathExcl;
    }

    /**
     * @param p2pLocClsPathExcl New list of packages from the system classpath that need to be loaded from task
     * originating node.
     */
    public void p2pLocalClassPathExclude(@Nullable String p2pLocClsPathExcl) {
        this.p2pLocClsPathExcl = p2pLocClsPathExcl;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPeerToPeerConfig.class, this);
    }
}
