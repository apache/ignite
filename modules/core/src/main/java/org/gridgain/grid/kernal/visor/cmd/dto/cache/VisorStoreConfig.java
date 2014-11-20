/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for cache store configuration properties.
 */
public class VisorStoreConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache store. */
    private String store;

    /** Should value bytes be stored. */
    private boolean valueBytes;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for cache store configuration properties.
     */
    public static VisorStoreConfig from(GridCacheConfiguration ccfg) {
        VisorStoreConfig cfg = new VisorStoreConfig();

        cfg.store(compactClass(ccfg.getStore()));
        cfg.valueBytes(ccfg.isStoreValueBytes());

        return cfg;
    }

    public boolean enabled() {
        return store != null;
    }

    /**
     * @return Cache store.
     */
    @Nullable public String store() {
        return store;
    }

    /**
     * @param store New cache store.
     */
    public void store(String store) {
        this.store = store;
    }

    /**
     * @return Should value bytes be stored.
     */
    public boolean valueBytes() {
        return valueBytes;
    }

    /**
     * @param valBytes New should value bytes be stored.
     */
    public void valueBytes(boolean valBytes) {
        valueBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorStoreConfig.class, this);
    }
}
