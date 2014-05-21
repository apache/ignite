/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto;

import java.io.*;

/**
 * Store configuration data.
 */
public class VisorStoreConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private final String store;
    private final boolean valueBytes;

    public VisorStoreConfig(String store, boolean valueBytes) {
        this.store = store;
        this.valueBytes = valueBytes;
    }

    public boolean enabled() {
        return store != null;
    }

    /**
     * @return Store.
     */
    public String store() {
        return store;
    }

    /**
     * @return Value bytes.
     */
    public boolean valueBytes() {
        return valueBytes;
    }
}
