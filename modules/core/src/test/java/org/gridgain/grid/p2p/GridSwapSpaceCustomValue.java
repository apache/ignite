/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;

/**
 */
public class GridSwapSpaceCustomValue implements Serializable {
    /** */
    private long id = -1;

    /** */
    private String data;

    /**
     * @return ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @param id ID.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * @return Data.
     */
    public String getData() {
        return data;
    }

    /**
     * @param data Data.
     */
    public void setData(String data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSwapSpaceCustomValue.class, this);
    }
}
