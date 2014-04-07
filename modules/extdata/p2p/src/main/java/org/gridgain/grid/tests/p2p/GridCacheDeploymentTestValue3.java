/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 *
 */
public class GridCacheDeploymentTestValue3 implements Serializable {
    /** */
    private String val = "test-" + System.currentTimeMillis();

    /**
     * @return Value.
     */
    public String getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDeploymentTestValue3.class, this);
    }
}
