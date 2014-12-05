/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.port;

import org.apache.ignite.spi.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * This class defines record about port use.
 */
public class GridPortRecord {
    /** Port. */
    private int port;

    /** Protocol. */
    private IgnitePortProtocol proto;

    /** Class which uses port. */
    private Class cls;

    /**
     * @param port Port.
     * @param proto Protocol.
     * @param cls Class.
     */
    GridPortRecord(int port, IgnitePortProtocol proto, Class cls) {
        this.port = port;
        this.proto = proto;
        this.cls = cls;
    }

    /**
     * @return Port.
     */
    public int port() {
        return port;
    }

    /**
     * @return Protocol.
     */
    public IgnitePortProtocol protocol() {
        return proto;
    }

    /**
     * @return Class.
     */
    public Class clazz() {
        return cls;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPortRecord.class, this);
    }
}
