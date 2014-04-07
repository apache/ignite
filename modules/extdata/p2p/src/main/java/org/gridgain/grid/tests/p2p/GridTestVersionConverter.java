/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.gridgain.grid.kernal.processors.version.*;

import java.nio.*;

/**
 * Test version converter.
 */
public class GridTestVersionConverter extends GridVersionConverter {
    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        buf.putInt(1111);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        buf.putInt(2222);

        return true;
    }
}
