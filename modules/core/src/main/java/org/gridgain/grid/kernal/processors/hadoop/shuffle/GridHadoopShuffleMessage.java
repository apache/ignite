/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.util.direct.*;

import java.nio.*;

/**
 * Shuffle message.
 */
public class GridHadoopShuffleMessage extends GridTcpCommunicationMessageAdapter {
    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 81;
    }

    /** {@inheritDoc} */
    @Override public GridTcpCommunicationMessageAdapter clone() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {

    }
}
