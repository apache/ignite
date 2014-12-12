/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.twostep;

import org.gridgain.grid.util.direct.*;

import java.nio.*;

/**
 * Request to fetch next page.
 */
public class GridNextPageRequest extends GridTcpCommunicationMessageAdapter {
    /** */
    private long reqId;

    /** */
    private long qryId;

    /** */
    private int qry;

    /** */
    private int offset;

    /** */
    private int pageSize;

    @Override public boolean writeTo(ByteBuffer buf) {
        return false;
    }

    @Override public boolean readFrom(ByteBuffer buf) {
        return false;
    }

    @Override public byte directType() {
        return 0;
    }

    @Override public GridTcpCommunicationMessageAdapter clone() {
        return null;
    }

    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {

    }
}
