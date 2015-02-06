/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.twostep.messages;


import java.io.*;

/**
 * Request to fetch next page.
 */
public class GridNextPageRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long qryReqId;

    /** */
    private int qry;

    /** */
    private int pageSize;

    /**
     * @param qryReqId Query request ID.
     * @param qry Query.
     * @param pageSize Page size.
     */
    public GridNextPageRequest(long qryReqId, int qry, int pageSize) {
        this.qryReqId = qryReqId;
        this.qry = qry;
        this.pageSize = pageSize;
    }

    /**
     * @return Query request ID.
     */
    public long queryRequestId() {
        return qryReqId;
    }

    /**
     * @return Query.
     */
    public int query() {
        return qry;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }
}
