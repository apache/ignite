/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.twostep.messages;

import org.apache.ignite.internal.processors.cache.query.*;

import java.io.*;
import java.util.*;

/**
 * Query request.
 */
public class GridQueryRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long reqId;

    /** */
    private int pageSize;

    /** */
    private Collection<GridCacheSqlQuery> qrys;

    /**
     * @param reqId Request ID.
     * @param pageSize Page size.
     * @param qrys Queries.
     */
    public GridQueryRequest(long reqId, int pageSize, Collection<GridCacheSqlQuery> qrys) {
        this.reqId = reqId;
        this.pageSize = pageSize;
        this.qrys = qrys;
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Queries.
     */
    public Collection<GridCacheSqlQuery> queries() {
        return qrys;
    }
}
