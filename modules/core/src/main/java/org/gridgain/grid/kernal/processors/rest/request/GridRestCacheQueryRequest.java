/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.request;

import org.gridgain.grid.kernal.processors.rest.client.message.*;

import java.io.Serializable;

/**
 * Cache query request.
 */
public class GridRestCacheQueryRequest extends GridRestRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Request message. */
    private GridClientCacheQueryRequest msg;

    /**
     * @param msg Client request message.
     */
    public GridRestCacheQueryRequest(GridClientCacheQueryRequest msg) {
        this.msg = msg;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return msg.queryId();
    }

    /**
     * @return Operation.
     */
    public GridClientCacheQueryRequest.GridQueryOperation operation() {
        return msg.operation();
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return msg.cacheName();
    }

    /**
     * @return Query clause.
     */
    public String clause() {
        return msg.clause();
    }

    /**
     * @return Query type.
     */
    public GridClientCacheQueryRequest.GridQueryType type() {
        return msg.type();
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return msg.pageSize();
    }

    /**
     * @return Timeout.
     */
    public long timeout() {
        return msg.timeout();
    }

    /**
     * @return Include backups.
     */
    public boolean includeBackups() {
        return msg.includeBackups();
    }

    /**
     * @return Enable dedup.
     */
    public boolean enableDedup() {
        return msg.enableDedup();
    }

    /**
     * @return Keep portable flag.
     */
    public boolean keepPortable() {
        return msg.keepPortable();
    }

    /**
     * @return Class name.
     */
    public String className() {
        return msg.className();
    }

    /**
     * @return Remot reducer class name.
     */
    public String remoteReducerClassName() {
        return msg.remoteReducerClassName();
    }

    /**
     * @return Remote transformer class name.
     */
    public String remoteTransformerClassName() {
        return msg.remoteTransformerClassName();
    }

    /**
     * @return Query arguments.
     */
    public Object[] queryArguments() {
        return msg.queryArguments();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return msg.toString();
    }
}
