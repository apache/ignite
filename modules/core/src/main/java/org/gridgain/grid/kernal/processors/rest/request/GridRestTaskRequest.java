/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.request;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Grid task command request.
 */
public class GridRestTaskRequest extends GridRestRequest {
    /** Task name. */
    private String taskName;

    /** Task Id. */
    private String taskId;

    /** Parameters. */
    private List<Object> params;

    /** Asynchronous execution flag. */
    private boolean async;

    /** Timeout. */
    private long timeout;

    /** Keep portables flag. */
    private boolean keepPortables;

    /**
     * @return Task name, if specified, {@code null} otherwise.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @param taskName Name of task for execution.
     */
    public void taskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * @return Task identifier, if specified, {@code null} otherwise.
     */
    public String taskId() {
        return taskId;
    }

    /**
     * @param taskId Task identifier.
     */
    public void taskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * @return Asynchronous execution flag.
     */
    public boolean async() {
        return async;
    }

    /**
     * @param async Asynchronous execution flag.
     */
    public void async(boolean async) {
        this.async = async;
    }

    /**
     * @return Task execution parameters.
     */
    public List<Object> params() {
        return params;
    }

    /**
     * @param params Task execution parameters.
     */
    public void params(List<Object> params) {
        this.params = params;
    }

    /**
     * @return Timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout Timeout.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Keep portables flag.
     */
    public boolean keepPortables() {
        return keepPortables;
    }

    /**
     * @param keepPortables Keep portables flag.
     */
    public void keepPortables(boolean keepPortables) {
        this.keepPortables = keepPortables;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestTaskRequest.class, this, super.toString());
    }
}
