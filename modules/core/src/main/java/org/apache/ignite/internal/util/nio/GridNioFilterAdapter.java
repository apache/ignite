/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.IgniteCheckedException;

/**
 * Class that defines the piece for application-to-network and vice-versa data conversions
 * (protocol transformations, encryption, etc.)
 */
public abstract class GridNioFilterAdapter implements GridNioFilter {
    /** Filter name. */
    private String name;

    /** Next filter in filter chain. */
    protected GridNioFilter nextFilter;

    /** Previous filter in filter chain. */
    protected GridNioFilter prevFilter;

    /**
     * Assigns filter name to a filter.
     *
     * @param name Filter name. Used in filter chain.
     */
    protected GridNioFilterAdapter(String name) {
        assert name != null;

        this.name = name;
    }

    /** {@inheritDoc} */
    public String toString() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridNioFilter nextFilter() {
        return nextFilter;
    }

    /** {@inheritDoc} */
    @Override public GridNioFilter previousFilter() {
        return prevFilter;
    }

    /** {@inheritDoc} */
    @Override public void nextFilter(GridNioFilter filter) {
        nextFilter = filter;
    }

    /** {@inheritDoc} */
    @Override public void previousFilter(GridNioFilter filter) {
        prevFilter = filter;
    }

    /** {@inheritDoc} */
    @Override public void proceedSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious();

        prevFilter.onSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void proceedSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious();

        prevFilter.onSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override public void proceedExceptionCaught(GridNioSession ses, IgniteCheckedException e) throws IgniteCheckedException {
        checkPrevious();

        prevFilter.onExceptionCaught(ses, e);
    }

    /** {@inheritDoc} */
    @Override public void proceedMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        checkPrevious();

        prevFilter.onMessageReceived(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> proceedSessionWrite(
        GridNioSession ses,
        Object msg,
        boolean fut
    ) throws IgniteCheckedException {
        checkNext();

        return nextFilter.onSessionWrite(ses, msg, fut);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> proceedSessionClose(GridNioSession ses) throws IgniteCheckedException {
        checkNext();

        return nextFilter.onSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void proceedSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious();

        prevFilter.onSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void proceedSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious();

        prevFilter.onSessionWriteTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> proceedPauseReads(GridNioSession ses) throws IgniteCheckedException {
        checkNext();

        return nextFilter.onPauseReads(ses);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> proceedResumeReads(GridNioSession ses) throws IgniteCheckedException {
        checkNext();

        return nextFilter.onResumeReads(ses);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onPauseReads(GridNioSession ses) throws IgniteCheckedException {
        return proceedPauseReads(ses);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onResumeReads(GridNioSession ses) throws IgniteCheckedException {
        return proceedResumeReads(ses);
    }

    /**
     * Checks that previous filter is set.
     *
     * @throws GridNioException If previous filter is not set.
     */
    private void checkPrevious() throws GridNioException {
        if (prevFilter == null)
            throw new GridNioException("Failed to proceed with filter call since previous filter is not set " +
                "(do you use filter outside the filter chain?): " + getClass().getName());
    }

    /**
     * Checks that next filter is set.
     *
     * @throws GridNioException If next filter is not set.
     */
    private void checkNext() throws GridNioException {
        if (nextFilter == null)
            throw new GridNioException("Failed to proceed with filter call since previous filter is not set " +
                "(do you use filter outside the filter chain?): " + getClass().getName());
    }
}
