/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteInClosure;

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
    @Override public String toString() {
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
        boolean fut,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteCheckedException {
        checkNext();

        return nextFilter.onSessionWrite(ses, msg, fut, ackC);
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
