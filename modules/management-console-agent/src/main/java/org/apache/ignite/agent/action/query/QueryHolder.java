/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.action.query;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Query holder.
 */
public class QueryHolder implements AutoCloseable {
    /** Query ID. */
    private final String qryId;

    /** Cursors. */
    private final Map<String, CursorHolder> cursors = new HashMap<>();

    /** Cancel hook. */
    private final GridQueryCancel cancelHook = new GridQueryCancel();

    /** Is accessed. */
    private boolean isAccessed;

    /**
     * @param qryId Query ID.
     */
    public QueryHolder(String qryId) {
        this.qryId = qryId;
    }

    /**
     * @return Query ID.
     */
    public String queryId() {
        return qryId;
    }

    /**
     * @param cursorHolder Cursor holder.
     */
    public void addCursor(String cursorId, CursorHolder cursorHolder) {
        cursors.putIfAbsent(cursorId, cursorHolder);
    }

    /**
     * @return Query cancel hook.
     */
    public GridQueryCancel cancelHook() {
        return cancelHook;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cancelHook.cancel();

        cursors.keySet().forEach(this::closeCursor);
    }

    /**
     * @param cursorId Cursor ID.
     */
    public CursorHolder cursor(String cursorId) {
        return cursors.get(cursorId);
    }

    /**
     * @param cursorId Cursor ID.
     */
    public void closeCursor(String cursorId) {
        CursorHolder cursor = cursor(cursorId);

        if (cursor != null) {
            U.closeQuiet(cursor);

            cursors.remove(cursorId);
        }
    }

    /**
     * @return @{code true} if holder was accessed.
     */
    public boolean accessed() {
        return isAccessed;
    }

    /**
     * @param accessed Accessed.
     */
    public QueryHolder accessed(boolean accessed) {
        isAccessed = accessed;

        return this;
    }
}
