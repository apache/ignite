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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;

/**
 * Special field set iterator based on database result set.
 */
public class H2FieldsIterator extends H2ResultSetIterator<List<?>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private transient MvccQueryTracker mvccTracker;

    /** Detached connection. */
    private final ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable detachedConn;

    /**
     * @param data Data.
     * @param mvccTracker Mvcc tracker.
     * @param forUpdate {@code SELECT FOR UPDATE} flag.
     * @param detachedConn Detached connection.
     * @throws IgniteCheckedException If failed.
     */
    public H2FieldsIterator(ResultSet data, MvccQueryTracker mvccTracker, boolean forUpdate,
        ThreadLocalObjectPool<H2ConnectionWrapper>.Reusable detachedConn)
        throws IgniteCheckedException {
        super(data, forUpdate);

        this.mvccTracker = mvccTracker;
        this.detachedConn = detachedConn;
    }

    /** {@inheritDoc} */
    @Override protected List<?> createRow() {
        List<Object> res = new ArrayList<>(row.length);

        Collections.addAll(res, row);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void onClose() throws IgniteCheckedException {
        try {
            super.onClose();
        }
        finally {
            if (detachedConn != null)
                detachedConn.recycle();

            if (mvccTracker != null)
                mvccTracker.onDone();
        }
    }
}
