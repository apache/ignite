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

    /**
     * @param data Data.
     * @param mvccTracker Mvcc tracker.
     * @param forUpdate {@code SELECT FOR UPDATE} flag.
     * @throws IgniteCheckedException If failed.
     */
    public H2FieldsIterator(ResultSet data, MvccQueryTracker mvccTracker, boolean forUpdate)
        throws IgniteCheckedException {
        super(data, forUpdate);

        this.mvccTracker = mvccTracker;
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
            if (mvccTracker != null)
                mvccTracker.onDone();
        }
    }
}
