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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.h2.value.*;

import java.util.*;

/**
 * Page result.
 */
public class GridResultPage {
    /** */
    private final UUID src;

    /** */
    protected final GridQueryNextPageResponse res;

    /** */
    private final Collection<Value[]> rows;

    /** */
    private final boolean last;

    /**
     * @param src Source.
     * @param res Response.
     * @param last If this is the globally last page.
     */
    @SuppressWarnings("unchecked")
    public GridResultPage(UUID src, GridQueryNextPageResponse res, boolean last) {
        assert src != null;

        this.src = src;
        this.res = res;
        this.last = last;

        if (last)
            assert res == null : "The last page must be dummy.";

        // res == null means that it is a terminating dummy page for the given source node ID.
        if (res != null) {
            Object plainRows = res.plainRows();

            rows = plainRows != null ? (Collection<Value[]>)plainRows : GridMapQueryExecutor.unmarshallRows(res.rows());
        }
        else
            rows = Collections.emptySet();
    }

    /**
     * @return {@code true} If this is a dummy last page for all the sources.
     */
    public boolean isLast() {
        return last;
    }

    /**
     * @return Rows.
     */
    public Collection<Value[]> rows() {
        return rows;
    }

    /**
     * @return Result source node ID.
     */
    public UUID source() {
        return src;
    }

    /**
     * @return Response.
     */
    public GridQueryNextPageResponse response() {
        return res;
    }

    /**
     * Request next page.
     */
    public void fetchNextPage() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridResultPage.class, this);
    }
}
