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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.cache.query.FieldsQueryCursor;

/**
 * Thin client query cursor.
 */
class ClientFieldsQueryCursor<T> extends ClientQueryCursor<T> implements FieldsQueryCursor<T> {
    /** Constructor. */
    ClientFieldsQueryCursor(FieldsQueryPager<T> pager) {
        super(pager);
    }

    /** {@inheritDoc} */
    @Override public String getFieldName(int idx) {
        return ((FieldsQueryPager<T>)getPager()).getFieldNames().get(idx);
    }

    /** {@inheritDoc} */
    @Override public int getColumnsCount() {
        return ((FieldsQueryPager<T>)getPager()).getFieldNames().size();
    }
}
