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

package org.apache.ignite.internal.processors.query.h2.index.keys;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.keys.AbstractDateIndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.h2.value.ValueDate;

/** */
public class DateIndexKey extends AbstractDateIndexKey {
    /** */
    private final ValueDate date;

    /** */
    public DateIndexKey(Object obj) {
        try {
            date = (ValueDate) H2Utils.wrap(null, obj, type());

        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to convert object to DateIndexKey.", e);
        }
    }

    /** */
    public static DateIndexKey fromDateValue(long dateVal) {
        return new DateIndexKey(ValueDate.fromDateValue(dateVal));
    }

    /** */
    private DateIndexKey(ValueDate date) {
        this.date = date;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return date.getDate();
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o, IndexKeyTypeSettings keySettings) {
        return date.compareTo(((DateIndexKey)o).date, null);
    }

    /** {@inheritDoc} */
    @Override public long dateValue() {
        return date.getDateValue();
    }
}
