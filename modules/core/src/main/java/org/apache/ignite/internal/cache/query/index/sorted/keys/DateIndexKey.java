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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import java.time.LocalDate;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;

/** */
public class DateIndexKey implements IndexKey {
    /** */
    private final long dateVal;

    /** */
    public DateIndexKey(long dateVal) {
        this.dateVal = dateVal;
    }

    /** */
    public DateIndexKey(java.sql.Date date) {
        dateVal = DateTimeUtils.dateValueFromDate(date.getTime());
    }

    /** */
    public DateIndexKey(LocalDate date) {
        dateVal = DateTimeUtils.dateValue(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
    }

    /** {@inheritDoc} */
    @Override public Object getKey() {
        return dateVal;
    }

    /** {@inheritDoc} */
    @Override public int getType() {
        return IndexKeyTypes.DATE;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o, IndexKeyTypeSettings keySettings) {
        long okey = (long) o.getKey();

        return Long.compare(dateVal, okey);
    }
}
