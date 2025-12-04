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

package org.apache.ignite.internal.cache.query.index;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Represents ordering of rows within sorted index.
 */
public class OrderMessage implements Message {
    /** */
    @Order(0)
    private boolean nullsFirst;

    /** */
    @Order(value = 1, method = "ascending")
    private boolean asc;

    /** Empty constructor for {@link GridIoMessageFactory}. */
    public OrderMessage() {
        // No-op.
    }

    /** */
    public OrderMessage(SortOrder sortOrder, NullsOrder nullsOrder) {
        asc = sortOrder == SortOrder.ASC;
        nullsFirst = nullsOrder == NullsOrder.NULLS_FIRST;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 64;
    }

    /** */
    public boolean ascending() {
        return asc;
    }

    /** */
    public void ascending(boolean asc) {
        this.asc = asc;
    }

    /** */
    public boolean nullsFirst() {
        return nullsFirst;
    }

    /** */
    public void nullsFirst(boolean nullsFirst) {
        this.nullsFirst = nullsFirst;
    }

    /** */
    public SortOrder sortOrder() {
        return asc ? SortOrder.ASC : SortOrder.DESC;
    }

    /** */
    public NullsOrder nullsOrder() {
        return nullsFirst ? NullsOrder.NULLS_FIRST : NullsOrder.NULLS_LAST;
    }
}
