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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.Order;
import org.h2.value.Value;
import org.h2.value.ValueTimestamp;

/**
 * H2 Timestamp.
 */
public class GridH2Timestamp extends GridH2ValueMessage {
    /** */
    @Order(0)
    long date;

    /** */
    @Order(1)
    long nanos;

    /**
     *
     */
    public GridH2Timestamp() {
        // No-op.
    }

    /**
     * @param val Value.
     */
    public GridH2Timestamp(Value val) {
        assert val.getType() == Value.TIMESTAMP : val.getType();

        ValueTimestamp t = (ValueTimestamp)val;

        date = t.getDateValue();
        nanos = t.getTimeNanos();
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        return ValueTimestamp.fromDateValueAndNanos(date, nanos);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -15;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return date + "_" + nanos;
    }
}
