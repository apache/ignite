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
import org.h2.value.ValueNull;

/**
 * Message for {@link Value#NULL}.
 */
public class GridH2Null extends GridH2ValueMessage {
    /** */
    public static GridH2Null INSTANCE = new GridH2Null();

    /** Dummy field to use codegen serializer. */
    @Order(0)
    byte dummy;

    /**
     * Disallow new instance creation.
     */
    private GridH2Null() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) {
        return ValueNull.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -4;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "NULL";
    }
}
