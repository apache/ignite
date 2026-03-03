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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.h2.value.Value;
import org.h2.value.ValueArray;

import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.fillArray;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.toMessage;

/**
 * H2 Array.
 */
public class GridH2Array extends GridH2ValueMessage {
    /** */
    @Order(0)
    Collection<Message> x;

    /**
     *
     */
    public GridH2Array() {
        // No-op.
    }

    /**
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Array(Value val) throws IgniteCheckedException {
        assert val.getType() == Value.ARRAY : val.getType();

        ValueArray arr = (ValueArray)val;

        x = new ArrayList<>(arr.getList().length);

        for (Value v : arr.getList())
            x.add(toMessage(v));
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) throws IgniteCheckedException {
        return ValueArray.get(fillArray(x.iterator(), new Value[x.size()], ctx));
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -18;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(x);
    }
}
