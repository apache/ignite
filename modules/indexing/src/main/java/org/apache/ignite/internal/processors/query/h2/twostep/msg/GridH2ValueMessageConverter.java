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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.plugin.extensions.communication.MessageConverter;
import org.h2.value.Value;

/**
 * Message converter.
 */
public class GridH2ValueMessageConverter implements MessageConverter<Value, GridH2ValueMessage> {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public GridH2ValueMessageConverter(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public GridH2ValueMessage convertOnWrite(Value val) {
        try {
            return H2Utils.toMessage(ctx, val);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to convert H2 value to message: " + val, e);
        }
    }

    /** {@inheritDoc} */
    @Override public Value convertOnRead(GridH2ValueMessage msg) {
        try {
            return msg.value();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to convert message to H2 value: " + msg, e);
        }
    }
}
