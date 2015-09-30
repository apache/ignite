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

import java.nio.ByteBuffer;
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;

/**
 * SQL Row message.
 */
public class GridH2RowMessage implements Message {
    /** */
    @GridDirectCollection(Message.class)
    private List<GridH2ValueMessage> vals;

    /**
     * @return Values of row.
     */
    public List<GridH2ValueMessage> values() {
        return vals;
    }

    /**
     * @param vals Values of row.
     */
    public void values(List<GridH2ValueMessage> vals) {
        this.vals = vals;
    }

    /**
     * @param ctx Kernal context.
     * @return Row.
     */
    public GridH2Row row(GridKernalContext ctx) {
        Value[] v = new Value[vals.size()];

        try {
            GridH2ValueMessageFactory.fillArray(vals.iterator(), v, ctx);
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }

        return new GridH2Row(v);
    }

    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return false;
    }

    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }

    @Override public byte directType() {
        return -25;
    }

    @Override public byte fieldsCount() {
        return 0;
    }
}
