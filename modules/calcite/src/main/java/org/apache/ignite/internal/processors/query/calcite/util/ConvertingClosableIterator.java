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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/**
 *
 */
class ConvertingClosableIterator<Row> implements Iterator<List<?>>, AutoCloseable {
    /** */
    private final Iterator<Row> it;

    /** Row handler. */
    private final RowHandler<Row> rowHnd;

    /** */
    public ConvertingClosableIterator(Iterator<Row> it, ExecutionContext<Row> ectx) {
        this.it = it;
        this.rowHnd = ectx.planningContext().rowHandler();
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean hasNext() {
        return it.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override public List<Object> next() {
        Row next = it.next();

        int rowSize = rowHnd.fieldsCount(next);

        List<Object> res = new ArrayList<>(rowSize);

        for (int i = 0; i < rowSize; i++)
            res.add(rowHnd.get(i, next));

        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void close() throws Exception {
        if (it instanceof AutoCloseable)
            ((AutoCloseable) it).close();
    }
}
