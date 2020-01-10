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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Iterator;

/**
 * Scan node.
 */
public class ScanNode extends AbstractNode<Object[]> implements SingleNode<Object[]> {
    /** */
    private final Iterable<Object[]> source;

    /** */
    private Iterator<Object[]> it;

    /** */
    private Object row;

    /**
     * @param ctx Execution context.
     * @param source Source.
     */
    public ScanNode(ExecutionContext ctx, Iterable<Object[]> source) {
        super(ctx);

        this.source = source;
    }

    /** {@inheritDoc} */
    @Override public void request() {
        if (context().cancelled()
            || row == EndMarker.INSTANCE
            || row != null && !target().push((Object[]) row))
            return;

        row = null;

        if (it == null)
            it = source.iterator();

        while (it.hasNext()) {
            if (context().cancelled()) {
                it = null;
                row = null;

                return;
            }

            row = it.next();

            if (!target().push((Object[]) row))
                return;

            row = null;
        }

        row = EndMarker.INSTANCE;
        target().end();
    }

    /** {@inheritDoc} */
    @Override public Sink<Object[]> sink(int idx) {
        throw new AssertionError();
    }
}
