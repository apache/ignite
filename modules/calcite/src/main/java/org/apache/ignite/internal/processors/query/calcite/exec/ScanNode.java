/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class ScanNode implements SingleNode<Object[]> {
    private static final Object[] END = new Object[0];

    /** */
    private final Sink<Object[]> target;
    private final Iterable<Object[]> source;

    private Iterator<Object[]> it;
    private Object[] row;

    public ScanNode(Sink<Object[]> target, Iterable<Object[]> source) {
        this.target = target;
        this.source = source;
    }

    @Override public void signal() {
        if (row == END)
            return;

        if (row != null && !target.push(row))
            return;

        row = null;

        if (it == null)
            it = source.iterator();

        while (it.hasNext()) {
            row = it.next();

            if (!target.push(row))
                return;
        }

        row = END;
        target.end();
    }

    @Override public void sources(List<Source> sources) {
        throw new UnsupportedOperationException();
    }

    @Override public Sink<Object[]> sink(int idx) {
        throw new UnsupportedOperationException();
    }
}
