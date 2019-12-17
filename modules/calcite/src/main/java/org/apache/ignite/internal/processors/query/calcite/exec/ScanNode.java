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
import java.util.List;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-12449
 */
public class ScanNode implements SingleNode<Object[]> {
    /** */
    private static final Object[] END = new Object[0];

    /** */
    private final Sink<Object[]> target;

    /** */
    private final Iterable<Object[]> source;

    /** */
    private Iterator<Object[]> it;

    /** */
    private Object[] row;

    /**
     * @param target Target.
     * @param source Source.
     */
    public ScanNode(Sink<Object[]> target, Iterable<Object[]> source) {
        this.target = target;
        this.source = source;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override public void sources(List<Source> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Sink<Object[]> sink(int idx) {
        throw new UnsupportedOperationException();
    }
}
