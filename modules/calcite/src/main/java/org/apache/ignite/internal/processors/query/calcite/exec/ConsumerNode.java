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

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-12449
 */
public class ConsumerNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Sink<Object[]>, Iterator<Object[]> {
    /** */
    private static final int DEFAULT_BUFFER_SIZE = 1000;

    /** */
    private static final Object[] END = new Object[0];

    /** */
    private ArrayDeque<Object[]> buff;

    /** */
    public ConsumerNode() {
        super(Sink.noOp());

        buff = new ArrayDeque<>(DEFAULT_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public Sink<Object[]> sink(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean push(Object[] row) {
        if (buff.size() == DEFAULT_BUFFER_SIZE)
            return false;

        buff.add(row);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void end() {
        buff.add(END);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (buff.isEmpty())
            signal();

        return buff.peek() != END;
    }

    /** {@inheritDoc} */
    @Override public Object[] next() {
        if (buff.isEmpty())
            signal();

        if(!hasNext())
            throw new NoSuchElementException();

        return Objects.requireNonNull(buff.poll());
    }
}
