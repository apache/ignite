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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.function.Function;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class ProjectNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Upstream<Object[]> {
    /** */
    private final Function<Object[], Object[]> projection;

    /**
     * @param ctx Execution context.
     * @param projection Projection.
     */
    public ProjectNode(ExecutionContext ctx, Function<Object[], Object[]> projection) {
        super(ctx);

        this.projection = projection;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;

        F.first(sources).request(rowsCount);
    }

    /** {@inheritDoc} */
    @Override public void push(Object[] row) {
        checkThread();

        assert upstream != null;

        try {
            upstream.push(projection.apply(row));
        }
        catch (Throwable e) {
            upstream.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert upstream != null;

        upstream.end();
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable e) {
        checkThread();

        assert upstream != null;

        upstream.onError(e);
    }

    /** {@inheritDoc} */
    @Override protected Upstream<Object[]> requestUpstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }
}
