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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Objects;

import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public class RunningFragment<Row> {
    /** Relation tree of the fragment is used to generate fragment human-readable description. */
    private final IgniteRel rootRel;

    /** */
    private final AbstractNode<Row> root;

    /** */
    private final ExecutionContext<Row> ectx;

    /** */
    public RunningFragment(
        IgniteRel rootRel,
        AbstractNode<Row> root,
        ExecutionContext<Row> ectx) {
        this.rootRel = rootRel;
        this.root = root;
        this.ectx = ectx;
    }

    /** */
    public ExecutionContext<Row> context() {
        return ectx;
    }

    /** */
    public AbstractNode<Row> root() {
        return root;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        RunningFragment<Row> fragment = (RunningFragment<Row>)o;

        return Objects.equals(ectx, fragment.ectx);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(ectx);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningFragment.class, this);
    }
}
