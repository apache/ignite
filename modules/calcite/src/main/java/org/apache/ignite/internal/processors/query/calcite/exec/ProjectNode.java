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

import java.util.function.Function;

/**
 *
 */
public class ProjectNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Sink<Object[]> {
    private final Function<Object[], Object[]> projection;

    public ProjectNode(Sink<Object[]> target, Function<Object[], Object[]> projection) {
        super(target);

        this.projection = projection;
    }

    @Override public Sink<Object[]> sink(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    @Override public boolean push(Object[] row) {
        return target.push(projection.apply(row));
    }

    @Override public void end() {
        target.end();
    }
}
