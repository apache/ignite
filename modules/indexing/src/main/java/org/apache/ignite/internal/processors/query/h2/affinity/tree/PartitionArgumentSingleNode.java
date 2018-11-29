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

package org.apache.ignite.internal.processors.query.h2.affinity.tree;

/**
 * Pa
 */
public class PartitionArgumentSingleNode extends PartitionSingleNode {
    /** Index. */
    private final int idx;

    /**
     * Constructor.
     *
     * @param idx Index.
     */
    public PartitionArgumentSingleNode(int idx) {
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public int applySingle(PartitionResolver resolver, Object... args) {
        assert args != null;
        assert idx < args.length;

        return resolver.resolve(args[idx]);
    }

    /** {@inheritDoc} */
    @Override public boolean constant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return idx;
    }
}
