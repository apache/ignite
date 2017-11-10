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

package org.apache.ignite.ml.trees.nodes;

import org.apache.ignite.ml.math.Vector;

import java.util.BitSet;

/**
 * Split node by categorical feature.
 */
public class CategoricalSplitNode extends SplitNode {
    /** Bitset specifying which categories belong to left subregion. */
    private final BitSet bs;

    /**
     * Construct categorical split node.
     *
     * @param featureIdx Index of feature by which split is done.
     * @param bs Bitset specifying which categories go to the left subtree.
     */
    public CategoricalSplitNode(int featureIdx, BitSet bs) {
        super(featureIdx);
        this.bs = bs;
    }

    /** {@inheritDoc} */
    @Override public boolean goLeft(Vector v) {
        return bs.get((int)v.getX(featureIdx));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CategoricalSplitNode [bs=" + bs + ']';
    }
}
