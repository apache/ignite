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

package org.apache.ignite.internal.processors.query.h2.affinity.join;

import org.apache.ignite.internal.util.typedef.F;

/**
 * Join condition.
 */
public class PartitionJoinCondition {
    /** Left alias. */
    private final String leftAlias;

    /** Right alias. */
    private final String rightAlias;

    /** Left column name. */
    private final String leftCol;

    /** Right column name. */
    private final String rightCol;

    /** Whether this is LEFT OUTER JOIN. */
    private final boolean left;

    /**
     * Constructor.
     *
     * @param leftAlias Left alias.
     * @param rightAlias Right alias.
     * @param leftCol Left column name.
     * @param rightCol Right column name.
     * @param left Left join flag.
     */
    public PartitionJoinCondition(String leftAlias, String rightAlias, String leftCol, String rightCol, boolean left) {
        this.leftAlias = leftAlias;
        this.rightAlias = rightAlias;
        this.leftCol = leftCol;
        this.rightCol = rightCol;
        this.left = left;
    }

    /**
     * Left alias.
     */
    public String leftAlias() {
        return leftAlias;
    }

    /**
     * Right alias.
     */
    public String rightAlias() {
        return rightAlias;
    }

    /**
     * @return Left column.
     */
    public String leftColumn() {
        return leftCol;
    }

    /**
     * @return Right column.
     */
    public String rightColumn() {
        return rightCol;
    }

    /**
     * @return Whether this is left join.
     */
    public boolean left() {
        return left;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = leftAlias.hashCode();

        res = 31 * res + rightAlias.hashCode();
        res = 31 * res + leftCol.hashCode();
        res = 31 * res + rightCol.hashCode();
        res = 31 * res + Boolean.hashCode(left);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj instanceof PartitionJoinCondition) {
            PartitionJoinCondition other = (PartitionJoinCondition)obj;

            return F.eq(leftAlias, other.leftAlias) && F.eq(rightAlias, other.rightAlias) &&
                F.eq(leftCol, other.leftCol) && F.eq(rightCol, other.rightCol) && F.eq(left, other.left);
        }

        return false;
    }
}
