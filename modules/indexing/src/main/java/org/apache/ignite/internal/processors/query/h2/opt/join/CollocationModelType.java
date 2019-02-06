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

package org.apache.ignite.internal.processors.query.h2.opt.join;

/**
 * Collocation type.
 */
public enum CollocationModelType {
    /** */
    PARTITIONED_COLLOCATED(true, true),

    /** */
    PARTITIONED_NOT_COLLOCATED(true, false),

    /** */
    REPLICATED(false, true);

    /** */
    private final boolean partitioned;

    /** */
    private final boolean collocated;

    /**
     * @param partitioned Partitioned.
     * @param collocated Collocated.
     */
    CollocationModelType(boolean partitioned, boolean collocated) {
        this.partitioned = partitioned;
        this.collocated = collocated;
    }

    /**
     * @return {@code true} If partitioned.
     */
    public boolean isPartitioned() {
        return partitioned;
    }

    /**
     * @return {@code true} If collocated.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @param partitioned Partitioned.
     * @param collocated Collocated.
     * @return Type.
     */
    public static CollocationModelType of(boolean partitioned, boolean collocated) {
        if (collocated)
            return partitioned ? PARTITIONED_COLLOCATED : REPLICATED;

        assert partitioned;

        return PARTITIONED_NOT_COLLOCATED;
    }
}
