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

package org.apache.ignite.internal.schema;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SortedIndexColumn;

/**
 * Primary key index.
 */
public class PrimaryIndexImpl extends SortedIndexImpl implements PrimaryIndex {
    /** Affinity columns. */
    private final List<String> affCols;

    /**
     * Constructor.
     *
     * @param cols Index columns.
     * @param affCols Affinity columns.
     */
    public PrimaryIndexImpl(List<SortedIndexColumn> cols, List<String> affCols) {
        super(PrimaryIndex.PRIMARY_KEY_INDEX_NAME, cols, true);
        this.affCols = Collections.unmodifiableList(affCols);
    }

    /** {@inheritDoc} */
    @Override public List<String> affinityColumns() {
        return affCols;
    }

    /** {@inheritDoc} */
    @Override public String type() {
        return "PK";
    }
}
