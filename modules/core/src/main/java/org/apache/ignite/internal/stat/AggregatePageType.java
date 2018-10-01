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
 *
 */

package org.apache.ignite.internal.stat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Enumeration aggregated IO types.
 */
public enum AggregatePageType {
    /** Pages related to H2 INDEX. */
    INDEX(
        PageType.T_H2_REF_LEAF,
        PageType.T_H2_REF_INNER,
        PageType.T_H2_MVCC_REF_LEAF,
        PageType.T_H2_MVCC_REF_INNER,
        PageType.T_H2_EX_REF_LEAF,
        PageType.T_H2_EX_REF_INNER,
        PageType.T_H2_EX_REF_MVCC_LEAF,
        PageType.T_H2_EX_REF_MVCC_INNER,
        PageType.T_DATA_REF_INNER,
        PageType.T_DATA_REF_LEAF,
        PageType.T_CACHE_ID_AWARE_DATA_REF_INNER,
        PageType.T_CACHE_ID_AWARE_DATA_REF_LEAF,
        PageType.T_DATA_REF_MVCC_INNER,
        PageType.T_DATA_REF_MVCC_LEAF,
        PageType.T_CACHE_ID_DATA_REF_MVCC_INNER,
        PageType.T_CACHE_ID_DATA_REF_MVCC_LEAF
    ),

    /** Data pages. */
    DATA(
        PageType.T_DATA
    ),
    /** */
    OTHER;

    /** */
    private Set<PageType> includedTypes = new HashSet<>();

    /**
     * @param pageTypes Page types which should be aggregated.
     */
    AggregatePageType(PageType... pageTypes) {
        includedTypes.addAll(Arrays.asList(pageTypes));
    }

    /**
     * @return List of types included to aggregation.
     */
    public Set<PageType> includedTypes() {
        return includedTypes;
    }

    /**
     * @param pageType Page type which need to convert to aggregate type.
     * @return Aggregate page type for given page type.
     */
    static AggregatePageType aggregate(PageType pageType) {
        for (AggregatePageType type : values()) {
            if (type.includedTypes.contains(pageType))
                return type;
        }

        return OTHER;
    }
}

