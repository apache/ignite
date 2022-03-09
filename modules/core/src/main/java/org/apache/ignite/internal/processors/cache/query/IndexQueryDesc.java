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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.IndexQueryCriterion;

/** Internal representation of {@link IndexQuery}. */
public class IndexQueryDesc implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final List<IndexQueryCriterion> criteria;

    /** */
    private final String idxName;

    /** */
    private final String valType;

    /** */
    public IndexQueryDesc(List<IndexQueryCriterion> criteria, String idxName, String valType) {
        this.criteria = criteria;
        this.idxName = idxName;
        this.valType = valType;
    }

    /** */
    public List<IndexQueryCriterion> criteria() {
        return criteria;
    }

    /** */
    public String idxName() {
        return idxName;
    }

    /** */
    public String valType() {
        return valType;
    }

    /** */
    @Override public String toString() {
        List<String> fields = criteria == null ? null : criteria.stream().map(IndexQueryCriterion::field).collect(Collectors.toList());

        return "IndexQuery[" +
            "idxName=" + idxName + ", " +
            "valType=" + valType + ", " +
            "fields=" + fields + "]";
    }
}
