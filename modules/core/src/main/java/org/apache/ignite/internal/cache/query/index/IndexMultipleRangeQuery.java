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

package org.apache.ignite.internal.cache.query.index;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents ordered list of independent index range queries.
 */
class IndexMultipleRangeQuery {
    /** Ordered list of index ranges queries. */
    private final List<IndexSingleRangeQuery> queries = new ArrayList<>();

    /** */
    private final int critSize;

    /** */
    private final int idxRowSize;

    /** */
    IndexMultipleRangeQuery(int idxRowSize, int critSize) {
        this.critSize = critSize;
        this.idxRowSize = idxRowSize;
    }

    /** */
    public List<IndexSingleRangeQuery> queries() {
        return queries;
    }

    /** */
    int critSize() {
        return critSize;
    }

    /**
     * Adds condition. In case of multiple queries it adds to every query.
     */
    void addIndexKeyCondition(int i, IndexKeyQueryCondition cond) {
        if (i == 0)
            addFirstIndexKeyCondition(cond);
        else {
            for (IndexSingleRangeQuery qry: queries)
                qry.addCondition(cond, i);
        }
    }

    /**
     * Add first condition. If it contains IN clause then split query to multiple index ranges joint with OR:
     *
     * IN(A, B) and GT(C) = (EQ(A) and GT(C)) or (EQ(B) and GT(C)).
     *
     * It ignores RANGE criterion if IN is specified. Intersection of them was already checked on prepare query phase.
     */
    private void addFirstIndexKeyCondition(IndexKeyQueryCondition keyCond) {
        if (keyCond.inVals() != null) {
            for (IndexKeyQueryCondition cond: keyCond.splitInToRanges()) {
                IndexSingleRangeQuery q = new IndexSingleRangeQuery(idxRowSize, critSize);

                q.addCondition(cond, 0);

                queries.add(q);
            }
        }
        else {
            IndexSingleRangeQuery qry = new IndexSingleRangeQuery(idxRowSize, critSize);

            qry.addCondition(keyCond, 0);

            queries.add(qry);
        }
    }
}
