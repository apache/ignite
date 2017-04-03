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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class CacheLockCandidatesList implements CacheLockCandidates {
    /** */
    @GridToStringInclude
    private List<GridCacheMvccCandidate> list = new ArrayList<>();

    /**
     * @param cand Candidate to add.
     */
    void add(GridCacheMvccCandidate cand) {
        assert !hasCandidate(cand.version()) : cand;

        list.add(cand);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate candidate(int idx) {
        assert idx < list.size() : idx;

        return list.get(idx);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return list.size();
    }

    /** {@inheritDoc} */
    @Override public boolean hasCandidate(GridCacheVersion ver) {
        for (int i = 0; i < list.size(); i++) {
            GridCacheMvccCandidate cand = list.get(i);

            if (cand.version().equals(ver))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheLockCandidatesList.class, this);
    }
}
