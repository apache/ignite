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
package org.apache.ignite.internal.visor.verify;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class ValidateIndexesPartitionResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Update counter. */
    private final long updateCntr;

    /** Size. */
    private final long size;

    /** Is primary. */
    private final boolean isPrimary;

    /** Consistent id. */
    @GridToStringInclude
    private final Object consistentId;

    /** Issues. */
    @GridToStringExclude
    private final List<Issue> issues = new ArrayList<>(10);

    /**
     * @param updateCntr Update counter.
     * @param size Size.
     * @param isPrimary Is primary.
     * @param consistentId Consistent id.
     */
    public ValidateIndexesPartitionResult(long updateCntr, long size, boolean isPrimary, Object consistentId) {
        this.updateCntr = updateCntr;
        this.size = size;
        this.isPrimary = isPrimary;
        this.consistentId = consistentId;
    }

    /**
     *
     */
    public long updateCntr() {
        return updateCntr;
    }

    /**
     *
     */
    public long size() {
        return size;
    }

    /**
     *
     */
    public boolean primary() {
        return isPrimary;
    }

    /**
     *
     */
    public Object consistentId() {
        return consistentId;
    }

    /**
     *
     */
    public List<Issue> issues() {
        return issues;
    }

    /**
     * @param t Issue.
     * @return True if there are already enough issues.
     */
    public boolean reportIssue(Issue t) {
        if (issues.size() >= 10)
            return true;

        issues.add(t);

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ValidateIndexesPartitionResult.class, this);
    }

    /**
     *
     */
    public static class Issue {
        /** Key. */
        private final String key;

        /** Cache name. */
        private final String cacheName;

        /** Index name. */
        private final String idxName;

        /** T. */
        @GridToStringExclude
        private final Throwable t;

        /**
         * @param key Key.
         * @param cacheName Cache name.
         * @param idxName Index name.
         * @param t T.
         */
        public Issue(String key, String cacheName, String idxName, Throwable t) {
            this.key = key;
            this.cacheName = cacheName;
            this.idxName = idxName;
            this.t = t;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Issue.class, this) + ", " + t.getClass() + ": " + t.getMessage();
        }
    }
}
