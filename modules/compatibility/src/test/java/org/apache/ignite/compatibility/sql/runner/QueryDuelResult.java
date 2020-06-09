/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.runner;

import java.util.List;
import java.util.Objects;

/**
 * The result of query duel.
 */
public class QueryDuelResult {
    /** */
    private final String qry;

    /** */
    private final List<Long> oldQryExecTimes;

    /** */
    private final List<Long> newQryExecTimes;

    /** */
    private final List<Exception> errors;

    /** */
    public QueryDuelResult(String qry, List<Long> oldQryExecTimes, List<Long> newQryExecTimes, List<Exception> errors) {
        this.qry = qry;
        this.oldQryExecTimes = oldQryExecTimes;
        this.newQryExecTimes = newQryExecTimes;
        this.errors = errors;
    }

    /** */
    public String query() {
        return qry;
    }

    /** */
    public List<Long> oldExecutionTime() {
        return oldQryExecTimes;
    }

    /** */
    public List<Long> newExecutionTime() {
        return newQryExecTimes;
    }

    /** */
    public List<Exception> error() {
        return errors;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueryDuelResult res = (QueryDuelResult)o;
        return Objects.equals(qry, res.qry);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(qry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "QueryDuelResult{" +
            "qry='" + qry + '\'' +
            ", oldQryExecTimes=" + oldQryExecTimes +
            ", newQryExecTimes=" + newQryExecTimes +
            ", err=" + errors +
            '}';
    }
}
