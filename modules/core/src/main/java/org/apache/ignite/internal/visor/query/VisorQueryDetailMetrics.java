/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for cache query detail metrics.
 */
public class VisorQueryDetailMetrics extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type. */
    private String qryType;

    /** Textual query representation. */
    private String qry;

    /** Cache name. */
    private String cache;

    /** Number of executions. */
    private int execs;

    /** Number of completions executions. */
    private int completions;

    /** Number of failures. */
    private int failures;

    /** Minimum time of execution. */
    private long minTime;

    /** Maximum time of execution. */
    private long maxTime;

    /** Average time of execution. */
    private double avgTime;

    /** Sum of execution time of completions time. */
    private long totalTime;

    /** Sum of execution time of completions time. */
    private long lastStartTime;

    /**
     * Default constructor
     */
    public VisorQueryDetailMetrics() {
        // No-op.
    }

    /**
     * @param m Cache query metrics.
     */
    public VisorQueryDetailMetrics(QueryDetailMetrics m) {
        qryType = m.queryType();
        qry = m.query();
        cache = m.cache();

        execs = m.executions();
        completions = m.completions();
        failures = m.failures();

        minTime = m.minimumTime();
        maxTime = m.maximumTime();
        avgTime = m.averageTime();
        totalTime = m.totalTime();
        lastStartTime = m.lastStartTime();
    }

    /**
     * @return Query type
     */
    public String getQueryType() {
        return qryType;
    }

    /**
     * @return Query type
     */
    public String getQuery() {
        return qry;
    }

    /**
     * @return Cache name where query was executed.
     */
    public String getCache() {
        return cache;
    }

    /**
     * @return Number of executions.
     */
    public int getExecutions() {
        return execs;
    }

    /**
     * @return Number of completed executions.
     */
    public int getCompletions() {
        return completions;
    }

    /**
     * @return Total number of times a query execution failed.
     */
    public int getFailures() {
        return failures;
    }

    /**
     * @return Minimum execution time of query.
     */
    public long getMinimumTime() {
        return minTime;
    }

    /**
     * @return Maximum execution time of query.
     */
    public long getMaximumTime() {
        return maxTime;
    }

    /**
     * @return Average execution time of query.
     */
    public double getAverageTime() {
        return avgTime;
    }

    /**
     * @return Total time of all query executions.
     */
    public long getTotalTime() {
        return totalTime;
    }

    /**
     * @return Latest time query was stared.
     */
    public long getLastStartTime() {
        return lastStartTime;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, qryType);
        U.writeString(out, qry);
        U.writeString(out, cache);
        out.writeInt(execs);
        out.writeInt(completions);
        out.writeInt(failures);
        out.writeLong(minTime);
        out.writeLong(maxTime);
        out.writeDouble(avgTime);
        out.writeLong(totalTime);
        out.writeLong(lastStartTime);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        qryType = U.readString(in);
        qry = U.readString(in);
        cache = U.readString(in);
        execs = in.readInt();
        completions = in.readInt();
        failures = in.readInt();
        minTime = in.readLong();
        maxTime = in.readLong();
        avgTime = in.readDouble();
        totalTime = in.readLong();
        lastStartTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryDetailMetrics.class, this);
    }
}
