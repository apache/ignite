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

package org.apache.ignite.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Task that performs an SQL query and returns results.
 */
public class PlatformSqlQueryTask extends ComputeTaskAdapter<String, Object> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String arg) throws IgniteException {
        return Collections.singletonMap(new SqlQueryJob(arg), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
        ComputeJobResult res = results.get(0);

        if (res.getException() != null)
            throw res.getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class SqlQueryJob extends ComputeJobAdapter implements Externalizable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Argument. */
        private String arg;

        /**
         * Constructor.
         */
        public SqlQueryJob() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param arg Argument.
         */
        private SqlQueryJob(String arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            IgniteCache<Integer, PlatformComputeBinarizable> cache = ignite.cache("default");

            SqlQuery<Integer, PlatformComputeBinarizable> qry = new SqlQuery<>("PlatformComputeBinarizable", arg);

            List<Cache.Entry<Integer, PlatformComputeBinarizable>> qryRes = cache.query(qry).getAll();

            Collection<PlatformComputeBinarizable> res = new ArrayList<>(qryRes.size());

            for (Cache.Entry<Integer, PlatformComputeBinarizable> e : qryRes)
                res.add(e.getValue());

            return res;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(arg);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            arg = (String)in.readObject();
        }
    }
}
