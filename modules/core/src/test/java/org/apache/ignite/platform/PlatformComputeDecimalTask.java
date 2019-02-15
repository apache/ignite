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

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"ConstantConditions"})
public class PlatformComputeDecimalTask extends ComputeTaskAdapter<Object[], BigDecimal> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object[] arg) {
        return Collections.singletonMap(new DecimalJob((BigDecimal)arg[0], (String)arg[1]), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public BigDecimal reduce(List<ComputeJobResult> results) {
        ComputeJobResult res = results.get(0);

        if (res.getException() != null)
            throw res.getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class DecimalJob extends ComputeJobAdapter implements Externalizable {
        /** Value. */
        private BigDecimal val;

        /** Value as string. */
        private String valStr;

        /**
         * Constructor.
         */
        public DecimalJob() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         * @param valStr Value as string.
         */
        private DecimalJob(BigDecimal val, String valStr) {
            this.val = val;
            this.valStr = valStr;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            BigDecimal exp = new BigDecimal(valStr.replace(',', '.'));

            if (val != null && !exp.equals(val))
                throw new IgniteException("Actual=" + val);

            return exp;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(val);
            out.writeObject(valStr);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = (BigDecimal)in.readObject();
            valStr = (String)in.readObject();
        }
    }
}