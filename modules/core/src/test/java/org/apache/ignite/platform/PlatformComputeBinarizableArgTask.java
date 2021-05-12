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

package org.apache.ignite.platform;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task working with binarizable argument.
 */
public class PlatformComputeBinarizableArgTask extends ComputeTaskAdapter<Object, Integer> {
    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
        return Collections.singletonMap(new BinarizableArgJob(arg), F.first(subgrid));
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer reduce(List<ComputeJobResult> results) {
        ComputeJobResult res = results.get(0);

        if (res.getException() != null)
            throw res.getException();
        else
            return results.get(0).getData();
    }

    /**
     * Job.
     */
    private static class BinarizableArgJob extends ComputeJobAdapter implements Externalizable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Argument. */
        private Object arg;

        /**
         * Constructor.
         */
        public BinarizableArgJob() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param arg Argument.
         */
        private BinarizableArgJob(Object arg) {
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            BinaryObjectEx arg0 = ((BinaryObjectEx)arg);

            BinaryType meta = ignite.binary().type(arg0.typeId());

            if (meta == null)
                throw new IgniteException("Metadata doesn't exist.");

            if (meta.fieldNames() == null || !meta.fieldNames().contains("Field"))
                throw new IgniteException("Field metadata doesn't exist.");

            if (!F.eq("int", meta.fieldTypeName("Field")))
                throw new IgniteException("Invalid field type: " + meta.fieldTypeName("Field"));

            if (meta.affinityKeyFieldName() != null)
                throw new IgniteException("Unexpected affinity key: " + meta.affinityKeyFieldName());

            return arg0.field("field");
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(arg);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            arg = in.readObject();
        }
    }
}
