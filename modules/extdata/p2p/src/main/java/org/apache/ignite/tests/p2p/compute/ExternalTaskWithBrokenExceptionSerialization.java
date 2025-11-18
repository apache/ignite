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

package org.apache.ignite.tests.p2p.compute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** */
public class ExternalTaskWithBrokenExceptionSerialization extends ComputeTaskAdapter<Object, Object> {
    /** Message 1. */
    private static final String JOB_EXEC_MSG = "Error occurred while executing the job step";

    /** Message 2. */
    private static final String SER_MSG = "Error occurred on serialization step";

    /** */
    private final boolean isSerializationBroken;

    /** */
    public ExternalTaskWithBrokenExceptionSerialization(boolean isSerializationBroken) {
        this.isSerializationBroken = isSerializationBroken;
    }

    /** {@inheritDoc} */
    @Override @NotNull public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
        return subgrid.stream().filter(g -> !g.isClient()).collect(Collectors.toMap(ignored -> job(), srv -> srv));
    }

    /** {@inheritDoc} */
    @Override public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }

    /** */
    private ComputeJobAdapter job() {
        return new ComputeJobAdapter() {
            @Override public Object execute() throws IgniteException {
                throw new ExternalizableExceptionWothBrokenSerialization(JOB_EXEC_MSG, true, isSerializationBroken);
            }
        };
    }

    /** Custom {@link Externalizable} Exception */
    private static class ExternalizableExceptionWothBrokenSerialization extends IgniteException implements Externalizable {
        /** */
        private boolean isBroken;

        /** */
        private boolean isSerializationBroken;

        /** */
        public ExternalizableExceptionWothBrokenSerialization() {
            // No-op.
        }

        /** */
        public ExternalizableExceptionWothBrokenSerialization(String msg, boolean isBroken, boolean isSerializationBroken) {
            super(msg);

            this.isBroken = isBroken;
            this.isSerializationBroken = isSerializationBroken;
        }

        /** */
        public boolean isBroken() {
            return isBroken;
        }

        /** */
        public void setBroken(boolean broken) {
            isBroken = broken;
        }

        /** */
        public boolean isSerializationBroken() {
            return isSerializationBroken;
        }

        /** */
        public void setSerializationBroken(boolean serializationBroken) {
            isSerializationBroken = serializationBroken;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            if (isBroken() && isSerializationBroken())
                throw new ExternalizableExceptionWothBrokenSerialization(SER_MSG, false, false);

            out.writeBoolean(isBroken());
            out.writeBoolean(isSerializationBroken());
            out.writeObject(getMessage());
            out.writeObject(getStackTrace());
            out.writeObject(getCause());
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setBroken(in.readBoolean());
            setSerializationBroken(in.readBoolean());

            if (isBroken() && !isSerializationBroken())
                throw new ExternalizableExceptionWothBrokenSerialization(SER_MSG, false, false);

            String msg = (String)in.readObject();

            try {
                Field detailMsg = Throwable.class.getDeclaredField("detailMessage");

                detailMsg.setAccessible(true);
                detailMsg.set(this, msg);
            }
            catch (Exception ignored) {
                // No-op.
            }

            setStackTrace((StackTraceElement[])in.readObject());

            Throwable cause = (Throwable)in.readObject();

            if (cause != null)
                initCause(cause);
        }
    }
}
