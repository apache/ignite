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
    /** */
    private static final String EX_MSG = "Message from Exception";

    /** */
    private static final String BROKEN_EX_MSG = "Exception occurred on serialization step";

    /** */
    private final boolean isWriteBroken;

    /** */
    public ExternalTaskWithBrokenExceptionSerialization(boolean isWriteBroken) {
        this.isWriteBroken = isWriteBroken;
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
                throw new ExternalizableExceptionWithBrokenSerialization(EX_MSG, isWriteBroken);
            }
        };
    }

    /** Custom {@link Externalizable} Exception */
    public static class ExternalizableExceptionWithBrokenSerialization extends IgniteException implements Externalizable {
        /** */
        private boolean isWriteBroken;

        /** */
        public ExternalizableExceptionWithBrokenSerialization() {
            // No-op.
        }

        /** */
        public ExternalizableExceptionWithBrokenSerialization(String msg, boolean isWriteBroken) {
            super(msg);

            this.isWriteBroken = isWriteBroken;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            if (isWriteBroken)
                throw new IgniteException(BROKEN_EX_MSG);

            out.writeBoolean(false);
            out.writeObject(getMessage());
            out.writeObject(getStackTrace());
            out.writeObject(getCause());
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            isWriteBroken = in.readBoolean();

            if (!isWriteBroken)
                throw new IgniteException(BROKEN_EX_MSG);

            String msg = (String)in.readObject();

            setMessage(msg);

            setStackTrace((StackTraceElement[])in.readObject());

            Throwable cause = (Throwable)in.readObject();

            if (cause != null)
                initCause(cause);
        }

        /** */
        private void setMessage(String msg) {
            try {
                Field detailMsg = Throwable.class.getDeclaredField("detailMessage");

                detailMsg.setAccessible(true);
                detailMsg.set(this, msg);
            }
            catch (Exception e) {
                throw new RuntimeException("Unexpected deserialization exception is caught!", e);
            }
        }
    }
}
