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

package org.apache.ignite.internal.processors.service;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Field;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class GridServiceExceptionPropagationTest extends GridCommonAbstractTest {
    /** */
    private static final String EX_BROKEN_SER_MSG = "Exception occurred on serialization step";

    /** */
    private static final String RETURNED_EX_BROKEN_SER_MSG = "See server logs for details";

    /** */
    private static final String EX_MSG = "Exception message";

    /** */
    private static final String SERVICE_NAME = "my-service";

    /** */
    private static final ExceptionThrower SERIAL_EX = ExceptionThrower.serializable();

    /** */
    private static final ExceptionThrower EXT_EX = ExceptionThrower.externalizable(false, false);

    /** */
    private static final ExceptionThrower EX_WITH_BROKEN_SER = ExceptionThrower.externalizable(true, true);

    /** */
    private static final ExceptionThrower EX_WITH_BROKEN_DESER = ExceptionThrower.externalizable(true, false);

    /** */
    private boolean isNodeInfoAvailableInExMsg = true;

    /** */
    @Test
    public void testServiceCancelThrowsSerializableException() throws Exception {
        Service svc = new ServiceWithException().withCancelException(SERIAL_EX);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceCancelThrowsExternalizableException() throws Exception {
        Service svc = new ServiceWithException().withCancelException(EXT_EX);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceCancelThrowsExternalizableExceptionWithBrokenSerialization() throws Exception {
        Service svc = new ServiceWithException().withCancelException(EX_WITH_BROKEN_SER);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceCancelThrowsExternalizableExceptionWithBrokenDeserialization() throws Exception {
        Service svc = new ServiceWithException().withCancelException(EX_WITH_BROKEN_DESER);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceInitThrowsSerializableException() throws Exception {
        Service svc = new ServiceWithException().withInitException(SERIAL_EX);

        testExceptionPropagation(getServiceConfiguration(svc), true, false);
    }

    /** */
    @Test
    public void testServiceInitThrowsExternalizableException() throws Exception {
        Service svc = new ServiceWithException().withInitException(EXT_EX);

        testExceptionPropagation(getServiceConfiguration(svc), true, false);
    }

    /** */
    @Test
    public void testServiceInitThrowsExternalizableExceptionWithBrokenSerialization() throws Exception {
        Service svc = new ServiceWithException().withInitException(EX_WITH_BROKEN_SER);

        testExceptionPropagation(getServiceConfiguration(svc), true, false);
    }

    /** */
    @Test
    public void testServiceInitThrowsExternalizableExceptionWithBrokenDeserialization() throws Exception {
        Service svc = new ServiceWithException().withInitException(EX_WITH_BROKEN_DESER);

        isNodeInfoAvailableInExMsg = false;

        testExceptionPropagation(getServiceConfiguration(svc), true, false);

        isNodeInfoAvailableInExMsg = true;
    }

    /** */
    @Test
    public void testServiceExecuteThrowsSerializableException() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(SERIAL_EX);

        testExceptionPropagation(getServiceConfiguration(svc), false, false);
    }

    /** */
    @Test
    public void testServiceExecuteThrowsExternalizableException() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(EXT_EX);

        testExceptionPropagation(getServiceConfiguration(svc), false, false);
    }

    /** */
    @Test
    public void testServiceExecuteThrowsExternalizableExceptionWithBrokenSerialization() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(EX_WITH_BROKEN_SER);

        testExceptionPropagation(getServiceConfiguration(svc), false, false);
    }

    /** */
    @Test
    public void testServiceExecuteThrowsExternalizableExceptionWithBrokenDeserialization() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(EX_WITH_BROKEN_DESER);

        testExceptionPropagation(getServiceConfiguration(svc), false, false);
    }

    /** */
    private void testExceptionPropagation(
        ServiceConfiguration srvcCfg,
        boolean shouldThrow,
        boolean withCancel
    ) throws Exception {
        try (IgniteEx srv = startGrid(0); IgniteEx client = startClientGrid(1)) {
            try {
                client.services().deploy(srvcCfg);

                if (withCancel)
                    client.services().cancel(SERVICE_NAME);

                if (shouldThrow)
                    fail("An expected exception has not been thrown.");
            }
            catch (ServiceDeploymentException ex) {
                assertTrue(shouldThrow);

                String errMsg = ex.getSuppressed()[0].getMessage();

                if (isNodeInfoAvailableInExMsg) {
                    assertTrue(errMsg.contains(srv.cluster().localNode().id().toString()));
                    assertTrue(errMsg.contains(SERVICE_NAME));
                }

                Throwable cause = ex.getSuppressed()[0].getCause();

                if (cause == null)
                    assertTrue(errMsg.contains(RETURNED_EX_BROKEN_SER_MSG));
                else
                    assertTrue(cause.getMessage().contains(EX_MSG));
            }
            catch (Throwable e) {
                throw new AssertionError("Unexpected exception has been thrown.", e);
            }
        }
    }

    /** */
    private ServiceConfiguration getServiceConfiguration(Service svc) {
        return new ServiceConfiguration()
            .setService(svc)
            .setName(SERVICE_NAME)
            .setMaxPerNodeCount(1)
            .setNodeFilter(new ClientNodeFilter());
    }

    /**
     *
     */
    private static class ClientNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.isClient();
        }
    }

    /**
     * A simple {@link Service} implementation that intentionally throws exceptions during
     * specific {@link Service} lifecycle phases for testing purposes.
     *
     * <p>This service can be configured to:
     * <ul>
     *     <li>Throw an {@link Exception} during either {@link #init()} or {@link #execute()}.</li>
     *     <li>Throw a {@link RuntimeException} during {@link #cancel()}.</li>
     * </ul>
     */
    private static class ServiceWithException implements Service {
        /** */
        private boolean throwOnCancel;

        /** */
        private boolean throwOnInit;

        /** */
        private boolean throwOnExec;

        /** */
        private ExceptionThrower exThrower;

        /** {@inheritDoc} */
        @Override public void cancel() {
            if (throwOnCancel)
                exThrower.throwRuntimeException();
        }

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            if (throwOnInit)
                exThrower.throwException();
        }

        /** {@inheritDoc} */
        @Override public void execute() throws Exception {
            if (throwOnExec)
                exThrower.throwException();
        }

        /** */
        public Service withCancelException(ExceptionThrower exThrower) {
            this.exThrower = exThrower;

            throwOnCancel = true;

            return this;
        }

        /** */
        public Service withInitException(ExceptionThrower exThrower) {
            this.exThrower = exThrower;

            throwOnInit = true;

            return this;
        }

        /** */
        public Service withExecuteException(ExceptionThrower exThrower) {
            this.exThrower = exThrower;

            throwOnExec = true;

            return this;
        }
    }

    /** */
    private static class ExceptionThrower implements Serializable {
        /** */
        private final boolean isSerializable;

        /** */
        private final boolean isBroken;

        /** */
        private final boolean isSerializationBroken;

        /** */
        private ExceptionThrower(boolean isSerializable, boolean isBroken, boolean isSerializationBroken) {
            this.isSerializable = isSerializable;
            this.isBroken = isBroken;
            this.isSerializationBroken = isSerializationBroken;
        }

        /**
         * @return Serializable exception configuration
         */
        static ExceptionThrower serializable() {
            return new ExceptionThrower(true, false, false);
        }

        /**
         * @return Externalizable exception configuration
         */
        static ExceptionThrower externalizable(boolean isBroken, boolean isSerializationBroken) {
            return new ExceptionThrower(false, isBroken, isSerializationBroken);
        }

        /** */
        public void throwException() throws Exception {
            if (isSerializable)
                throw new Exception(EX_MSG);

            throw new ExternalizableException(EX_MSG, isBroken, isSerializationBroken);
        }

        /** */
        public void throwRuntimeException() {
            if (isSerializable)
                throw new RuntimeException(EX_MSG);

            throw new ExternalizableRuntimeException(EX_MSG, isBroken, isSerializationBroken);
        }
    }

    /** Custom {@link Externalizable} Exception */
    public static class ExternalizableException extends Exception implements Externalizable {
        /** */
        private boolean isBroken;

        /** */
        private boolean isSerializationBroken;

        /** */
        public ExternalizableException() {
            // No-op.
        }

        /** */
        public ExternalizableException(
            String msg,
            boolean isBroken,
            boolean isSerializationBroken
        ) {
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
                throw new ExternalizableRuntimeException(EX_BROKEN_SER_MSG);

            out.writeBoolean(isBroken());
            out.writeBoolean(isSerializationBroken());

            out.writeObject(getMessage());
            out.writeObject(getStackTrace());
            out.writeObject(getCause());

            Throwable[] suppressed = getSuppressed();

            out.writeInt(suppressed.length);

            for (Throwable t : suppressed)
                out.writeObject(t);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setBroken(in.readBoolean());
            setSerializationBroken(in.readBoolean());

            if (isBroken() && !isSerializationBroken())
                throw new ExternalizableRuntimeException(EX_BROKEN_SER_MSG);

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

            int suppressedLen = in.readInt();

            for (int i = 0; i < suppressedLen; i++)
                addSuppressed((Throwable)in.readObject());
        }
    }

    /** Custom externalizable {@link RuntimeException} Exception */
    public static class ExternalizableRuntimeException extends RuntimeException implements Externalizable {
        /** */
        private boolean isBroken;

        /** */
        private boolean isSerializationBroken;

        /** */
        public ExternalizableRuntimeException() {
            // No-op.
        }

        /** */
        public ExternalizableRuntimeException(String msg) {
            super(msg);
        }

        /** */
        public ExternalizableRuntimeException(
            String msg,
            boolean isBroken,
            boolean isSerializationBroken
        ) {
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
                throw new ExternalizableRuntimeException(EX_BROKEN_SER_MSG);

            out.writeBoolean(isBroken());
            out.writeBoolean(isSerializationBroken());

            out.writeObject(getMessage());
            out.writeObject(getStackTrace());
            out.writeObject(getCause());

            Throwable[] suppressed = getSuppressed();

            out.writeInt(suppressed.length);

            for (Throwable t : suppressed)
                out.writeObject(t);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setBroken(in.readBoolean());
            setSerializationBroken(in.readBoolean());

            if (isBroken() && !isSerializationBroken())
                throw new ExternalizableRuntimeException(EX_BROKEN_SER_MSG);

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

            int suppressedLen = in.readInt();

            for (int i = 0; i < suppressedLen; i++)
                addSuppressed((Throwable)in.readObject());
        }
    }
}
