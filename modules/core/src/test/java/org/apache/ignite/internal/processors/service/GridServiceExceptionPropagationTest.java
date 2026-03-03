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
    private static final String BROKEN_EX_MSG = "Exception occurred on serialization step";

    /** */
    private static final String BROKEN_EX_WRAPPER_MSG = ", see server logs for details";

    /** */
    private static final String EX_MSG = "Exception message";

    /** */
    private static final String SERVICE_NAME = "my-service";

    /** */
    private static final ExceptionThrower SERIALIZABLE_EX_THROWER = ExceptionThrower.serializable();

    /** */
    private static final ExceptionThrower EXTERNALIZABLE_EX_THROWER =
        ExceptionThrower.externalizable(false, false);

    /** */
    private static final ExceptionThrower BROKEN_WRITE_EX_THROWER =
        ExceptionThrower.externalizable(true, true);

    /** */
    private static final ExceptionThrower BROKEN_READ_EX_THROWER =
        ExceptionThrower.externalizable(true, false);

    /** */
    private boolean isNodeInfoAvailableInExMsg = true;

    /** */
    @Test
    public void testServiceCancelThrowsSerializableException() throws Exception {
        Service svc = new ServiceWithException().withCancelException(SERIALIZABLE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceCancelThrowsExternalizableException() throws Exception {
        Service svc = new ServiceWithException().withCancelException(EXTERNALIZABLE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceCancelThrowsExternalizableExceptionWithBrokenSerialization() throws Exception {
        Service svc = new ServiceWithException().withCancelException(BROKEN_WRITE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceCancelThrowsExternalizableExceptionWithBrokenDeserialization() throws Exception {
        Service svc = new ServiceWithException().withCancelException(BROKEN_READ_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), false, true);
    }

    /** */
    @Test
    public void testServiceInitThrowsSerializableException() throws Exception {
        Service svc = new ServiceWithException().withInitException(SERIALIZABLE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), true, false);
    }

    /** */
    @Test
    public void testServiceInitThrowsExternalizableException() throws Exception {
        Service svc = new ServiceWithException().withInitException(EXTERNALIZABLE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), true, false);
    }

    /** */
    @Test
    public void testServiceInitThrowsExternalizableExceptionWithBrokenSerialization() throws Exception {
        Service svc = new ServiceWithException().withInitException(BROKEN_WRITE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), true, false);
    }

    /** */
    @Test
    public void testServiceInitThrowsExternalizableExceptionWithBrokenDeserialization() throws Exception {
        Service svc = new ServiceWithException().withInitException(BROKEN_READ_EX_THROWER);

        isNodeInfoAvailableInExMsg = false;

        testExceptionPropagation(getServiceConfiguration(svc), true, false);

        isNodeInfoAvailableInExMsg = true;
    }

    /** */
    @Test
    public void testServiceExecuteThrowsSerializableException() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(SERIALIZABLE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), false, false);
    }

    /** */
    @Test
    public void testServiceExecuteThrowsExternalizableException() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(EXTERNALIZABLE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), false, false);
    }

    /** */
    @Test
    public void testServiceExecuteThrowsExternalizableExceptionWithBrokenSerialization() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(BROKEN_WRITE_EX_THROWER);

        testExceptionPropagation(getServiceConfiguration(svc), false, false);
    }

    /** */
    @Test
    public void testServiceExecuteThrowsExternalizableExceptionWithBrokenDeserialization() throws Exception {
        Service svc = new ServiceWithException().withExecuteException(BROKEN_READ_EX_THROWER);

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
                    assertTrue(errMsg.contains(BROKEN_EX_WRAPPER_MSG));
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
        private final boolean isExternalizable;

        /** */
        private final boolean isBroken;

        /** */
        private final boolean isWriteBroken;

        /** */
        private ExceptionThrower(boolean isExternalizable, boolean isBroken, boolean isWriteBroken) {
            this.isExternalizable = isExternalizable;
            this.isBroken = isBroken;
            this.isWriteBroken = isWriteBroken;
        }

        /**
         * @return Serializable exception configuration
         */
        static ExceptionThrower serializable() {
            return new ExceptionThrower(false, false, false);
        }

        /**
         * @return Externalizable exception configuration
         */
        static ExceptionThrower externalizable(boolean isBroken, boolean isWriteBroken) {
            return new ExceptionThrower(true, isBroken, isWriteBroken);
        }

        /** */
        public void throwException() throws Exception {
            if (!isExternalizable)
                throw new Exception(EX_MSG);

            if (!isBroken)
                throw new ExternalizableException(EX_MSG);

            throw new BrokenExternalizableException(EX_MSG, isWriteBroken);
        }

        /** */
        public void throwRuntimeException() {
            if (!isExternalizable)
                throw new RuntimeException(EX_MSG);

            if (!isBroken)
                throw new ExternalizableRuntimeException(EX_MSG);

            throw new BrokenExternalizableRuntimeException(EX_MSG, isWriteBroken);
        }
    }

    /** Custom {@link Externalizable} Exception */
    public static class ExternalizableException extends Exception implements Externalizable {
        /** */
        public ExternalizableException() {
            // No-op.
        }

        /** */
        public ExternalizableException(String msg) {
            super(msg);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(getMessage());
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            String msg = (String)in.readObject();

            try {
                Field detailMsg = Throwable.class.getDeclaredField("detailMessage");

                detailMsg.setAccessible(true);
                detailMsg.set(this, msg);
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /** Custom {@link Externalizable} Exception with broken serialization. */
    public static class BrokenExternalizableException extends ExternalizableException {
        /** */
        private boolean isWriteBroken;

        /** */
        public BrokenExternalizableException() {
            // No-op.
        }

        /** */
        public BrokenExternalizableException(String msg, boolean isWriteBroken) {
            super(msg);

            this.isWriteBroken = isWriteBroken;
        }

        /** */
        public boolean isWriteBroken() {
            return isWriteBroken;
        }

        /** */
        public void setWriteBroken(boolean writeBroken) {
            isWriteBroken = writeBroken;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            if (isWriteBroken())
                throw new RuntimeException(BROKEN_EX_MSG);

            out.writeBoolean(isWriteBroken());

            super.writeExternal(out);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setWriteBroken(in.readBoolean());

            if (!isWriteBroken())
                throw new RuntimeException(BROKEN_EX_MSG);

            super.readExternal(in);
        }
    }

    /** Custom externalizable {@link RuntimeException} Exception */
    public static class ExternalizableRuntimeException extends RuntimeException implements Externalizable {
        /** */
        public ExternalizableRuntimeException() {
            // No-op.
        }

        /** */
        public ExternalizableRuntimeException(String msg) {
            super(msg);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(getMessage());
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            String msg = (String)in.readObject();

            try {
                Field detailMsg = Throwable.class.getDeclaredField("detailMessage");

                detailMsg.setAccessible(true);
                detailMsg.set(this, msg);
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /** Custom externalizable {@link RuntimeException} Exception with broken serialization. */
    public static class BrokenExternalizableRuntimeException extends ExternalizableRuntimeException {
        /** */
        private boolean isWriteBroken;

        /** */
        public BrokenExternalizableRuntimeException() {
            // No-op.
        }

        /** */
        public BrokenExternalizableRuntimeException(String msg, boolean isWriteBroken) {
            super(msg);

            this.isWriteBroken = isWriteBroken;
        }

        /** */
        public boolean isWriteBroken() {
            return isWriteBroken;
        }

        /** */
        public void setWriteBroken(boolean writeBroken) {
            isWriteBroken = writeBroken;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            if (isWriteBroken())
                throw new RuntimeException(BROKEN_EX_MSG);

            out.writeBoolean(isWriteBroken());

            super.writeExternal(out);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setWriteBroken(in.readBoolean());

            if (!isWriteBroken())
                throw new RuntimeException(BROKEN_EX_MSG);

            super.readExternal(in);
        }
    }
}
