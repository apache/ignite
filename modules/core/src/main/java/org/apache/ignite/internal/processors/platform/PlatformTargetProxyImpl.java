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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;

/**
 * Platform target that is invoked via JNI and propagates calls to underlying {@link PlatformTarget}.
 */
public class PlatformTargetProxyImpl implements PlatformTargetProxy {
    /** Context. */
    protected final PlatformContext platformCtx;

    /** Underlying target. */
    private final PlatformTarget target;

    public PlatformTargetProxyImpl(PlatformTarget target, PlatformContext platformCtx) {
        assert platformCtx != null;
        assert target != null;

        this.platformCtx = platformCtx;
        this.target = target;
    }

    /** {@inheritDoc} */
    @Override public long inLongOutLong(int type, long val) throws Exception {
        try {
            return target.processInLongOutLong(type, val);
        }
        catch (Exception e) {
            throw target.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long inStreamOutLong(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
            BinaryRawReaderEx reader = platformCtx.reader(mem);

            return target.processInStreamOutLong(type, reader, mem);
        }
        catch (Exception e) {
            throw target.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object inStreamOutObject(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = memPtr != 0 ? platformCtx.memory().get(memPtr) : null) {
            BinaryRawReaderEx reader = mem != null ? platformCtx.reader(mem) : null;

            return wrapProxy(target.processInStreamOutObject(type, reader));
        }
        catch (Exception e) {
            throw target.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void outStream(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            target.processOutStream(type, writer);

            out.synchronize();
        }
        catch (Exception e) {
            throw target.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object outObject(int type) throws Exception {
        try {
            return wrapProxy(target.processOutObject(type));
        }
        catch (Exception e) {
            throw target.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void inStreamOutStream(int type, long inMemPtr, long outMemPtr) throws Exception {
        try (PlatformMemory inMem = platformCtx.memory().get(inMemPtr)) {
            BinaryRawReaderEx reader = platformCtx.reader(inMem);

            try (PlatformMemory outMem = platformCtx.memory().get(outMemPtr)) {
                PlatformOutputStream out = outMem.output();

                BinaryRawWriterEx writer = platformCtx.writer(out);

                target.processInStreamOutStream(type, reader, writer);

                out.synchronize();
            }
        }
        catch (Exception e) {
            throw target.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object inObjectStreamOutObjectStream(int type, Object arg, long inMemPtr, long outMemPtr)
        throws Exception {
        PlatformMemory inMem = null;
        PlatformMemory outMem = null;

        try {
            BinaryRawReaderEx reader = null;

            if (inMemPtr != 0) {
                inMem = platformCtx.memory().get(inMemPtr);

                reader = platformCtx.reader(inMem);
            }

            PlatformOutputStream out = null;
            BinaryRawWriterEx writer = null;

            if (outMemPtr != 0) {
                outMem = platformCtx.memory().get(outMemPtr);

                out = outMem.output();

                writer = platformCtx.writer(out);
            }

            PlatformTarget res = target.processInObjectStreamOutObjectStream(type, unwrapProxy(arg), reader, writer);

            if (out != null)
                out.synchronize();

            return wrapProxy(res);
        }
        catch (Exception e) {
            throw target.convertException(e);
        }
        finally {
            try {
                if (inMem != null)
                    inMem.close();
            }
            finally {
                if (outMem != null)
                    outMem.close();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void listenFuture(final long futId, int typ) throws Exception {
        PlatformFutureUtils.listen(platformCtx, currentFuture(), futId, typ, null, target);
    }

    /** {@inheritDoc} */
    @Override public void listenFutureForOperation(final long futId, int typ, int opId) throws Exception {
        PlatformFutureUtils.listen(platformCtx, currentFuture(), futId, typ, futureWriter(opId), target);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget unwrap() {
        return target;
    }

    /**
     * @return Future writer.
     */
    private PlatformFutureUtils.Writer futureWriter(int opId) {
        return ((PlatformAsyncTarget)target).futureWriter(opId);
    }

    /**
     * @return Current future.
     */
    private IgniteInternalFuture currentFuture() throws IgniteCheckedException {
        return ((PlatformAsyncTarget)target).currentFuture();
    }

    /**
     * Wraps an object in a proxy when possible.
     *
     * @param obj Object to wrap.
     * @return Wrapped object.
     */
    private Object wrapProxy(PlatformTarget obj) {
        return obj == null ? null : new PlatformTargetProxyImpl(obj, platformCtx);
    }

    /**
     * Unwraps an object from a proxy when possible.
     *
     * @param obj Object to unwrap.
     * @return Unwrapped object.
     */
    private PlatformTarget unwrapProxy(Object obj) {
        return obj == null ? null : ((PlatformTargetProxyImpl)obj).target;
    }
}
