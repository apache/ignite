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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.portable.PortableRawReaderEx;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract interop target.
 */
public abstract class PlatformAbstractTarget implements PlatformTarget {
    /** Constant: TRUE.*/
    protected static final int TRUE = 1;

    /** Constant: FALSE. */
    protected static final int FALSE = 0;

    /** */
    private static final int OP_META = -1;

    /** Context. */
    protected final PlatformContext platformCtx;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    protected PlatformAbstractTarget(PlatformContext platformCtx) {
        this.platformCtx = platformCtx;

        log = platformCtx.kernalContext().log(PlatformAbstractTarget.class);
    }

    /** {@inheritDoc} */
    @Override public long inStreamOutLong(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
            PortableRawReaderEx reader = platformCtx.reader(mem);

            if (type == OP_META) {
                platformCtx.processMetadata(reader);

                return TRUE;
            }
            else
                return processInStreamOutLong(type, reader);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object inStreamOutObject(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
            PortableRawReaderEx reader = platformCtx.reader(mem);

            return processInStreamOutObject(type, reader);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long outLong(int type) throws Exception {
        try {
            return processOutLong(type);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void outStream(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = platformCtx.writer(out);

            processOutStream(type, writer);

            out.synchronize();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object outObject(int type) throws Exception {
        try {
            return processOutObject(type);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void inStreamOutStream(int type, long inMemPtr, long outMemPtr) throws Exception {
        try (PlatformMemory inMem = platformCtx.memory().get(inMemPtr)) {
            PortableRawReaderEx reader = platformCtx.reader(inMem);

            try (PlatformMemory outMem = platformCtx.memory().get(outMemPtr)) {
                PlatformOutputStream out = outMem.output();

                PortableRawWriterEx writer = platformCtx.writer(out);

                processInStreamOutStream(type, reader, writer);

                out.synchronize();
            }
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void inObjectStreamOutStream(int type, Object arg, long inMemPtr, long outMemPtr) throws Exception {
        try (PlatformMemory inMem = platformCtx.memory().get(inMemPtr)) {
            PortableRawReaderEx reader = platformCtx.reader(inMem);

            try (PlatformMemory outMem = platformCtx.memory().get(outMemPtr)) {
                PlatformOutputStream out = outMem.output();

                PortableRawWriterEx writer = platformCtx.writer(out);

                processInObjectStreamOutStream(type, arg, reader, writer);

                out.synchronize();
            }
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /**
     * Convert caught exception.
     *
     * @param e Exception to convert.
     * @return Converted exception.
     */
    public Exception convertException(Exception e) {
        return e;
    }

    /**
     * @return Context.
     */
    public PlatformContext platformContext() {
        return platformCtx;
    }

    /** {@inheritDoc} */
    @Override public void listenFuture(final long futId, int typ) throws Exception {
        PlatformFutureUtils.listen(platformCtx, currentFutureWrapped(), futId, typ, null, this);
    }

    /** {@inheritDoc} */
    @Override public void listenFutureForOperation(final long futId, int typ, int opId) throws Exception {
        PlatformFutureUtils.listen(platformCtx, currentFutureWrapped(), futId, typ, futureWriter(opId), this);
    }

    /**
     * Get current future with proper exception conversions.
     *
     * @return Future.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
    protected IgniteInternalFuture currentFutureWrapped() throws IgniteCheckedException {
        IgniteFutureImpl fut = (IgniteFutureImpl)currentFuture();

        return fut.internalFuture();
    }

    /**
     * When overridden in a derived class, gets future for the current operation.
     *
     * @return current future.
     * @throws IgniteCheckedException
     */
    protected IgniteFuture currentFuture() throws IgniteCheckedException {
        throw new IgniteCheckedException("Future listening is not supported in " + this.getClass());
    }

    /**
     * When overridden in a derived class, gets a custom future writer.
     *
     * @param opId Operation id.
     * @return A custom writer for given op id.
     */
    protected @Nullable PlatformFutureUtils.Writer futureWriter(int opId){
        return null;
    }

    /**
     * Process IN operation.
     *
     * @param type Type.
     * @param reader Portable reader.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    protected long processInStreamOutLong(int type, PortableRawReaderEx reader) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param reader Portable reader.
     * @param writer Portable writer.
     * @throws IgniteCheckedException In case of exception.
     */
    protected void processInStreamOutStream(int type, PortableRawReaderEx reader, PortableRawWriterEx writer)
        throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /**
     * Process IN operation with managed object as result.
     *
     * @param type Type.
     * @param reader Portable reader.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    protected Object processInStreamOutObject(int type, PortableRawReaderEx reader) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param arg Argument.
     * @param reader Portable reader.
     * @param writer Portable writer.
     * @throws IgniteCheckedException In case of exception.
     */
    protected void processInObjectStreamOutStream(int type, @Nullable Object arg, PortableRawReaderEx reader,
        PortableRawWriterEx writer) throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /**
     * Process OUT operation.
     *
     * @param type Type.
     * @throws IgniteCheckedException In case of exception.
     */
    protected long processOutLong(int type) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process OUT operation.
     *
     * @param type Type.
     * @param writer Portable writer.
     * @throws IgniteCheckedException In case of exception.
     */
    protected void processOutStream(int type, PortableRawWriterEx writer) throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /**
     * Process OUT operation.
     *
     * @param type Type.
     * @throws IgniteCheckedException In case of exception.
     */
    protected Object processOutObject(int type) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Throw an exception rendering unsupported operation type.
     *
     * @param type Operation type.
     * @return Dummy value which is never returned.
     * @throws IgniteCheckedException Exception to be thrown.
     */
    protected <T> T throwUnsupported(int type) throws IgniteCheckedException {
        throw new IgniteCheckedException("Unsupported operation type: " + type);
    }
}