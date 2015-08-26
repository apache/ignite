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

import org.apache.ignite.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.platform.memory.*;
import org.apache.ignite.internal.processors.platform.utils.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

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

    /** Interop context. */
    protected final PlatformContext interopCtx;

    /** Logger. */
    protected final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param interopCtx Interop context.
     */
    protected PlatformAbstractTarget(PlatformContext interopCtx) {
        this.interopCtx = interopCtx;

        log = interopCtx.kernalContext().log(PlatformAbstractTarget.class);
    }

    /** {@inheritDoc} */
    @Override public int inOp(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = interopCtx.memory().get(memPtr)) {
            PortableRawReaderEx reader = interopCtx.reader(mem);

            if (type == OP_META) {
                interopCtx.processMetadata(reader);

                return TRUE;
            }
            else
                return processInOp(type, reader);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object inOpObject(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = interopCtx.memory().get(memPtr)) {
            PortableRawReaderEx reader = interopCtx.reader(mem);

            return processInOpObject(type, reader);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void outOp(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = interopCtx.memory().get(memPtr)) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = interopCtx.writer(out);

            processOutOp(type, writer);

            out.synchronize();
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void inOutOp(int type, long inMemPtr, long outMemPtr) throws Exception {
        inOutOp(type, inMemPtr, outMemPtr, null);
    }

    /** {@inheritDoc} */
    @Override public void inOutOp(int type, long inMemPtr, long outMemPtr, Object arg) throws Exception {
        try (PlatformMemory inMem = interopCtx.memory().get(inMemPtr)) {
            PortableRawReaderEx reader = interopCtx.reader(inMem);

            try (PlatformMemory outMem = interopCtx.memory().get(outMemPtr)) {
                PlatformOutputStream out = outMem.output();

                PortableRawWriterEx writer = interopCtx.writer(out);

                processInOutOp(type, reader, writer, arg);

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
    protected Exception convertException(Exception e) {
        return e;
    }

    /**
     * @return Interop context.
     */
    public PlatformContext interopContext() {
        return interopCtx;
    }

    /**
     * Start listening for the future.
     *
     * @param futId Future ID.
     * @param typ Result type.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void listenFuture(final long futId, int typ) throws IgniteCheckedException {
        PlatformFutureUtils.listen(interopCtx, currentFutureWrapped(), futId, typ, null);
    }

    /**
     * Start listening for the future.
     *
     * @param futId Future ID.
     * @param typ Result type.
     * @param opId Operation ID required to pick correct result writer.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void listenFuture(final long futId, int typ, int opId) throws IgniteCheckedException {
        PlatformFutureUtils.listen(interopCtx, currentFutureWrapped(), futId, typ, futureWriter(opId));
    }

    /**
     * Get current future with proper exception conversions.
     *
     * @return Future.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
    protected IgniteFuture currentFutureWrapped() throws IgniteCheckedException {
        return currentFuture().chain(new IgniteClosure<IgniteFuture, Object>() {
            @Override public Object apply(IgniteFuture o) {
                try {
                    return o.get();
                }
                catch (RuntimeException e) {
                    Exception converted = convertException(e);

                    if (converted instanceof RuntimeException)
                        throw (RuntimeException)converted;
                    else {
                        log.error("Interop future result cannot be obtained due to exception.", converted);

                        throw new IgniteException("Interop future result cannot be obtained due to exception " +
                            "(see log for more details).");
                    }
                }
            }
        });
    }

    /**
     * When overridden in a derived class, gets future for the current operation.
     *
     * @return current future.
     * @throws org.apache.ignite.IgniteCheckedException
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
     * @throws org.apache.ignite.IgniteCheckedException In case of exception.
     */
    protected int processInOp(int type, PortableRawReaderEx reader) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process IN operation with managed object as result.
     *
     * @param type Type.
     * @param reader Portable reader.
     * @return Result.
     * @throws org.apache.ignite.IgniteCheckedException In case of exception.
     */
    protected Object processInOpObject(int type, PortableRawReaderEx reader) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process OUT operation.
     *
     * @param type Type.
     * @param writer Portable writer.
     * @throws org.apache.ignite.IgniteCheckedException In case of exception.
     */
    protected void processOutOp(int type, PortableRawWriterEx writer) throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param reader Portable reader.
     * @param writer Portable writer.
     * @param arg Argument.
     * @throws org.apache.ignite.IgniteCheckedException In case of exception.
     */
    protected void processInOutOp(int type, PortableRawReaderEx reader, PortableRawWriterEx writer,
        @Nullable Object arg) throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /**
     * Throw an exception rendering unsupported operation type.
     *
     * @param type Operation type.
     * @return Dummy value which is never returned.
     * @throws org.apache.ignite.IgniteCheckedException Exception to be thrown.
     */
    protected <T> T throwUnsupported(int type) throws IgniteCheckedException {
        throw new IgniteCheckedException("Unsupported operation type: " + type);
    }
}
