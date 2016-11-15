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
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformListenable;
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

    /** Constant: ERROR. */
    protected static final int ERROR = -1;

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
    @Override public long inLongOutLong(int type, long val) throws Exception {
        try {
            return processInLongOutLong(type, val);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long inStreamOutLong(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
            BinaryRawReaderEx reader = platformCtx.reader(mem);

            return processInStreamOutLong(type, reader, mem);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object inStreamOutObject(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = memPtr != 0 ? platformCtx.memory().get(memPtr) : null) {
            BinaryRawReaderEx reader = mem != null ? platformCtx.reader(mem) : null;

            return processInStreamOutObject(type, reader);
        }
        catch (Exception e) {
            throw convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void outStream(int type, long memPtr) throws Exception {
        try (PlatformMemory mem = platformCtx.memory().get(memPtr)) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

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
            BinaryRawReaderEx reader = platformCtx.reader(inMem);

            try (PlatformMemory outMem = platformCtx.memory().get(outMemPtr)) {
                PlatformOutputStream out = outMem.output();

                BinaryRawWriterEx writer = platformCtx.writer(out);

                processInStreamOutStream(type, reader, writer);

                out.synchronize();
            }
        }
        catch (Exception e) {
            throw convertException(e);
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

            Object res = processInObjectStreamOutObjectStream(type, arg, reader, writer);

            if (out != null)
                out.synchronize();

            return res;
        }
        catch (Exception e) {
            throw convertException(e);
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
        PlatformFutureUtils.listen(platformCtx, currentFuture(), futId, typ, null, this);
    }

    /** {@inheritDoc} */
    @Override public void listenFutureForOperation(final long futId, int typ, int opId) throws Exception {
        PlatformFutureUtils.listen(platformCtx, currentFuture(), futId, typ, futureWriter(opId), this);
    }

    /**
     * When overridden in a derived class, gets future for the current operation.
     *
     * @return current future.
     * @throws IgniteCheckedException If failed.
     */
    protected IgniteInternalFuture currentFuture() throws IgniteCheckedException {
        throw new IgniteCheckedException("Future listening is not supported in " + getClass());
    }

    /**
     * When overridden in a derived class, gets a custom future writer.
     *
     * @param opId Operation id.
     * @return A custom writer for given op id.
     */
    @Nullable protected PlatformFutureUtils.Writer futureWriter(int opId){
        return null;
    }

    /**
     * Process IN operation.
     *
     * @param type Type.
     * @param val Value.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    protected long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process IN operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    protected long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process IN operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    protected long processInStreamOutLong(int type, BinaryRawReaderEx reader, PlatformMemory mem) throws IgniteCheckedException {
        return processInStreamOutLong(type, reader);
    }

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @param writer Binary writer.
     * @throws IgniteCheckedException In case of exception.
     */
    protected void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /**
     * Process IN operation with managed object as result.
     *
     * @param type Type.
     * @param reader Binary reader.
     * @return Result.
     * @throws IgniteCheckedException In case of exception.
     */
    protected Object processInStreamOutObject(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process IN-OUT operation.
     *
     * @param type Type.
     * @param arg Argument.
     * @param reader Binary reader.
     * @param writer Binary writer.
     * @throws IgniteCheckedException In case of exception.
     */
    protected Object processInObjectStreamOutObjectStream(int type, @Nullable Object arg, BinaryRawReaderEx reader,
        BinaryRawWriterEx writer) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Process OUT operation.
     *
     * @param type Type.
     * @param writer Binary writer.
     * @throws IgniteCheckedException In case of exception.
     */
    protected void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
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

    /**
     * Reads future information and listens.
     *
     * @param reader Reader.
     * @param fut Future.
     * @param writer Writer.
     * @throws IgniteCheckedException In case of error.
     */
    protected PlatformListenable readAndListenFuture(BinaryRawReader reader, IgniteInternalFuture fut,
                                                     PlatformFutureUtils.Writer writer)
            throws IgniteCheckedException {
        long futId = reader.readLong();
        int futTyp = reader.readInt();

        return PlatformFutureUtils.listen(platformCtx, fut, futId, futTyp, writer, this);
    }

    /**
     * Reads future information and listens.
     *
     * @param reader Reader.
     * @param fut Future.
     * @param writer Writer.
     * @throws IgniteCheckedException In case of error.
     */
    protected PlatformListenable readAndListenFuture(BinaryRawReader reader, IgniteFuture fut,
                                                     PlatformFutureUtils.Writer writer)
            throws IgniteCheckedException {
        long futId = reader.readLong();
        int futTyp = reader.readInt();

        return PlatformFutureUtils.listen(platformCtx, fut, futId, futTyp, writer, this);
    }

    /**
     * Reads future information and listens.
     *
     * @param reader Reader.
     * @param fut Future.
     * @throws IgniteCheckedException In case of error.
     */
    protected PlatformListenable readAndListenFuture(BinaryRawReader reader, IgniteInternalFuture fut)
        throws IgniteCheckedException {
        return readAndListenFuture(reader, fut, null);
    }

    /**
     * Reads future information and listens.
     *
     * @param reader Reader.
     * @param fut Future.
     * @throws IgniteCheckedException In case of error.
     */
    protected PlatformListenable readAndListenFuture(BinaryRawReader reader, IgniteFuture fut)
        throws IgniteCheckedException {
        return readAndListenFuture(reader, fut, null);
    }

    /**
     * Reads future information and listens.
     *
     * @param reader Reader.
     * @throws IgniteCheckedException In case of error.
     */
    protected long readAndListenFuture(BinaryRawReader reader) throws IgniteCheckedException {
        readAndListenFuture(reader, currentFuture(), null);

        return TRUE;
    }
}
