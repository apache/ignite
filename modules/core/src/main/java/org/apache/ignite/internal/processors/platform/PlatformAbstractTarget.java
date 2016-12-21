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
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformListenable;
import org.apache.ignite.internal.processors.platform.utils.PlatformListenableTarget;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract interop target.
 */
public abstract class PlatformAbstractTarget implements PlatformTarget, PlatformAsyncTarget {
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

    /**
     * @return Context.
     */
    public PlatformContext platformContext() {
        return platformCtx;
    }

    /** {@inheritDoc} */
    @Override public Exception convertException(Exception e) {
        return e;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture currentFuture() throws IgniteCheckedException {
        throw new IgniteCheckedException("Future listening is not supported in " + getClass());
    }

    /** {@inheritDoc} */
    @Override @Nullable public PlatformFutureUtils.Writer futureWriter(int opId){
        return null;
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader, PlatformMemory mem) throws IgniteCheckedException {
        return processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInObjectStreamOutObjectStream(int type, @Nullable PlatformTarget arg,
        BinaryRawReaderEx reader, BinaryRawWriterEx writer) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        return throwUnsupported(type);
    }

    /**
     * Throw an exception rendering unsupported operation type.
     *
     * @param type Operation type.
     * @return Dummy value which is never returned.
     * @throws IgniteCheckedException Exception to be thrown.
     */
    private <T> T throwUnsupported(int type) throws IgniteCheckedException {
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

    /**
     * Wraps a listenable to be returned to platform.
     *
     * @param listenable Listenable.
     * @return Target.
     */
    protected PlatformTarget wrapListenable(PlatformListenable listenable) {
        return new PlatformListenableTarget(listenable, platformCtx);
    }
}
