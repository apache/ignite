/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

    /** {@inheritDoc} */
    @Override public PlatformAsyncResult processInStreamAsync(int type, BinaryRawReaderEx reader)
            throws IgniteCheckedException {
        throwUnsupported(type);

        return null;
    }

    /**
     * Throw an exception rendering unsupported operation type.
     *
     * @param type Operation type.
     * @return Dummy value which is never returned.
     * @throws IgniteCheckedException Exception to be thrown.
     */
    public static <T> T throwUnsupported(int type) throws IgniteCheckedException {
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
    private PlatformListenable readAndListenFuture(BinaryRawReader reader, IgniteInternalFuture fut,
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
     * Wraps a listenable to be returned to platform.
     *
     * @param listenable Listenable.
     * @return Target.
     */
    protected PlatformTarget wrapListenable(PlatformListenable listenable) {
        return new PlatformListenableTarget(listenable, platformCtx);
    }
}
