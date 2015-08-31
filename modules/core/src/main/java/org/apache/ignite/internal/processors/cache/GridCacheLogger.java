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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Logger which automatically attaches {@code [cacheName]} to every log statement.
 */
@GridToStringExclude
class GridCacheLogger implements IgniteLogger, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static ThreadLocal<IgniteBiTuple<String, GridCacheContext>> stash =
        new ThreadLocal<IgniteBiTuple<String, GridCacheContext>>() {
            @Override protected IgniteBiTuple<String, GridCacheContext> initialValue() {
                return F.t2();
            }
        };

    /** Logger. */
    private IgniteLogger log;

    /** Cache name. */
    private GridCacheContext<?, ?> cctx;

    /** Cache name. */
    private String cacheName;

    /** Category. */
    private String ctgr;

    /**
     * @param cctx Cache context.
     * @param ctgr Category.
     */
    GridCacheLogger(GridCacheContext<?, ?> cctx, String ctgr) {
        assert cctx != null;
        assert ctgr != null;

        this.cctx = cctx;
        this.ctgr = ctgr;

        cacheName = '<' + cctx.namexx() + "> ";

        log = cctx.kernalContext().log().getLogger(ctgr);
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLogger() {
        // No-op.
    }

    /**
     * @param msg Message.
     * @return Formatted log message.
     */
    private String format(String msg) {
        return cacheName + msg;
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        log.debug(format(msg));
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new GridCacheLogger(cctx, ctgr.toString());
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        log.trace(format(msg));
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        log.info(format(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        log.warning(format(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        log.warning(format(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        log.error(format(msg));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        log.error(format(msg), e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return log.isQuiet();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return log.fileName();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, ctgr);
        out.writeObject(cctx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<String, GridCacheContext> t = stash.get();

        t.set1(U.readString(in));
        t.set2((GridCacheContext)in.readObject());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<String, GridCacheContext> t = stash.get();

            return t.get2().logger(t.get1());
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLogger.class, this);
    }
}