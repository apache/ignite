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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Collections;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_GRID_NAME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_INSTANCE_NAME;

/**
 *
 */
public class GridLoggerProxy implements IgniteLogger, LifecycleAware, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static ThreadLocal<IgniteBiTuple<String, Object>> stash = new ThreadLocal<IgniteBiTuple<String, Object>>() {
        @Override protected IgniteBiTuple<String, Object> initialValue() {
            return new IgniteBiTuple<>();
        }
    };

    /** */
    private IgniteLogger impl;

    /** */
    private String igniteInstanceName;

    /** */
    private String id8;

    /** */
    @GridToStringExclude
    private Object ctgr;

    /** Whether or not to log Ignite instance name. */
    private static final boolean logIgniteInstanceName = System.getProperty(IGNITE_LOG_INSTANCE_NAME) != null ||
        System.getProperty(IGNITE_LOG_GRID_NAME) != null;

    /**
     * No-arg constructor is required by externalization.
     */
    public GridLoggerProxy() {
        // No-op.
    }

    /**
     *
     * @param impl Logger implementation to proxy to.
     * @param ctgr Optional logger category.
     * @param igniteInstanceName Ignite instance name (can be {@code null} for default grid).
     * @param id8 Node ID.
     */
    public GridLoggerProxy(IgniteLogger impl, @Nullable Object ctgr, @Nullable String igniteInstanceName, String id8) {
        assert impl != null;

        this.impl = impl;
        this.ctgr = ctgr;
        this.igniteInstanceName = igniteInstanceName;
        this.id8 = id8;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (impl instanceof LifecycleAware)
            ((LifecycleAware)impl).start();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        U.stopLifecycleAware(this, Collections.singleton(impl));
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        assert ctgr != null;

        return new GridLoggerProxy(impl.getLogger(ctgr), ctgr, igniteInstanceName, id8);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return impl.fileName();
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        impl.trace(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void trace(@Nullable String marker, String msg) {
        impl.trace(marker, enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        impl.debug(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void debug(@Nullable String marker, String msg) {
        impl.debug(marker, enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        impl.info(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void info(@Nullable String marker, String msg) {
        impl.info(marker, enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        impl.warning(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, Throwable e) {
        impl.warning(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void warning(@Nullable String marker, String msg, @Nullable Throwable e) {
        impl.warning(marker, enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        impl.error(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, Throwable e) {
        impl.error(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void error(@Nullable String marker, String msg, @Nullable Throwable e) {
        impl.error(marker, enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return impl.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return impl.isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return impl.isInfoEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return impl.isQuiet();
    }

    /**
     * Gets the class name and parameters of the Logger type used.
     *
     * @return Logger information (name and parameters)
     */
    public String getLoggerInfo() {
        return impl.toString();
    }

    /**
     * Enriches the log message with Ignite instance name if
     * {@link org.apache.ignite.IgniteSystemProperties#IGNITE_LOG_INSTANCE_NAME} or
     * {@link org.apache.ignite.IgniteSystemProperties#IGNITE_LOG_GRID_NAME} system property is set.
     *
     * @param m Message to enrich.
     * @return Enriched message or the original one.
     */
    private String enrich(@Nullable String m) {
        return logIgniteInstanceName && m != null ? "<" + igniteInstanceName + '-' + id8 + "> " + m : m;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, igniteInstanceName);
        out.writeObject(ctgr);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        IgniteBiTuple<String, Object> t = stash.get();

        t.set1(U.readString(in));
        t.set2(in.readObject());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            IgniteBiTuple<String, Object> t = stash.get();

            Object ctgrR = t.get2();

            IgniteLogger log = IgnitionEx.localIgnite().log();

            return ctgrR != null ? log.getLogger(ctgrR) : log;
        }
        catch (IllegalStateException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridLoggerProxy.class, this);
    }
}
