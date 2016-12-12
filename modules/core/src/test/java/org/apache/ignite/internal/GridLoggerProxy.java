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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LOG_GRID_NAME;

/**
 *
 */
public class GridLoggerProxy implements IgniteLogger, LifecycleAware, Externalizable {

    /**
     * If this system property is present the Ignite will test all strings passed into log.
     * It's a special mode for testing purposes. All debug information will be generated,
     * but only suspicious will be written into log with "error" level and special prefix SENSITIVE>
     */
    public static final String IGNITE_LOG_TEST_SENSITIVE = "IGNITE_LOG_TEST_SENSITIVE";

    /** */
    private static final long serialVersionUID = 0L;
    /** Whether or not to log grid name. */
    private static final boolean logGridName = System.getProperty(IGNITE_LOG_GRID_NAME) != null;
    /** Test sensitive mode. */
    private static final boolean testSensitive = System.getProperty(IGNITE_LOG_TEST_SENSITIVE) != null;
    /** Prefix for all suspicious sensitive data */
    private static final String SENSITIVE_PREFIX = "SENSITIVE> ";
    /** Sensitive patterns */
    private static final Pattern[] SENSITIVE_PS = {
        Pattern.compile("\\b(k|keys?|v|vals?|(new|old|merge|loc)Vals?|" +
            "rows?|fields?|params?|args?|items?|elements?|" +
            "data|obj|res|result|err|error|clause|query|qry|sqlQry|ordinal|" +
            "entrySet|keys?Set|vals?Set)" +
            "\\b\\s*=(?!\\s*(\\w+)\\s*\\[)(?!\\s*null\\s*,)")
    };
    /** */
    private static ThreadLocal<IgniteBiTuple<String, Object>> stash = new ThreadLocal<IgniteBiTuple<String, Object>>() {
        @Override protected IgniteBiTuple<String, Object> initialValue() {
            return new IgniteBiTuple<>();
        }
    };
    /** */
    private static ThreadLocal<StringBuilder> sbLocal = new ThreadLocal<StringBuilder>() {
        @Override protected StringBuilder initialValue() {
            return new StringBuilder(SENSITIVE_PREFIX);
        }
    };
    /** */
    private IgniteLogger impl;
    /** */
    private String gridName;
    /** */
    private String id8;
    /** */
    @GridToStringExclude
    private Object ctgr;

    /**
     * No-arg constructor is required by externalization.
     */
    public GridLoggerProxy() {
        // No-op.
    }

    /**
     * @param impl Logger implementation to proxy to.
     * @param ctgr Optional logger category.
     * @param gridName Grid name (can be {@code null} for default grid).
     * @param id8 Node ID.
     */
    @SuppressWarnings({"IfMayBeConditional", "SimplifiableIfStatement"})
    public GridLoggerProxy(IgniteLogger impl, @Nullable Object ctgr, @Nullable String gridName, String id8) {
        assert impl != null;

        this.impl = impl;
        this.ctgr = ctgr;
        this.gridName = gridName;
        this.id8 = id8;
        if (testSensitive && ctgr == null && gridName == null && id8 == null)
            impl.warning("Test sensitive mode is enabled");
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

        return new GridLoggerProxy(impl.getLogger(ctgr), ctgr, gridName, id8);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String fileName() {
        return impl.fileName();
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (testSensitive) {
            testSensitive(msg);
            if (impl.isTraceEnabled())
                impl.trace(enrich(msg));
        }
        else
            impl.trace(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (testSensitive) {
            testSensitive(msg);
            if (impl.isDebugEnabled())
                impl.debug(enrich(msg));
        }
        else
            impl.debug(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (testSensitive) {
            testSensitive(msg);
            if (impl.isInfoEnabled())
                impl.info(enrich(msg));
        }
        else
            impl.info(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg) {
        if (testSensitive)
            testSensitive(msg);
        impl.warning(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, Throwable e) {
        if (testSensitive)
            testSensitive(msg, e);
        impl.warning(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public void error(String msg) {
        if (testSensitive)
            testSensitive(msg);
        impl.error(enrich(msg));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, Throwable e) {
        if (testSensitive)
            testSensitive(msg, e);
        impl.error(enrich(msg), e);
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return testSensitive || impl.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return testSensitive || impl.isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return testSensitive || impl.isInfoEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return !testSensitive && impl.isQuiet();
    }

    /**
     * Enriches the log message with grid name if {@link org.apache.ignite.IgniteSystemProperties#IGNITE_LOG_GRID_NAME}
     * system property is set.
     *
     * @param m Message to enrich.
     * @return Enriched message or the original one.
     */
    private String enrich(@Nullable String m) {
        return logGridName && m != null ? "<" + gridName + '-' + id8 + "> " + m : m;
    }

    /**
     * Testing the message by sensitive patterns.<br>
     * All sensitive data will be logged with {@link #SENSITIVE_PREFIX}
     */
    private void testSensitive(String m) {
        if (isSensitive(m))
            logSensitive(m);
    }

    /**
     * Testing the message and exceptions chain by sensitive patterns.<br>
     * All sensitive data will be logged with {@link #SENSITIVE_PREFIX}
     */
    private void testSensitive(String m, Throwable e) {
        if (isSensitive(m))
            logSensitive(m, e);
        while (e != null) {
            if (isSensitive(e.getMessage()))
                logSensitive(m, e);
            e = e.getCause();
        }
    }

    /** Checking the message by sensitive patterns */
    private boolean isSensitive(String msg) {
        if (msg == null || msg.isEmpty())
            return false;
        for (Pattern p : SENSITIVE_PS) {
            Matcher m = p.matcher(msg);
            if (m.find()) {
                logSensitive("Found: " + m.group());
                return true;
            }
        }
        return false;
    }

    /** Logging the message with {@link #SENSITIVE_PREFIX} */
    private void logSensitive(String m) {
        logSensitive(m, null);
    }

    /** Logging the message and the exception with {@link #SENSITIVE_PREFIX} */
    private void logSensitive(String m, Throwable e) {
        StringBuilder sb = sbLocal.get();
        sb.setLength(SENSITIVE_PREFIX.length());
        sb.append(m);
        if (e != null)
            sb.append(". ").append(e.getClass()).append(": ").append(e.getMessage());
        impl.error(sb.toString());
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, gridName);
        out.writeObject(ctgr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
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
