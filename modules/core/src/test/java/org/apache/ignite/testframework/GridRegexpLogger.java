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

package org.apache.ignite.testframework;

import java.util.LinkedHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of {@link org.apache.ignite.IgniteLogger} that perform any actions when certain message is logged.
 */
public class GridRegexpLogger implements IgniteLogger, Cloneable {
    /** */
    private IgniteLogger delegate;

    /** Error lsnrs. */
    private Map<Matcher, IgniteInClosure<String>> errorLsnrs = new LinkedHashMap<>();

    /** Warning lsnrs. */
    private Map<Matcher, IgniteInClosure<String>> warningLsnrs = new LinkedHashMap<>();

    /** Info lsnrs. */
    private Map<Matcher, IgniteInClosure<String>> infoLsnrs = new LinkedHashMap<>();

    /** Trace lsnrs. */
    private Map<Matcher, IgniteInClosure<String>> traceLsnrs = new LinkedHashMap<>();

    /** Debug lsnrs. */
    private Map<Matcher, IgniteInClosure<String>> debugLsnrs = new LinkedHashMap<>();

    /**
     * @param echo IgniteLogger implementation, all logs are delegated to.
     */
    public GridRegexpLogger(IgniteLogger echo) {
        this.delegate = echo;
    }

    /**
     * Default constructor.
     */
    public GridRegexpLogger() {
        delegate = null;
    }

    /** {@inheritDoc} */
    @Override public GridRegexpLogger getLogger(Object ctgr) {
        GridRegexpLogger cp = null;

        try {
            cp = (GridRegexpLogger)this.clone();
        }
        catch (CloneNotSupportedException e) {
            assert false;
        }

        if (delegate != null)
            cp.delegate = delegate.getLogger(ctgr);

        return cp;
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (delegate != null)
            delegate.trace(msg);

        traceLsnrs.forEach((matcher, igniteInClosure) -> {
            if (matcher.reset(msg).matches())
                igniteInClosure.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (delegate != null)
            delegate.debug(msg);

        debugLsnrs.forEach((matcher, igniteInClosure) -> {
            if (matcher.reset(msg).matches())
                igniteInClosure.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (delegate != null)
            delegate.info(msg);

        infoLsnrs.forEach((matcher, igniteInClosure) -> {
            if (matcher.reset(msg).matches())
                igniteInClosure.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        if (delegate != null)
            delegate.warning(msg);

        warningLsnrs.forEach((matcher, igniteInClosure) -> {
            if (matcher.reset(msg).matches())
                igniteInClosure.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        if (delegate != null)
            delegate.error(msg);

        errorLsnrs.forEach((matcher, igniteInClosure) -> {
            if (matcher.reset(msg).matches())
                igniteInClosure.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        if (!traceLsnrs.isEmpty())
            return true;

        if (delegate != null)
            return delegate.isTraceEnabled();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        if (!debugLsnrs.isEmpty())
            return true;

        if (delegate != null)
            return delegate.isDebugEnabled();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        if (!infoLsnrs.isEmpty())
            return true;

        if (delegate != null)
            return delegate.isInfoEnabled();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override public String fileName() {
        return null;
    }

    /**
     * Clears all listeners.
     */
    public void clearListeners() {
        debugLsnrs.clear();
        errorLsnrs.clear();
        infoLsnrs.clear();
        traceLsnrs.clear();
        warningLsnrs.clear();
    }

    /**
     * Set log message listener.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listen(String regexp, IgniteInClosure<String> lsnr) {
        listenDebug(regexp, lsnr);
        listenError(regexp, lsnr);
        listenInfo(regexp, lsnr);
        listenTrace(regexp, lsnr);
        listenWarning(regexp, lsnr);
    }

    /**
     * Set error log message listener.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenError(String regexp, IgniteInClosure<String> lsnr) {
        errorLsnrs.put(Pattern.compile(regexp).matcher(""), lsnr);
    }

    /**
     * Set warning log message listener.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenWarning(String regexp, IgniteInClosure<String> lsnr) {
        warningLsnrs.put(Pattern.compile(regexp).matcher(""), lsnr);
    }

    /**
     * Set info log message listener.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenInfo(String regexp, IgniteInClosure<String> lsnr) {
        infoLsnrs.put(Pattern.compile(regexp).matcher("meow"), lsnr);
    }

    /**
     * Set trace log message listener.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenTrace(String regexp, IgniteInClosure<String> lsnr) {
        traceLsnrs.put(Pattern.compile(regexp).matcher(""), lsnr);
    }

    /**
     * Set debug log message listener.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenDebug(String regexp, IgniteInClosure<String> lsnr) {
        debugLsnrs.put(Pattern.compile(regexp).matcher(""), lsnr);
    }

    /**
     * Waits timeout time for the log message to occur.
     *
     * @param regexp Regexp matched against log messages.
     * @param timeout Timeout time to wait.
     */
    @SuppressWarnings("unchecked")
    public boolean waitFor(String regexp, long timeout) throws InterruptedException {
        CompletableFuture fut = new CompletableFuture();

        listen(regexp, o -> fut.complete(null));

        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e) {
            assert false;
        }
        catch (TimeoutException e) {
            return false;
        }

        return true;
    }

    /**
     * Waits timeout time for the debug log message to occur.
     *
     * @param regexp Regexp matched against log messages.
     * @param timeout Timeout time to wait.
     */
    @SuppressWarnings("unchecked")
    public boolean waitForDebug(String regexp, long timeout) throws InterruptedException {
        CompletableFuture fut = new CompletableFuture();

        listenDebug(regexp, o -> fut.complete(null));

        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e) {
            assert false;
        }
        catch (TimeoutException e) {
            return false;
        }

        return true;
    }

    /**
     * Waits timeout time for the warning log message to occur.
     *
     * @param regexp Regexp matched against log messages.
     * @param timeout Timeout time to wait.
     */
    @SuppressWarnings("unchecked")
    public boolean waitForWarning(String regexp, long timeout) throws InterruptedException {
        CompletableFuture fut = new CompletableFuture();

        listenWarning(regexp, o -> fut.complete(null));

        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e) {
            assert false;
        }
        catch (TimeoutException e) {
            return false;
        }

        return true;
    }

    /**
     * Waits timeout time for the trace log message to occur.
     *
     * @param regexp Regexp matched against log messages.
     * @param timeout Timeout time to wait.
     */
    @SuppressWarnings("unchecked")
    public boolean waitForTrace(String regexp, long timeout) throws InterruptedException {
        CompletableFuture fut = new CompletableFuture();

        listenTrace(regexp, o -> fut.complete(null));

        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e) {
            assert false;
        }
        catch (TimeoutException e) {
            return false;
        }

        return true;
    }

    /**
     * Waits timeout time for the info log message to occur.
     *
     * @param regexp Regexp matched against log messages.
     * @param timeout Timeout time to wait.
     */
    @SuppressWarnings("unchecked")
    public boolean waitForInfo(String regexp, long timeout) throws InterruptedException {
        CompletableFuture fut = new CompletableFuture();

        listenInfo(regexp, o -> fut.complete(null));

        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e) {
            assert false;
        }
        catch (TimeoutException e) {
            return false;
        }

        return true;
    }

    /**
     * Waits timeout time for the error log message to occur.
     *
     * @param regexp Regexp matched against log messages.
     * @param timeout Timeout time to wait.
     */
    @SuppressWarnings("unchecked")
    public boolean waitForError(String regexp, long timeout) throws InterruptedException {
        CompletableFuture fut = new CompletableFuture();

        listenError(regexp, o -> fut.complete(null));

        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException e) {
            assert false;
        }
        catch (TimeoutException e) {
            return false;
        }

        return true;
    }
}
