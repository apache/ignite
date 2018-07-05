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

import java.util.ArrayList;
import java.util.List;
import javafx.util.Pair;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of {@link org.apache.ignite.IgniteLogger} that perform any actions when certain message is logged.
 */
public class GridRegexpLogger implements IgniteLogger, Cloneable {
    /** */
    private IgniteLogger delegate;

    /** Error lsnrs. */
    private List<Pair<Matcher, IgniteInClosure<String>>> errorLsnrs = new ArrayList<>();

    /** Warning lsnrs. */
    private List<Pair<Matcher, IgniteInClosure<String>>> warningLsnrs = new ArrayList<>();

    /** Info lsnrs. */
    private List<Pair<Matcher, IgniteInClosure<String>>> infoLsnrs = new ArrayList<>();

    /** Trace lsnrs. */
    private List<Pair<Matcher, IgniteInClosure<String>>> traceLsnrs = new ArrayList<>();

    /** Debug lsnrs. */
    private List<Pair<Matcher, IgniteInClosure<String>>> debugLsnrs = new ArrayList<>();

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

        traceLsnrs.forEach(pair -> {
            Matcher matcher = pair.getKey();
            IgniteInClosure<String> lsnr = pair.getValue();

            if (matcher == null || matcher.reset(msg).matches())
                lsnr.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (delegate != null)
            delegate.debug(msg);

        debugLsnrs.forEach(pair -> {
            Matcher matcher = pair.getKey();
            IgniteInClosure<String> lsnr = pair.getValue();

            if (matcher == null || matcher.reset(msg).matches())
                lsnr.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (delegate != null)
            delegate.info(msg);

        infoLsnrs.forEach(pair -> {
            Matcher matcher = pair.getKey();
            IgniteInClosure<String> lsnr = pair.getValue();

            if (matcher == null || matcher.reset(msg).matches())
                lsnr.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        if (delegate != null)
            delegate.warning(msg);

        warningLsnrs.forEach(pair -> {
            Matcher matcher = pair.getKey();
            IgniteInClosure<String> lsnr = pair.getValue();

            if (matcher == null || matcher.reset(msg).matches())
                lsnr.apply(msg);
        });
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        if (delegate != null)
            delegate.error(msg);

        errorLsnrs.forEach(pair -> {
            Matcher matcher = pair.getKey();
            IgniteInClosure<String> lsnr = pair.getValue();

            if (matcher == null || matcher.reset(msg).matches())
                lsnr.apply(msg);
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
     * Set log message listener. Pass null-regexp to match every log message.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listen(@Nullable String regexp, IgniteInClosure<String> lsnr) {
        listenDebug(regexp, lsnr);
        listenError(regexp, lsnr);
        listenInfo(regexp, lsnr);
        listenTrace(regexp, lsnr);
        listenWarning(regexp, lsnr);
    }

    /**
     * Set error log message listener. Pass null-regexp to match every log message.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenError(@Nullable String regexp, IgniteInClosure<String> lsnr) {
        errorLsnrs.add(new Pair<>(regexp != null ? Pattern.compile(regexp).matcher("") : null, lsnr));
    }

    /**
     * Set warning log message listener. Pass null-regexp to match every log message.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenWarning(@Nullable String regexp, IgniteInClosure<String> lsnr) {
        warningLsnrs.add(new Pair<>(regexp != null ? Pattern.compile(regexp).matcher("") : null, lsnr));
    }

    /**
     * Set info log message listener. Pass null-regexp to match every log message.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenInfo(@Nullable String regexp, IgniteInClosure<String> lsnr) {
        infoLsnrs.add(new Pair<>(regexp != null ? Pattern.compile(regexp).matcher("") : null, lsnr));
    }

    /**
     * Set trace log message listener. Pass null-regexp to match every log message.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenTrace(@Nullable String regexp, IgniteInClosure<String> lsnr) {
        traceLsnrs.add(new Pair<>(regexp != null ? Pattern.compile(regexp).matcher("") : null, lsnr));
    }

    /**
     * Set debug log message listener. Pass null-regexp to match every log message.
     *
     * @param regexp Regexp matched against log messages.
     * @param lsnr Listener to execute.
     */
    public void listenDebug(@Nullable String regexp, IgniteInClosure<String> lsnr) {
        debugLsnrs.add(new Pair<>(regexp != null ? Pattern.compile(regexp).matcher("") : null, lsnr));
    }
}
