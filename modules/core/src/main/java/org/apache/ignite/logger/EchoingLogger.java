/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IgniteLogger} wrapper that echoes log messages to arbitrary target.
 */
public class EchoingLogger implements IgniteLogger {
    /** */
    private final IgniteLogger delegate;

    /** */
    private final Consumer<String> echoTo;

    /**
     * @param echoTo Echo to.
     * @param delegate Delegate.
     */
    public EchoingLogger(@NotNull IgniteLogger delegate, @NotNull Consumer<String> echoTo) {
        this.delegate = Objects.requireNonNull(delegate);
        this.echoTo = Objects.requireNonNull(echoTo);
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger getLogger(Object ctgr) {
        return new EchoingLogger(delegate.getLogger(ctgr), echoTo);
    }

    /** {@inheritDoc} */
    @Override public void trace(String msg) {
        if (delegate.isTraceEnabled()) {
            delegate.trace(msg);

            echoTo.accept(String.format("[%-23s][%-5s] %s", now(), "TRACE", msg));
        }
    }

    /** {@inheritDoc} */
    @Override public void debug(String msg) {
        if (delegate.isDebugEnabled()) {
            delegate.debug(msg);

            echoTo.accept(String.format("[%-23s][%-5s] %s", now(), "DEBUG", msg));
        }
    }

    /** {@inheritDoc} */
    @Override public void info(String msg) {
        if (delegate.isInfoEnabled()) {
            delegate.info(msg);

            echoTo.accept(String.format("[%-23s][%-5s] %s", now(), "INFO", msg));
        }
    }

    /** {@inheritDoc} */
    @Override public void warning(String msg, @Nullable Throwable e) {
        delegate.warning(msg, e);

        echoTo.accept(String.format("[%-23s][%-5s] %s%s", now(), "WARN", msg, formatThrowable(Optional.ofNullable(e))));
    }

    /** {@inheritDoc} */
    @Override public void error(String msg, @Nullable Throwable e) {
        delegate.error(msg, e);

        echoTo.accept(String.format("[%-23s][%-5s] %s%s", now(), "ERROR", msg, formatThrowable(Optional.ofNullable(e))));
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isQuiet() {
        return delegate.isQuiet();
    }

    /** {@inheritDoc} */
    @Override public String fileName() {
        return delegate.fileName();
    }

    /**
     *
     */
    private static String now() {
        return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    /**
     *
     */
    private static String formatThrowable(Optional<Throwable> e) {
        return e.map(EchoingLogger::formatThrowable).orElse("");
    }

    /**
     *
     */
    private static String formatThrowable(@NotNull Throwable e) {
        return (e.getMessage() != null ? ": " + e.getMessage() : "") + System.lineSeparator() +
            Arrays.stream(e.getStackTrace())
                .map(StackTraceElement::toString)
                .map(s -> "at " + s)
                .collect(Collectors.joining(System.lineSeparator()));
    }
}
