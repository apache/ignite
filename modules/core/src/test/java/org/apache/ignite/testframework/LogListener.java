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

import java.time.temporal.ValueRange;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;

/**
 * The basic listener for custom log contents checking in {@link ListeningTestLogger}.<br><br>
 *
 * Supports {@link #matches(String) substring}, {@link #matches(Pattern) regular expression} or
 * {@link #matches(Predicate) predicate} listeners and the following optional modifiers:
 * <ul>
 *  <li>{@link Builder#times times()} sets the exact number of occurrences</li>
 *  <li>{@link Builder#atLeast atLeast()} sets the minimum number of occurrences</li>
 *  <li>{@link Builder#atMost atMost()} sets the maximum number of occurrences</li>
 * </ul>
 * {@link Builder#atLeast atLeast()} and {@link Builder#atMost atMost()} can be used together.<br><br>
 *
 * If the expected number of occurrences is not specified for the listener,
 * then at least one occurence is expected by default. In other words:<pre>
 *
 * {@code LogListener.matches(msg).build();}
 *
 * is equivalent to
 *
 * {@code LogListener.matches(msg).atLeast(1).build();}
 * </pre>
 *
 * If only the expected maximum number of occurrences is specified, then
 * the minimum number of entries for successful validation is zero. In other words:<pre>
 *
 * {@code LogListener.matches(msg).atMost(10).build();}
 *
 * is equivalent to
 *
 * {@code LogListener.matches(msg).atLeast(0).atMost(10).build();}
 * </pre>
 */
public abstract class LogListener implements Consumer<String> {
    /**
     * Checks that all conditions are met.
     *
     * @return {@code True} if all conditions are met.
     */
    public abstract boolean check();

    /**
     * Checks that all conditions are met with timeout.
     *
     * @return {@code True} if all conditions are met.
     */
    public boolean check(long millis) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        while (startTime + millis >= System.currentTimeMillis()) {
            if (check())
                return true;

            Thread.sleep(1000);
        }

        return check();
    }

    /**
     * Reset listener state.
     */
    public abstract void reset();

    /**
     * Creates new builder.
     *
     * @return new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates new listener builder.
     *
     * @param substr Substring to search for in a log message.
     * @return Log message listener builder.
     */
    public static Builder matches(String substr) {
        return new Builder().andMatches(substr);
    }

    /**
     * Creates new listener builder.
     *
     * @param regexp Regular expression to search for in a log message.
     * @return Log message listener builder.
     */
    public static Builder matches(Pattern regexp) {
        return new Builder().andMatches(regexp);
    }

    /**
     * Creates new listener builder.
     *
     * @param pred Log message predicate.
     * @return Log message listener builder.
     */
    public static Builder matches(Predicate<String> pred) {
        return new Builder().andMatches(pred);
    }

    /**
     * Log listener builder.
     */
    public static class Builder {
        /** */
        private final CompositeMessageListener lsnr = new CompositeMessageListener();

        /** */
        private Node prev;

        /**
         * Add new substring predicate.
         *
         * @param substr Substring.
         * @return current builder instance.
         */
        public Builder andMatches(String substr) {
            addLast(new Node(msg -> {
                if (substr.isEmpty())
                    return msg.isEmpty() ? 1 : 0;

                int cnt = 0;

                for (int idx = 0; (idx = msg.indexOf(substr, idx)) != -1; idx++)
                    ++cnt;

                return cnt;
            }));

            return this;
        }

        /**
         * Add new regular expression predicate.
         *
         * @param regexp Regular expression.
         * @return current builder instance.
         */
        public Builder andMatches(Pattern regexp) {
            addLast(new Node(msg -> {
                int cnt = 0;

                Matcher matcher = regexp.matcher(msg);

                while (matcher.find())
                    ++cnt;

                return cnt;
            }));

            return this;
        }

        /**
         * Add new log message predicate.
         *
         * @param pred Log message predicate.
         * @return current builder instance.
         */
        public Builder andMatches(Predicate<String> pred) {
            addLast(new Node(msg -> pred.test(msg) ? 1 : 0));

            return this;
        }

        /**
         * Set expected number of matches.<br>
         * Each log message may contain several matches that will be counted,
         * except {@code Predicate} which can have only one match for message.
         *
         * @param n Expected number of matches.
         * @return current builder instance.
         */
        public Builder times(int n) {
            if (prev != null)
                prev.cnt = n;

            return this;
        }

        /**
         * Set expected minimum number of matches.<br>
         * Each log message may contain several matches that will be counted,
         * except {@code Predicate} which can have only one match for message.
         *
         * @param n Expected number of matches.
         * @return current builder instance.
         */
        public Builder atLeast(int n) {
            if (prev != null) {
                prev.min = n;

                prev.cnt = null;
            }

            return this;
        }

        /**
         * Set expected maximum number of matches.<br>
         * Each log message may contain several matches that will be counted,
         * except {@code Predicate} which can have only one match for message.
         *
         * @param n Expected number of matches.
         * @return current builder instance.
         */
        public Builder atMost(int n) {
            if (prev != null) {
                prev.max = n;

                prev.cnt = null;
            }

            return this;
        }

        /**
         * Constructs message listener.
         *
         * @return Log message listener.
         */
        public LogListener build() {
            addLast(null);

            return lsnr.lsnrs.size() == 1 ? lsnr.lsnrs.get(0) : lsnr;
        }

        /**
         * @param node Log listener attributes.
         */
        private void addLast(Node node) {
            if (prev != null)
                lsnr.add(prev.listener());

            prev = node;
        }

        /** */
        private Builder() {}

        /**
         * Mutable attributes for log listener.
         */
        static final class Node {
            /** */
            final Function<String, Integer> func;

            /** */
            Integer min;

            /** */
            Integer max;

            /** */
            Integer cnt;

            /** */
            Node(Function<String, Integer> func) {
                this.func = func;
            }

            /** */
            LogMessageListener listener() {
                ValueRange range;

                if (cnt != null)
                    range = ValueRange.of(cnt, cnt);
                else if (min == null && max == null)
                    range = ValueRange.of(1, Integer.MAX_VALUE);
                else
                    range = ValueRange.of(min == null ? 0 : min, max == null ? Integer.MAX_VALUE : max);

                return new LogMessageListener(func, range);
            }
        }
    }

    /** */
    private static class LogMessageListener extends LogListener {
        /** */
        private final Function<String, Integer> func;

        /** */
        private final AtomicReference<Throwable> err = new AtomicReference<>();

        /** */
        private final AtomicInteger matches = new AtomicInteger();

        /** */
        private final ValueRange exp;

        /**
         * @param exp Expected occurrences.
         * @param func Function of counting matches in the message.
         */
        private LogMessageListener(@NotNull Function<String, Integer> func, @NotNull ValueRange exp) {
            this.func = func;
            this.exp = exp;
        }

        /** {@inheritDoc} */
        @Override public void accept(String msg) {
            if (err.get() != null)
                return;

            try {
                int cnt = func.apply(msg);

                if (cnt > 0)
                    matches.addAndGet(cnt);
            }
            catch (Throwable t) {
                err.compareAndSet(null, t);

                if (t instanceof VirtualMachineError)
                    throw t;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean check() {
            errCheck();

            int matchesCnt = matches.get();

            return exp.isValidIntValue(matchesCnt);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            matches.set(0);
        }

        /**
         * Check that there were no runtime errors.
         */
        private void errCheck() {
            Throwable t = err.get();

            if (t instanceof Error)
                throw (Error)t;

            if (t instanceof RuntimeException)
                throw (RuntimeException)t;

            assert t == null : t;
        }
    }

    /** */
    private static class CompositeMessageListener extends LogListener {
        /** */
        private final List<LogMessageListener> lsnrs = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public boolean check() {
            for (LogMessageListener lsnr : lsnrs)
                if (!lsnr.check())
                    return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            for (LogMessageListener lsnr : lsnrs)
                lsnr.reset();
        }

        /** {@inheritDoc} */
        @Override public void accept(String msg) {
            for (LogMessageListener lsnr : lsnrs)
                lsnr.accept(msg);
        }

        /**
         * @param lsnr Listener.
         */
        private void add(LogMessageListener lsnr) {
            lsnrs.add(lsnr);
        }
    }
}
