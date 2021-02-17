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

package org.apache.ignite.metastorage.common;

import java.util.Arrays;

/**
 * Represents a condition for conditional update.
 */
public final class Condition {
    /** Actual condition implementation. */
    private final InnerCondition cond;

    /**
     * Constructs a condition which wraps the actual condition implementation.
     *
     * @param cond The actual condition implementation.
     */
    Condition(InnerCondition cond) {
        this.cond = cond;
    }

    /**
     * Tests the given entry on satisfaction of the condition.
     *
     * @param e Entry.
     * @return The result of condition test. {@code true} - if the entry satisfies to the condition,
     * otherwise - {@code false}.
     */
    public boolean test(Entry e) {
        return cond.test(e);
    }

    /**
     * Represents condition on entry revision. Only one type of condition could be applied to
     * the one instance of condition. Subsequent invocations of any method which produces condition will throw
     * {@link IllegalStateException}.
     */
    public static final class RevisionCondition implements InnerCondition {
        /**
         * The type of condition.
         *
         * @see Type
         */
        private Type type;

        /**
         * The revision as the condition argument.
         */
        private long rev;

        /**
         * Default no-op constructor.
         */
        RevisionCondition() {
            // No-op.
        }

        /**
         * Produces the condition of type {@link Type#EQUAL}. This condition tests the given revision on equality with
         * target entry revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition eq(long rev) {
            validate(type);

            this.type = Type.EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#NOT_EQUAL}. This condition tests the given revision on inequality
         * with target entry revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#NOT_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition ne(long rev) {
            validate(type);

            this.type = Type.NOT_EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#GREATER}. This condition tests that the target entry revision
         * is greater than given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#GREATER}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition gt(long rev) {
            validate(type);

            this.type = Type.GREATER;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#GREATER_OR_EQUAL}. This condition tests that the target entry
         * revision is greater than or equal to given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#GREATER_OR_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition ge(long rev) {
            validate(type);

            this.type = Type.GREATER_OR_EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#LESS}. This condition tests that target entry revision
         * is less than the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#LESS}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition lt(long rev) {
            validate(type);

            this.type = Type.LESS;
            this.rev = rev;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#LESS_OR_EQUAL}. This condition tests that target entry revision
         * is less than or equal to the given revision.
         *
         * @param rev The revision.
         * @return The condition of type {@link Type#LESS_OR_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition le(long rev) {
            validate(type);

            this.type = Type.LESS_OR_EQUAL;
            this.rev = rev;

            return new Condition(this);
        }

        /** {@inheritDoc} */
        @Override public boolean test(Entry e) {
            int res = Long.compare(e.revision(), rev);

            return type.test(res);
        }

        /**
         * Defines possible condition types which can be applied to the revision.
         */
        enum Type {
            /** Equality condition type. */
            EQUAL {
                @Override public boolean test(long res) {
                    return res == 0;
                }
            },

            /** Inequality condition type. */
            NOT_EQUAL {
                @Override public boolean test(long res) {
                    return res != 0;
                }
            },

            /** Greater than condition type. */
            GREATER {
                @Override public boolean test(long res) {
                    return res > 0;
                }
            },

            /** Less than condition type. */
            LESS {
                @Override public boolean test(long res) {
                    return res < 0;
                }
            },

            /** Less than or equal to condition type. */
            LESS_OR_EQUAL {
                @Override public boolean test(long res) {
                    return res <= 0;
                }
            },

            /** Greater than or equal to condition type. */
            GREATER_OR_EQUAL {
                @Override public boolean test(long res) {
                    return res >= 0;
                }
            };

            /**
             * Interprets comparison result.
             *
             * @param res The result of comparison.
             * @return The interpretation of the comparison result.
             */
            public abstract boolean test(long res);
        }
    }

    /**
     * Represents condition on entry value. Only one type of condition could be applied to
     * the one instance of condition. Subsequent invocations of any method which produces condition will throw
     * {@link IllegalStateException}.
     */
    public static final class ValueCondition implements InnerCondition {
        /**
         * The type of condition.
         *
         * @see Type
         */
        private Type type;

        /**
         * The value as the condition argument.
         */
        private byte[] val;

        /**
         * Default no-op constructor.
         */
        ValueCondition() {
            // No-op.
        }

        /**
         * Produces the condition of type {@link Type#EQUAL}. This condition tests the given value on equality with
         * target entry value.
         *
         * @param val The value.
         * @return The condition of type {@link Type#EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition eq(byte[] val) {
            validate(type);

            this.type = Type.EQUAL;
            this.val = val;

            return new Condition(this);
        }

        /**
         * Produces the condition of type {@link Type#NOT_EQUAL}. This condition tests the given value on inequality
         * with target entry value.
         *
         * @param val The value.
         * @return The condition of type {@link Type#NOT_EQUAL}.
         * @throws IllegalStateException In case when the condition is already defined.
         */
        public Condition ne(byte[] val) {
            validate(type);

            this.type = Type.NOT_EQUAL;
            this.val = val;

            return new Condition(this);
        }

        /** {@inheritDoc} */
        @Override public boolean test(Entry e) {
            int res = Arrays.compare(e.value(), val);

            return type.test(res);
        }

        /**
         * Defines possible condition types which can be applied to the value.
         */
        enum Type {
            /** Equality condition type. */
            EQUAL {
                @Override public boolean test(long res) {
                    return res == 0;
                }
            },

            /** Inequality condition type. */
            NOT_EQUAL {
                @Override public boolean test(long res) {
                    return res != 0;
                }
            };

            /**
             * Interprets comparison result.
             *
             * @param res The result of comparison.
             * @return The interpretation of the comparison result.
             */
            public abstract boolean test(long res);
        }
    }

    /**
     * Checks that condition is not defined yet. If the condition is already defined then exception will be thrown.
     *
     * @throws IllegalStateException In case when the condition is already defined.
     */
    private static void validate(Enum<?> type) {
        if (type != null)
            throw new IllegalStateException("Condition type " + type.name() + " is already defined.");
    }

    /**
     * Defines condition interface.
     */
    private interface InnerCondition {
        /**
         * Tests the given entry on satisfaction of the condition.
         *
         * @param e Entry.
         * @return The result of condition test. {@code true} - if the entry satisfies to the condition,
         * otherwise - {@code false}.
         */
        boolean test(Entry e);
    }
}
