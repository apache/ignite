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

package org.apache.ignite.internal.util;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Extended version of {@link AtomicInteger}.
 * <p>
 * In addition to operations provided in java atomic data structures, this class
 * also adds greater than and less than atomic set operations.
 */
public class GridAtomicInteger extends AtomicInteger {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new AtomicInteger with initial value {@code 0}.
     */
    public GridAtomicInteger() {
        // No-op.
    }

    /**
     * Creates a new AtomicInteger with the given initial value.
     *
     * @param initVal the initial value
     */
    public GridAtomicInteger(int initVal) {
        super(initVal);
    }

    /**
     * Atomically updates value only if {@code check} value is greater
     * than current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean greaterAndSet(int check, int update) {
        while (true) {
            int cur = get();

            if (check > cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if {@code check} value is greater
     * than or equal to current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean greaterEqualsAndSet(int check, int update) {
        while (true) {
            int cur = get();

            if (check >= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if {@code check} value is less
     * than current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean lessAndSet(int check, int update) {
        while (true) {
            int cur = get();

            if (check < cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if {@code check} value is less
     * than current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean lessEqualsAndSet(int check, int update) {
        while (true) {
            int cur = get();

            if (check <= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is greater than current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfGreater(int update) {
        while (true) {
            int cur = get();

            if (update > cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is greater than or equals to current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfGreaterEquals(int update) {
        while (true) {
            int cur = get();

            if (update >= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is less than current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfLess(int update) {
        while (true) {
            int cur = get();

            if (update < cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is less than or equals to current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfLessEquals(int update) {
        while (true) {
            int cur = get();

            if (update <= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }


    /**
     * Sets value only if it is not equals to current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfNotEquals(int update) {
        while (true) {
            int cur = get();

            if (update != cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if passed in predicate returns {@code true}.
     *
     * @param p Predicate to check.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean checkAndSet(IgnitePredicate<Integer> p, int update) {
        while (true) {
            int cur = get();

            if (p.apply(cur)) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }
}