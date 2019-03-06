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

package org.apache.ignite.internal.util;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Extended version of {@link AtomicLong}.
 * <p>
 * In addition to operations provided in java atomic data structures, this class
 * also adds greater than and less than atomic set operations.
 */
public class GridAtomicLong extends AtomicLong {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new AtomicLong with initial value {@code 0}.
     */
    public GridAtomicLong() {
        // No-op.
    }

    /**
     * Creates a new AtomicLong with the given initial value.
     *
     * @param initVal the initial value
     */
    public GridAtomicLong(long initVal) {
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
    public boolean greaterAndSet(long check, long update) {
        while (true) {
            long cur = get();

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
    public boolean greaterEqualsAndSet(long check, long update) {
        while (true) {
            long cur = get();

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
    public boolean lessAndSet(long check, long update) {
        while (true) {
            long cur = get();

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
    public boolean lessEqualsAndSet(long check, long update) {
        while (true) {
            long cur = get();

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
    public boolean setIfGreater(long update) {
        while (true) {
            long cur = get();

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
    public boolean setIfGreaterEquals(long update) {
        while (true) {
            long cur = get();

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
    public boolean setIfLess(long update) {
        while (true) {
            long cur = get();

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
    public boolean setIfLessEquals(long update) {
        while (true) {
            long cur = get();

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
    public boolean setIfNotEquals(long update) {
        while (true) {
            long cur = get();

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
    public boolean checkAndSet(IgnitePredicate<Long> p, long update) {
        while (true) {
            long cur = get();

            if (p.apply(cur)) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }
}