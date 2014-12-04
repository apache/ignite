/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.lang.*;

import java.util.concurrent.atomic.*;

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
