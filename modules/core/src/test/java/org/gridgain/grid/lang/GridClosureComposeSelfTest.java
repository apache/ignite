/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

/**
 * Tests for {@code andThen} and {@code compose} methods.
 */
@GridCommonTest(group = "Lang")
public class GridClosureComposeSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testClosure3AndThen() throws Exception {
        GridClosure3<Integer, Integer, Integer, Integer> c1 = new GridClosure3<Integer, Integer, Integer, Integer>() {
            @Override public Integer apply(Integer e1, Integer e2, Integer e3) {
                return e1 + e2 + e3;
            }
        };

        GridClosure3<Integer, Integer, Integer, Integer> c2 = c1.andThen(new GridClosure<Integer, Integer>() {
            @Override public Integer apply(Integer e) {
                return e * 3;
            }
        });

        assert c2.apply(5, 5, 5) == 45;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosure3AndThenIn() throws Exception {
        GridClosure3<Integer, Integer, Integer, Integer> c1 = new GridClosure3<Integer, Integer, Integer, Integer>() {
            @Override public Integer apply(Integer e1, Integer e2, Integer e3) {
                return e1 + e2 + e3;
            }
        };

        final AtomicInteger i = new AtomicInteger();

        GridInClosure3<Integer, Integer, Integer> c2 = c1.andThen(new GridInClosure<Integer>() {
            @Override public void apply(Integer e) {
                i.set(e);
            }
        });

        c2.apply(5, 5, 5);

        assert i.get() == 15;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPredicate3AndThen() throws Exception {
        GridPredicate3<Integer, Integer, Integer> p = new GridPredicate3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer e1, Integer e2, Integer e3) {
                return e1 + e2 + e3 > 5;
            }
        };

        GridClosure3<Integer, Integer, Integer, Integer> c = p.andThen(new GridClosure<Boolean, Integer>() {
            @Override public Integer apply(Boolean e) {
                return e ? 1 : 0;
            }
        });

        assert c.apply(2, 2, 1) == 0;
        assert c.apply(2, 2, 2) == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPredicate3AndThenIn() throws Exception {
        GridPredicate3<Integer, Integer, Integer> p = new GridPredicate3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer e1, Integer e2, Integer e3) {
                return e1 + e2 + e3 > 5;
            }
        };

        final AtomicBoolean b = new AtomicBoolean();

        GridInClosure3<Integer, Integer, Integer> c = p.andThen(new GridInClosure<Boolean>() {
            @Override public void apply(Boolean e) {
                b.set(e);
            }
        });

        c.apply(1, 2, 2);

        assert !b.get();

        c.apply(2, 2, 2);

        assert b.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPredicate3AndThenPredicate() throws Exception {
        GridPredicate3<Integer, Integer, Integer> p = new GridPredicate3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer e1, Integer e2, Integer e3) {
                return e1 + e2 + e3 > 5;
            }
        };

        GridPredicate3<Integer, Integer, Integer> c = p.andThen(new GridPredicate<Boolean>() {
            @Override public boolean apply(Boolean e) {
                return e;
            }
        });

        assert !c.apply(1, 2, 2);
        assert c.apply(2, 2, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAbsPredicateAndThen() throws Exception {
        GridAbsPredicate p = new GridAbsPredicate() {
            @Override public boolean apply() {
                return true;
            }
        };

        GridOutClosure<Integer> c = p.andThen(new GridClosure<Boolean, Integer>() {
            @Override public Integer apply(Boolean e) {
                return e ? 1 : 0;
            }
        });

        assert c.apply() == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAbsPredicateAndThenIn() throws Exception {
        GridAbsPredicate p = new GridAbsPredicate() {
            @Override public boolean apply() {
                return true;
            }
        };

        final AtomicBoolean b = new AtomicBoolean();

        GridAbsClosure c = p.andThen(new GridInClosure<Boolean>() {
            @Override public void apply(Boolean e) {
                b.set(e);
            }
        });

        c.apply();

        assert b.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAbsPredicateAndThenPredicate() throws Exception {
        GridAbsPredicate p = new GridAbsPredicate() {
            @Override public boolean apply() {
                return true;
            }
        };

        GridAbsPredicate c = p.andThen(new GridPredicate<Boolean>() {
            @Override public boolean apply(Boolean e) {
                return e;
            }
        });

        assert c.apply();
    }
}
