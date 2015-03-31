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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.*;
import java.io.*;
import java.lang.reflect.*;
import java.math.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Contains factory and utility methods for {@code closures}, {@code predicates}, and {@code tuples}.
 * It also contains functional style collection comprehensions.
 * <p>
 * Most of the methods in this class can be divided into two groups:
 * <ul>
 * <li><b>Factory</b> higher-order methods for closures, predicates and tuples, and</li>
 * <li><b>Utility</b> higher-order methods for processing collections with closures and predicates.</li>
 * </ul>
 * Note that contrary to the usual design this class has substantial number of
 * methods (over 200). This design is chosen to simulate a global namespace
 * (like a {@code Predef} in Scala) to provide general utility and functional
 * programming functionality in a shortest syntactical context using {@code F}
 * typedef.
 * <p>
 * Also note, that in all methods with predicates, null predicate has a {@code true} meaning. So does
 * the empty predicate array.
 */
public class GridFunc {
    /** */
    private static final GridAbsClosure NOOP = new CA() {
        @Override public void apply() { /* No-op. */ }
    };

    /** */
    private static final IgniteClosure IDENTITY = new C1() {
        @Override public Object apply(Object o) {
            return o;
        }

        @Override public String toString() {
            return "Identity closure.";
        }
    };

    /** */
    private static final IgnitePredicate<Boolean> IDENTITY_PRED = new P1<Boolean>() {
        @Override public boolean apply(Boolean e) {
            return e;
        }
    };

    /** */
    private static final IgnitePredicate<Object> ALWAYS_TRUE = new P1<Object>() {
        @Override public boolean apply(Object e) {
            return true;
        }

        @Override public String toString() {
            return "Always true predicate.";
        }
    };

    /** */
    private static final IgnitePredicate<Object> ALWAYS_FALSE = new P1<Object>() {
        @Override public boolean apply(Object e) {
            return false;
        }

        @Override public String toString() {
            return "Always false predicate.";
        }
    };

    /** */
    private static final IgniteCallable<?> DEQUE_FACTORY = new IgniteCallable<ConcurrentLinkedDeque8>() {
        @Override public ConcurrentLinkedDeque8 call() {
            return new ConcurrentLinkedDeque8();
        }

        @Override public String toString() {
            return "Deque factory.";
        }
    };

    /** */
    public static final IgnitePredicate<Object> IS_NULL = new P1<Object>() {
        @Override public boolean apply(Object o) {
            return o == null;
        }
    };

    /** */
    public static final IgnitePredicate<Object> IS_NOT_NULL = new P1<Object>() {
        @Override public boolean apply(Object o) {
            return o != null;
        }
    };

    /** */
    public static final IgnitePredicate<String> EMPTY_STRING = new P1<String>() {
        @Override public boolean apply(String s) {
            return isEmpty(s);
        }
    };

    /** */
    public static final IgnitePredicate<String> NOT_EMPTY_STRING = new P1<String>() {
        @Override public boolean apply(String s) {
            return !isEmpty(s);
        }
    };

    /** */
    public static final IgnitePredicate EMPTY_COLLECTION = new P1<Collection>() {
        @Override public boolean apply(Collection c) {
            return isEmpty(c);
        }
    };

    /** */
    public static final IgnitePredicate NOT_EMPTY_COLLECTION = new P1<Collection>() {
        @Override public boolean apply(Collection c) {
            return !isEmpty(c);
        }
    };

    /** */
    private static final IgniteCallable<?> LIST_FACTORY = new IgniteCallable<List>() {
        @Override public List call() {
            return new ArrayList();
        }

        @Override public String toString() {
            return "Array list factory.";
        }
    };

    /** */
    private static final IgniteCallable<?> LINKED_LIST_FACTORY = new IgniteCallable<LinkedList>() {
        @Override public LinkedList call() {
            return new LinkedList();
        }

        @Override public String toString() {
            return "Linked list factory.";
        }
    };

    /** */
    private static final IgniteCallable<?> SET_FACTORY = new IgniteCallable<Set>() {
        @Override public Set call() {
            return new HashSet();
        }

        @Override public String toString() {
            return "Hash set factory.";
        }
    };

    /** */
    private static final IgniteCallable<AtomicInteger> ATOMIC_INT_FACTORY = new IgniteCallable<AtomicInteger>() {
        @Override public AtomicInteger call() {
            return new AtomicInteger(0);
        }

        @Override public String toString() {
            return "Atomic integer factory.";
        }
    };

    /** */
    private static final IgniteCallable<AtomicLong> ATOMIC_LONG_FACTORY = new IgniteCallable<AtomicLong>() {
        @Override public AtomicLong call() {
            return new AtomicLong(0);
        }

        @Override public String toString() {
            return "Atomic long factory.";
        }
    };

    /** */
    private static final IgniteCallable<AtomicBoolean> ATOMIC_BOOL_FACTORY = new IgniteCallable<AtomicBoolean>() {
        @Override public AtomicBoolean call() {
            return new AtomicBoolean();
        }

        @Override public String toString() {
            return "Atomic boolean factory.";
        }
    };

    /** */
    private static final IgniteCallable<?> ATOMIC_REF_FACTORY = new IgniteCallable<AtomicReference>() {
        @Override public AtomicReference call() {
            return new AtomicReference();
        }

        @Override public String toString() {
            return "Atomic reference factory.";
        }
    };

    /** */
    private static final IgniteCallable<?> MAP_FACTORY = new IgniteCallable<Map>() {
        @Override public Map call() {
            return new HashMap();
        }

        @Override public String toString() {
            return "Hash map factory.";
        }
    };

    /** */
    private static final IgniteCallable<?> CONCURRENT_MAP_FACTORY = new IgniteCallable<ConcurrentMap>() {
        @Override public ConcurrentMap call() {
            return new ConcurrentHashMap8();
        }

        @Override public String toString() {
            return "Concurrent hash map factory.";
        }
    };

    /** */
    private static final IgniteCallable<?> CONCURRENT_SET_FACTORY = new IgniteCallable<GridConcurrentHashSet>() {
        @Override public GridConcurrentHashSet call() {
            return new GridConcurrentHashSet();
        }

        @Override public String toString() {
            return "Concurrent hash set factory.";
        }
    };

    /** */
    private static final IgniteInClosure<?> PRINTLN = new CI1() {
        @Override public void apply(Object o) {
            System.out.println(o);
        }

        @Override public String toString() {
            return "Print line closure.";
        }
    };

    /** */
    private static final IgniteInClosure<?> PRINT = new CI1() {
        @Override public void apply(Object o) {
            System.out.print(o);
        }

        @Override public String toString() {
            return "Print closure.";
        }
    };

    /** */
    private static final IgniteOutClosure<?> NILL = new CO() {
        @Nullable @Override public Object apply() {
            return null;
        }

        @Override public String toString() {
            return "Nill closure.";
        }
    };

    /** */
    private static final IgniteClosure<Runnable, GridAbsClosure> R2C = new C1<Runnable, GridAbsClosure>() {
        @Override public GridAbsClosure apply(Runnable r) {
            return as(r);
        }

        @Override public String toString() {
            return "Runnable to absolute closure transformer.";
        }
    };

    /** */
    private static final IgniteClosure<ClusterGroup, IgnitePredicate<ClusterNode>> P2P =
        new C1<ClusterGroup, IgnitePredicate<ClusterNode>>() {
            @Override public IgnitePredicate<ClusterNode> apply(ClusterGroup e) {
                return e.predicate();
            }

            @Override public String toString() {
                return "Projection to its predicate transformer closure.";
            }
    };

    /** */
    private static final IgniteClosure<Object, Class<?>> CLAZZ = new C1<Object, Class<?>>() {
        @Override public Class<?> apply(Object o) {
            return o.getClass();
        }

        @Override public String toString() {
            return "Object to class transformer closure.";
        }
    };

    /** */
    private static final IgniteClosure MAP_ENTRY_KEY = new IgniteClosure() {
        @Override public Object apply(Object o) {
            return ((Map.Entry)o).getKey();
        }

        @Override public String toString() {
            return "Map entry to key transformer closure.";
        }
    };

    /** */
    private static final IgniteClosure CACHE_ENTRY_KEY = new IgniteClosure() {
        @Override public Object apply(Object o) {
            return ((Cache.Entry)o).getKey();
        }

        @Override public String toString() {
            return "Map entry to key transformer closure.";
        }
    };

    /** */
    private static final IgniteClosure MAP_ENTRY_VAL = new IgniteClosure() {
        @Override public Object apply(Object o) {
            return ((Map.Entry)o).getValue();
        }

        @Override public String toString() {
            return "Map entry to value transformer closure.";
        }
    };

    /** */
    private static final IgniteClosure CACHE_ENTRY_VAL_GET = new IgniteClosure() {
        @SuppressWarnings({"unchecked"})
        @Nullable @Override public Object apply(Object o) {
            return ((Cache.Entry)o).getValue();
        }

        @Override public String toString() {
            return "Cache entry to get-value transformer closure.";
        }
    };

    /** */
    private static final IgnitePredicate CACHE_ENTRY_HAS_PEEK_VAL = new IgnitePredicate() {
        @SuppressWarnings({"unchecked"})
        @Override public boolean apply(Object o) {
            return ((Cache.Entry)o).getValue() != null;
        }

        @Override public String toString() {
            return "Cache entry has-peek-value predicate.";
        }
    };

    /** */
    private static final IgnitePredicate CACHE_ENTRY_NO_PEEK_VAL = new IgnitePredicate() {
        @SuppressWarnings({"unchecked"})
        @Override public boolean apply(Object o) {
            return ((Cache.Entry)o).getValue() == null;
        }

        @Override public String toString() {
            return "Cache entry no-peek-value predicate.";
        }
    };

    /** */
    private static final IgniteClosure<ClusterNode, UUID> NODE2ID = new IgniteClosure<ClusterNode, UUID>() {
        @Override public UUID apply(ClusterNode n) {
            return n.id();
        }

        @Override public String toString() {
            return "Grid node to node ID transformer closure.";
        }
    };

    /** */
    private static final IgniteClosure<ClusterNode, String> NODE2ID8 = new IgniteClosure<ClusterNode, String>() {
        @Override public String apply(ClusterNode n) {
            return U.id8(n.id());
        }

        @Override public String toString() {
            return "Grid node to node ID8 transformer closure.";
        }
    };

    /** */
    private static final IgniteClosure<UUID, String> ID2ID8 = new IgniteClosure<UUID, String>() {
        @Override public String apply(UUID id) {
            return U.id8(id);
        }

        @Override public String toString() {
            return "UUID to ID8 transformer closure.";
        }
    };

    /** */
    private static final IgnitePredicate<IgniteInternalFuture<?>> UNFINISHED_FUTURE = new IgnitePredicate<IgniteInternalFuture<?>>() {
        @Override public boolean apply(IgniteInternalFuture<?> f) {
            return !f.isDone();
        }
    };

    /**
     * Gets breaker predicate which will return a predicate that will
     * evaluate to {@code firstVal} when checked the first time,
     * but then will always evaluate to the opposite value.
     *
     * @param firstVal First value.
     * @param <T> Predicate type.
     * @return Breaker predicate.
     */
    public static <T> IgnitePredicate<T> breaker(final boolean firstVal) {
        return new IgnitePredicate<T>() {
            private boolean b = true;

            @Override public boolean apply(T e) {
                if (b) {
                    b = false;

                    return firstVal;
                }

                return !firstVal;
            }

            @Override public String toString() {
                return "Breaker predicate.";
            }
        };
    }

    /**
     * Gets closure that transform a grid projection into its predicate.
     *
     * @return Closure transforming a grid projection into its predicate.
     */
    public static IgniteClosure<ClusterGroup, IgnitePredicate<ClusterNode>> predicate() {
        return P2P;
    }

    /**
     * Gets predicate that evaluates to {@code true} only for given local node ID.
     *
     * @param locNodeId Local node ID.
     * @param <T> Type of the node.
     * @return Return {@code true} only for the node with given local node ID.
     */
    public static <T extends ClusterNode> IgnitePredicate<T> localNode(final UUID locNodeId) {
        return new P1<T>() {
            @Override public boolean apply(T n) {
                return n.id().equals(locNodeId);
            }
        };
    }

    /**
     * Gets predicate that evaluates to {@code false} for given local node ID.
     *
     * @param locNodeId Local node ID.
     * @param <T> Type of the node.
     * @return Return {@code false} for the given local node ID.
     */
    public static <T extends ClusterNode> IgnitePredicate<T> remoteNodes(final UUID locNodeId) {
        return new P1<T>() {
            @Override public boolean apply(T n) {
                return !n.id().equals(locNodeId);
            }
        };
    }

    /**
     * Returns out closure that always returns {@code null}.
     *
     * @return Out closure that always returns {@code null}.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteOutClosure<T> nill() {
        return (IgniteOutClosure<T>)NILL;
    }

    /**
     * Creates closure that will reflectively call a method with the given name on
     * closure's argument and return result of that call.
     * <p>
     * Method reflects the typedef for {@link org.apache.ignite.lang.IgniteClosure} which is {@link C1}.
     *
     * @param mtdName Method name.
     * @param args Optional set of arguments for the method call.
     * @param <R> Type of closure return value.
     * @param <T> Type of closure argument.
     * @return Reflective closure.
     * @throws GridClosureException Thrown in case of any reflective invocation errors.
     */
    public static <T, R> IgniteClosure<T, R> cInvoke(final String mtdName, final Object... args) {
        A.notNull(mtdName, "mtdName");

        return new C1<T, R>() {
            private Method mtd;

            @SuppressWarnings("unchecked")
            @Override public R apply(T t) {
                try {
                    // No synchronization allows for double creation - ignoring...
                    if (mtd == null) {
                        mtd = method(t.getClass(), mtdName, args);

                        mtd.setAccessible(true);
                    }

                    return (R)mtd.invoke(t, args);
                }
                catch (Throwable e) {
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Creates in closure that will reflectively call a method with the given name on
     * closure's argument.
     * <p>
     * Method reflects the typedef for {@link org.apache.ignite.lang.IgniteClosure} which is {@link C1}.
     *
     * @param mtdName Method name.
     * @param args Optional set of arguments for the method call.
     * @param <T> Type of closure argument.
     * @return Reflective in closure.
     * @throws GridClosureException Thrown in case of any reflective invocation errors.
     */
    public static <T> IgniteInClosure<T> ciInvoke(final String mtdName, final Object... args) {
        A.notNull(mtdName, "mtdName");

        return new CI1<T>() {
            private Method mtd;

            @Override public void apply(T t) {
                try {
                    // No synchronization allows for double creation - ignoring...
                    if (mtd == null) {
                        mtd = method(t.getClass(), mtdName, args);

                        mtd.setAccessible(true);
                    }

                    mtd.invoke(t, args);
                }
                catch (Throwable e) {
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Creates out closure that will reflectively call a method with the given name on provided
     * object and return result of that call.
     * <p>
     * Method reflects the typedef for {@link org.apache.ignite.lang.IgniteOutClosure} which is {@link CO}.
     *
     * @param o Target object to call the method on.
     * @param mtdName Method name.
     * @param args Optional set of arguments for the method call.
     * @param <R> Type of closure return value.
     * @return Reflective out closure.
     * @throws GridClosureException Thrown in case of any reflective invocation errors.
     */
    public static <R> IgniteOutClosure<R> coInvoke(final Object o, final String mtdName, final Object... args) {
        A.notNull(o, "o", mtdName, "mtdName");

        return new CO<R>() {
            private Method mtd;

            @SuppressWarnings("unchecked")
            @Override public R apply() {
                try {
                    // No synchronization allows for double creation - ignoring...
                    if (mtd == null) {
                        mtd = method(o.getClass(), mtdName, args);

                        mtd.setAccessible(true);
                    }

                    return (R)mtd.invoke(o, args);
                }
                catch (Throwable e) {
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Creates absolute closure that will reflectively call a method with the given name on provided object.
     * <p>
     * Method reflects the typedef for {@link GridAbsClosure} which is {@link CA}.
     *
     * @param o Target object to call the method on.
     * @param mtdName Method name.
     * @param args Optional set of arguments for the method call.
     * @return Reflective absolute closure.
     * @throws GridClosureException Thrown in case of any reflective invocation errors.
     */
    public static GridAbsClosure caInvoke(final Object o, final String mtdName, @Nullable final Object... args) {
        A.notNull(o, "o", mtdName, "mtdName");

        return new CA() {
            /** */
            private Method mtd;

            @SuppressWarnings("unchecked")
            @Override public void apply() {
                try {
                    // No synchronization allows for double creation - ignoring...
                    if (mtd == null) {
                        mtd = method(o.getClass(), mtdName, args);

                        mtd.setAccessible(true);
                    }

                    mtd.invoke(o, args);
                }
                catch (Throwable e) {
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Creates out closure that will reflectively call a static method with the given name
     * and return result of that call.
     * <p>
     * Method reflects the typedef for {@link org.apache.ignite.lang.IgniteOutClosure} which is {@link CO}.
     *
     * @param cls Class to call a static method on.
     * @param mtdName Method name.
     * @param args Optional set of arguments for the method call.
     * @param <R> Type of closure return value.
     * @return Reflective out closure.
     * @throws GridClosureException Thrown in case of any reflective invocation errors.
     */
    public static <R> IgniteOutClosure<R> coInvoke(final Class<?> cls, final String mtdName,
        @Nullable final Object... args) {
        A.notNull(cls, "cls", mtdName, "mtdName");

        return new CO<R>() {
            /** */
            private Method mtd;

            @SuppressWarnings("unchecked")
            @Override public R apply() {
                try {
                    // No synchronization allows for double creation - ignoring...
                    if (mtd == null) {
                        mtd = method(cls, mtdName, args);

                        mtd.setAccessible(true);
                    }

                    return (R)mtd.invoke(null, args);
                }
                catch (Throwable e) {
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Creates absolute closure that will reflectively call a static method with the given name.
     * <p>
     * Method reflects the typedef for {@link GridAbsClosure} which is {@link CA}.
     *
     * @param cls Class to call a static method on.
     * @param mtdName Method name.
     * @param args Optional set of arguments for the method call.
     * @return Reflective absolute closure.
     * @throws GridClosureException Thrown in case of any reflective invocation errors.
     */
    public static GridAbsClosure caInvoke(final Class<?> cls, final String mtdName, @Nullable final Object... args) {
        A.notNull(cls, "cls", mtdName, "mtdName");

        return new CA() {
            /** */
            private Method mtd;

            @SuppressWarnings("unchecked")
            @Override public void apply() {
                try {
                    // No synchronization allows for double creation - ignoring...
                    if (mtd == null) {
                        mtd = method(cls, mtdName, args);

                        mtd.setAccessible(true);
                    }

                    mtd.invoke(null, args);
                }
                catch (Throwable e) {
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Looks up the method with given parameters.
     *
     * @param cls Class to look up in.
     * @param mtdName Method name to look up.
     * @param args Optional set of method parameters.
     * @return Method instance.
     * @throws Exception Thrown in case of any reflective errors.
     */
    private static Method method(Class<?> cls, String mtdName, @Nullable Object... args) throws Exception {
        assert cls != null;
        assert mtdName != null;

        int cnt = 0;

        Method m = null;

        for (Method mtd : cls.getDeclaredMethods())
            if (mtd.getName().equals(mtdName)) {
                cnt++;

                m = mtd;
            }

        if (cnt == 0)
            throw new NoSuchMethodException(cls.getName() + '#' + mtdName);

        // If there is only one method with provided name we
        // don't use lookup that requires parameters' types since
        // it is a lot more complex to deal with type inheritance there.
        if (cnt == 1)
            return m;

        if (!isEmpty(args)) {
            assert args != null;

            Class<?>[] types = new Class[args.length];

            int i = 0;

            for (Object arg : args) {
                // This is not going to work in cases when method expects
                // an interface or supertype. Accept this limitation for now...
                types[i++] = arg.getClass();
            }

            return cls.getDeclaredMethod(mtdName, types);
        }
        else
            return cls.getDeclaredMethod(mtdName);
    }

    /**
     * Gets closure that converts object to its runtime class.
     *
     * @return Closure that converts object to its runtime class.
     */
    public static IgniteClosure<Object, Class<?>> clazz() {
        return CLAZZ;
    }

    /**
     * Creates new collection by removing duplicates from the given collection.
     *
     * @param c Collection to remove duplicates from.
     * @param <T> Type of the collection.
     * @return De-duped collection.
     */
    public static <T> Collection<T> dedup(Collection<? extends T> c) {
        A.notNull(c, "c");

        Collection<T> set = new GridLeanSet<>();

        set.addAll(c);

        return set;
    }

    /**
     * Calculates sum of all elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @param c Collection of elements.
     * @return Sum of all elements.
     */
    public static int sumInt(Iterable<Integer> c) {
        A.notNull(c, "c");

        int sum = 0;

        for (int t : c) {
            sum += t;
        }

        return sum;
    }

    /**
     * Calculates sum of all elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @param c Collection of elements.
     * @return Sum of all elements.
     */
    public static double sumDouble(Iterable<Double> c) {
        A.notNull(c, "c");

        double sum = 0;

        for (double t : c) {
            sum += t;
        }

        return sum;
    }

    /**
     * Calculates sum of all elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @param c Collection of elements.
     * @return Sum of all elements.
     */
    public static BigDecimal sumBigDecimal(Iterable<BigDecimal> c) {
        A.notNull(c, "c");

        BigDecimal sum = BigDecimal.ZERO;

        for (BigDecimal t : c) {
            sum = sum.add(t);
        }

        return sum;
    }

    /**
     * Calculates sum of all elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @param c Collection of elements.
     * @return Sum of all elements.
     */
    public static BigInteger sumBigInt(Iterable<BigInteger> c) {
        A.notNull(c, "c");

        BigInteger sum = BigInteger.ZERO;

        for (BigInteger t : c) {
            sum = sum.add(t);
        }

        return sum;
    }

    /**
     * Calculates arithmetic mean.
     * <p>
     * <img src="{@docRoot}/img/avg.png">
     *
     * @param c Input collection.
     * @return Arithmetic mean of the input collection.
     */
    public static double avg(Iterable<? extends Number> c) {
        A.notNull(c, "c");

        double sum = 0;

        int i = 0;

        for (Number t : c) {
            sum += t.doubleValue();

            i++;
        }

        return sum / i;
    }

    /**
     * Gets reducer closure that calculates arithmetic mean.
     * <p>
     * <img src="{@docRoot}/img/avg.png">
     *
     * @return Reducer closure that calculated arithmetic mean.
     */
    public static <T extends Number> IgniteReducer<T, Double> avgReducer() {
        return new R1<T, Double>() {
            private double sum;
            private int i;

            private final Object lock = new Object();

            @Override public boolean collect(T e) {
                if (e != null)
                    synchronized (lock) {
                        sum += e.doubleValue();
                        i++;
                    }

                return true;
            }

            @Override public Double reduce() {
                synchronized (lock) {
                    return sum / i;
                }
            }
        };
    }

    /**
     * Calculates quadratic mean.
     * <p>
     * <img src="{@docRoot}/img/qavg.png">
     *
     * @param c Input collection.
     * @return Quadratic mean of the input collection.
     */
    public static double qavg(Iterable<? extends Number> c) {
        A.notNull(c, "c");

        double sum = 0;

        int i = 0;

        for (Number t : c) {
            double d = t.doubleValue();

            sum += d * d;

            i++;
        }

        return Math.sqrt(sum / i);
    }

    /**
     * Gets reducer closure that calculates quadratic mean.
     * <p>
     * <img src="{@docRoot}/img/qavg.png">
     *
     * @return Reducer closure that calculated quadratic mean.
     */
    public static <T extends Number> IgniteReducer<T, Double> qavgReducer() {
        return new R1<T, Double>() {
            private double sum;
            private int i;

            private final Object lock = new Object();

            @Override public boolean collect(T e) {
                if (e != null) {
                    double d = e.doubleValue();

                    synchronized (lock) {
                        sum += d * d;

                        i++;
                    }
                }

                return true;
            }

            @Override public Double reduce() {
                synchronized (lock) {
                    return Math.sqrt(sum / i);
                }
            }
        };
    }

    /**
     * Calculates geometric mean.
     * <p>
     * <img src="{@docRoot}/img/gavg.png">
     *
     * @param c Input collection.
     * @return Geometric mean of the input collection.
     */
    public static double gavg(Iterable<? extends Number> c) {
        A.notNull(c, "c");

        double sum = 0;

        int i = 0;

        for (Number t : c) {
            sum *= t.doubleValue();

            i++;
        }

        return Math.pow(sum, 1f / i);
    }

    /**
     * Gets reducer closure that calculates geometric mean.
     * <p>
     * <img src="{@docRoot}/img/gavg.png">
     *
     * @return Reducer closure that calculated geometric mean.
     */
    public static <T extends Number> IgniteReducer<T, Double> gavgReducer() {
        return new R1<T, Double>() {
            private double sum;
            private int i;

            private final Object lock = new Object();

            @Override public boolean collect(T e) {
                if (e != null)
                    synchronized (lock) {
                        sum *= e.doubleValue();

                        i++;
                    }

                return true;
            }

            @Override public Double reduce() {
                synchronized (lock) {
                    return Math.pow(sum, 1f / i);
                }
            }
        };
    }

    /**
     * Calculates weighted mean.
     * <p>
     * <img src="{@docRoot}/img/wavg.png">
     *
     * @param c Collection of elements.
     * @param w Collection of weights.
     * @return Weighted mean of the input collection.
     */
    public static double wavg(Collection<? extends Number> c, Collection<? extends Number> w) {
        A.notNull(c, "c", w, "w");
        A.ensure(c.size() == w.size(), "c.size() == w.size()");

        double sumC = 0;
        double sumW = 0;

        Iterator<? extends Number> iterC = c.iterator();
        Iterator<? extends Number> iterW = w.iterator();

        while (iterC.hasNext()) {
            assert iterW.hasNext();

            double dc = iterC.next().doubleValue();
            double dw = iterW.next().doubleValue();

            sumW += dw;
            sumC += dw * dc;
        }

        return sumC / sumW;
    }

    /**
     * Calculates harmonic mean.
     * <p>
     * <img src="{@docRoot}/img/havg.png">
     *
     * @param c Input collection.
     * @return Harmonic mean of the input collection.
     */
    public static double havg(Iterable<? extends Number> c) {
        A.notNull(c, "c");

        double sum = 0;

        int i = 0;

        for (Number t : c) {

            sum += 1 / t.doubleValue();

            i++;
        }

        return i / sum;
    }

    /**
     * Gets reducer closure that collects only a single value and returns it
     * without any transformations.
     *
     * @return Reducer closure that collects and returns single value.
     */
    public static <T> IgniteReducer<T, T> singleReducer() {
        return new R1<T, T>() {
            private T obj;

            @Override public boolean collect(T e) {
                // No synchronization needed here.
                obj = e;

                return false;
            }

            @Override public T reduce() {
                return obj;
            }
        };
    }

    /**
     * Gets reducer which always returns {@code true} from {@link org.apache.ignite.lang.IgniteReducer#collect(Object)}
     * method and passed in {@code element} from {@link org.apache.ignite.lang.IgniteReducer#reduce()} method.
     *
     * @param elem Element to return from {@link org.apache.ignite.lang.IgniteReducer#reduce()} method.
     * @param <T> Reducer element type.
     * @param <R> Return element type.
     * @return Passed in element.
     */
    public static <T, R> IgniteReducer<T, R> continuousReducer(final R elem) {
        return new R1<T, R>() {
            @Override public boolean collect(T e) {
                return true;
            }

            @Override public R reduce() {
                return elem;
            }
        };
    }

    /**
     * Gets reducer which always returns {@code true} from {@link org.apache.ignite.lang.IgniteReducer#collect(Object)}
     * method and passed in {@code element} from {@link org.apache.ignite.lang.IgniteReducer#reduce()} method.
     *
     * @param elem Element to return from {@link org.apache.ignite.lang.IgniteReducer#reduce()} method.
     * @param <T> Reducer element type.
     * @return Passed in element.
     */
    public static <T> IgniteReducer<T, T> identityReducer(final T elem) {
        return new R1<T, T>() {
            @Override public boolean collect(T e) {
                return true;
            }

            @Override public T reduce() {
                return elem;
            }
        };
    }

    /**
     * Gets reducer closure that calculates harmonic mean.
     * <p>
     * <img src="{@docRoot}/img/havg.png">
     *
     * @return Reducer closure that calculated harmonic mean.
     */
    public static <T extends Number> IgniteReducer<T, Double> havgReducer() {
        return new R1<T, Double>() {
            private double sum;
            private int i;

            private final Object lock = new Object();

            @Override public boolean collect(T e) {
                if (e != null)
                    synchronized (lock) {
                        sum += 1 / e.doubleValue();

                        i++;
                    }

                return true;
            }

            @Override public Double reduce() {
                synchronized (lock) {
                    return i / sum;
                }
            }
        };
    }

    /**
     * Gets reducer closure that calculates sum of integer elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @return Reducer that calculates sum of integer elements.
     */
    public static IgniteReducer<Integer, Integer> sumIntReducer() {
        return new R1<Integer, Integer>() {
            private AtomicInteger sum = new AtomicInteger(0);

            @Override public boolean collect(Integer e) {
                if (e != null)
                    sum.addAndGet(e);

                return true;
            }

            @Override public Integer reduce() {
                return sum.get();
            }
        };
    }

    /**
     * Gets reducer closure that calculates sum of long integer elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @return Reducer that calculates sum of long integer elements.
     */
    public static IgniteReducer<Long, Long> sumLongReducer() {
        return new R1<Long, Long>() {
            private AtomicLong sum = new AtomicLong(0);

            @Override public boolean collect(Long e) {
                if (e != null)
                    sum.addAndGet(e);

                return true;
            }

            @Override public Long reduce() {
                return sum.get();
            }
        };
    }

    /**
     * Gets reducer closure that calculates sum of all elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @return Reducer that calculates sum of all elements.
     */
    @SuppressWarnings("unchecked")
    public static IgniteReducer<Double, Double> sumDoubleReducer() {
        return new R1<Double, Double>() {
            private double sum;

            private final Object lock = new Object();

            @Override public boolean collect(Double e) {
                if (e != null)
                    synchronized (lock) {
                        sum += e;
                    }

                return true;
            }

            @Override public Double reduce() {
                synchronized (lock) {
                    return sum;
                }
            }
        };
    }

    /**
     * Creates a range list containing numbers in given range.
     *
     * @param fromIncl Inclusive start of the range.
     * @param toExcl Exclusive stop of the range.
     * @return List containing numbers in range.
     */
    public static List<Integer> range(int fromIncl, int toExcl) {
        A.ensure(fromIncl >= 0, "fromIncl >= 0");
        A.ensure(toExcl >= 0, "toExcl >= 0");
        A.ensure(toExcl >= fromIncl, "toExcl > fromIncl");

        if (toExcl == fromIncl)
            return Collections.emptyList();

        List<Integer> list = new ArrayList<>(toExcl - fromIncl);

        for (int i = fromIncl; i < toExcl; i++)
            list.add(i);

        return list;
    }

    /**
     * Gets reducer closure that calculates sum of all elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @return Reducer that calculates sum of all elements.
     */
    @SuppressWarnings("unchecked")
    public static IgniteReducer<BigDecimal, BigDecimal> sumBigDecimalReducer() {
        return new R1<BigDecimal, BigDecimal>() {
            private BigDecimal sum = BigDecimal.ZERO;

            private final Object lock = new Object();

            @Override public boolean collect(BigDecimal e) {
                if (e != null)
                    synchronized (lock) {
                        sum = sum.add(e);
                    }

                return true;
            }

            @Override public BigDecimal reduce() {
                synchronized (lock) {
                    return sum;
                }
            }
        };
    }

    /**
     * Gets reducer closure that calculates sum of all elements.
     * <p>
     * <img src="{@docRoot}/img/sum.png">
     *
     * @return Reducer that calculates sum of all elements.
     */
    @SuppressWarnings("unchecked")
    public static IgniteReducer<BigInteger, BigInteger> sumBigIntegerReducer() {
        return new R1<BigInteger, BigInteger>() {
            private BigInteger sum = BigInteger.ZERO;

            private final Object lock = new Object();

            @Override public boolean collect(BigInteger e) {
                if (e != null)
                    synchronized (lock) {
                        sum = sum.add(e);
                    }

                return true;
            }

            @Override public BigInteger reduce() {
                synchronized (lock) {
                    return sum;
                }
            }
        };
    }

    /**
     * Gets reducer closure that concatenates strings using provided delimiter.
     *
     * @param delim Delimiter (optional).
     * @return Reducer that concatenates strings using provided delimeter.
     */
    public static IgniteReducer<String, String> concatReducer(@Nullable final String delim) {
        return new R1<String, String>() {
            private SB sb = new SB();

            private boolean first = true;

            private final Object lock = new Object();

            @Override public boolean collect(String s) {
                synchronized (lock) {
                    if (!first && !isEmpty(delim))
                        sb.a(delim);

                    sb.a(s);

                    first = false;
                }

                return true;
            }

            @Override public String reduce() {
                synchronized (lock) {
                    return sb.toString();
                }
            }
        };
    }

    /**
     * Concatenates strings using provided delimiter.
     *
     * @param c Input collection.
     * @param delim Delimiter (optional).
     * @return Concatenated string.
     */
    public static String concat(Iterable<String> c, @Nullable String delim) {
        A.notNull(c, "c");

        return reduce(c, concatReducer(delim));
    }

    /**
     * Gets collections of data items from grid job res casted to specified type.
     * <p>
     * Here's the typical example of how this method is used in {@code reduce()} method
     * implementation (this example sums up all the values of {@code Integer} type):
     * <pre name="code" class="java">
     * public Integer reduce(List&lt;GridComputeJobResult&gt; res) throws IgniteCheckedException {
     *     return F.sum(F.&lt;Integer&gt;jobResults(res));
     * }
     * </pre>
     * <p>
     * Note that this method doesn't create a new collection but simply iterates over the input one.
     *
     * @param res Collection of grid job res.
     * @param <T> Type of the data item to cast to. See {@link org.apache.ignite.compute.ComputeJobResult#getData()} method.
     * @return Collections of data items casted to type {@code T}.
     * @see org.apache.ignite.compute.ComputeJobResult#getData()
     */
    public static <T> Collection<T> jobResults(@Nullable Collection<? extends ComputeJobResult> res) {
        if (isEmpty(res))
            return Collections.emptyList();

        assert res != null;

        Collection<T> c = new ArrayList<>(res.size());

        for (ComputeJobResult r : res)
            c.add(r.<T>getData());

        return c;
    }

    /**
     * Convenient utility method that returns collection of node IDs for a given
     * collection of grid nodes.
     * <p>
     * Note that this method doesn't create a new collection but simply iterates
     * over the input one.
     *
     * @param nodes Collection of grid nodes.
     * @return Collection of node IDs for given collection of grid nodes.
     */
    public static Collection<UUID> nodeIds(@Nullable Collection<? extends ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty())
            return Collections.emptyList();

        return F.viewReadOnly(nodes, node2id());
    }

    /**
     * Convenient utility method that returns collection of node ID8s for a given
     * collection of grid nodes. ID8 is a shorter string representation of node ID,
     * mainly the first 8 characters.
     * <p>
     * Note that this method doesn't create a new collection but simply iterates
     * over the input one.
     *
     * @param nodes Collection of grid nodes.
     * @return Collection of node IDs for given collection of grid nodes.
     */
    public static Collection<String> nodeId8s(@Nullable Collection<? extends ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty())
            return Collections.emptyList();

        return F.viewReadOnly(nodes, node2id8());
    }

    /**
     * Convenient utility method that returns collection of node ID8s for a given
     * collection of node IDs. ID8 is a shorter string representation of node ID,
     * mainly the first 8 characters.
     * <p>
     * Note that this method doesn't create a new collection but simply iterates
     * over the input one.
     *
     * @param ids Collection of nodeIds.
     * @return Collection of node IDs for given collection of grid nodes.
     */
    public static Collection<String> id8s(@Nullable Collection<UUID> ids) {
        if (ids == null || ids.isEmpty())
            return Collections.emptyList();

        return F.viewReadOnly(ids, id2id8());
    }

    /**
     * Convenient utility method that returns collection of node attributes for a given
     * collection of grid nodes.
     * <p>
     * Note that this method doesn't create a new collection but simply iterates over the input one.
     *
     * @param nodes Collection of grid nodes.
     * @param attr Name of the attribute to return from each node.
     * @param <T> Type of the attribute.
     * @return Collection of node attributes for given collection of grid nodes.
     */
    public static <T> Collection<T> nodeAttributes(Collection<? extends ClusterNode> nodes, String attr) {
        A.notNull(nodes, "nodes", attr, "attr");

        Collection<T> c = new ArrayList<>(nodes.size());

        for (ClusterNode n : nodes)
            c.add(n.<T>attribute(attr));

        return c;
    }

    /**
     * Gets closure that calls {@code System.out.println()} on its bound variable.
     *
     * @param <T> Type of the bound variable.
     * @return Closure that calls {@code System.out.println()} on its bound variable.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteInClosure<T> println() {
        return (IgniteInClosure<T>)PRINTLN;
    }

    /**
     * Creates absolute closure that does <tt>System.out.println(msg)</tt>.
     *
     * @param msg Message to print.
     * @return Absolute closure that print message.
     */
    public static GridAbsClosure println(final String msg) {
        return new CA() {
            @Override public void apply() {
                System.out.println(msg);
            }
        };
    }

    /**
     * Creates absolute closure that does <tt>System.out.print(msg)</tt>.
     *
     * @param msg Message to print.
     * @return Absolute closure that print message.
     */
    public static GridAbsClosure print(final String msg) {
        return new CA() {
            @Override public void apply() {
                System.out.print(msg);
            }
        };
    }

    /**
     * Gets closure that prints out its bound variable.
     *
     * @param pre String value to print before each variable.
     * @param post String value to print after each variable.
     * @param <T> Type of the bound variable.
     * @return Closure that calls {@code System.out.print(pre); System.out.print(t); System.out.println(post)}
     *      on its bound variable.
     */
    public static <T> IgniteInClosure<T> println(@Nullable final String pre, @Nullable final String post) {
        return new CI1<T>() {
            @Override public void apply(T t) {
                String sPre = pre == null ? "" : pre;
                String sPost = post == null ? "" : post;

                System.out.println(sPre + t + sPost);
            }
        };
    }

    /**
     * Gets closure that prints out its bound variable.
     *
     * @param fmt Format string as for {@link PrintStream#printf(String, Object...)} method.
     * @param <T> Type of the bound variable.
     * @return Closure that prints out its bound variable.
     */
    public static <T> IgniteInClosure<T> printf(final String fmt) {
        return new CI1<T>() {
            @Override public void apply(T t) {
                System.out.printf(fmt, t);
            }
        };
    }

    /**
     * Gets closure that prints out its bound variable
     *
     * @return Closure that prints out its bound variable.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteInClosure<T> print() {
        return (IgniteInClosure<T>)PRINT;
    }

    /**
     * Gets closure that prints out its bound variable.
     *
     * @param pre String value to print before each variable.
     * @param post String value to print after each variable.
     * @return Closure that prints out its bound variable.
     */
    public static <T> IgniteInClosure<T> print(@Nullable final String pre, @Nullable final String post) {
        return new CI1<T>() {
            @Override public void apply(T t) {
                String sPre = pre == null ? "" : pre;
                String sPost = post == null ? "" : post;

                System.out.print(sPre + t + sPost);
            }
        };
    }

    /**
     * Gets random value from given collection.
     *
     * @param c Input collection (no {@code null} and not emtpy).
     * @param <T> Type of the collection.
     * @return Random value from the input collection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static <T> T rand(Collection<? extends T> c) {
        A.notNull(c, "c");

        int n = ThreadLocalRandom8.current().nextInt(c.size());

        int i = 0;

        for (T t : c) {
            if (i++ == n)
                return t;
        }

        throw new ConcurrentModificationException();
    }

    /**
     * Gets random value from given collection which may be modified concurrently.
     *
     * @param c Input collection.
     * @param <T> Type of the collection.
     * @return Random value from the input collection.
     */
    @Nullable public static <T> T randConcurrent(Collection<? extends T> c) {
        A.notNull(c, "c");

        int size = c.size();

        if (size == 0)
            return null;

        int n = ThreadLocalRandom8.current().nextInt(size);

        int i = 0;

        T res = null;

        for (T t : c) {
            if (i++ == n)
                return t;

            res = t;
        }

        return res;
    }

    /**
     * Gets random value from given list. For random-access lists this
     * operation is O(1), otherwise O(n).
     *
     * @param l Input collection.
     * @param <T> Type of the list elements.
     * @return Random value from the input list.
     */
    public static <T> T rand(List<T> l) {
        A.notNull(l, "l");

        return l.get(ThreadLocalRandom8.current().nextInt(l.size()));
    }

    /**
     * Gets random value from given array. This operation
     * does not iterate through array elements and returns immediately.
     *
     * @param c Input collection.
     * @param <T> Type of the collection.
     * @return Random value from the input collection.
     */
    public static <T> T rand(T... c) {
        A.notNull(c, "c");

        return c[ThreadLocalRandom8.current().nextInt(c.length)];
    }

    /**
     * Concatenates an element to a collection. If {@code copy} flag is {@code true}, then
     * a new collection will be created and the element and passed in collection will be
     * copied into the new one. The returned collection will be modifiable. If {@code copy}
     * flag is {@code false}, then a read-only view will be created over the element and given
     * collections and no copying will happen.
     *
     * @param cp Copy flag.
     * @param t First element.
     * @param c Second collection.
     * @param <T> Element type.
     * @return Concatenated collection.
     */
    public static <T> Collection<T> concat(boolean cp, @Nullable final T t, @Nullable final Collection<T> c) {
        if (cp) {
            if (isEmpty(c)) {
                Collection<T> l = new ArrayList<>(1);

                l.add(t);

                return l;
            }

            assert c != null;

            Collection<T> ret = new ArrayList<>(c.size() + 1);

            ret.add(t);
            ret.addAll(c);

            return ret;
        }
        else {
            if (isEmpty(c))
                return Collections.singletonList(t);

            assert c != null;

            return new GridSerializableCollection<T>() {
                @NotNull
                @Override public Iterator<T> iterator() {
                    return new GridSerializableIterator<T>() {
                        private Iterator<T> it;

                        @Override public boolean hasNext() {
                            return it == null || it.hasNext();
                        }

                        @Nullable @Override public T next() {
                            if (it == null) {
                                it = c.iterator();

                                return t;
                            }

                            return it.next();
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override public int size() {
                    return c.size() + 1;
                }

                @Override public boolean equals(Object obj) {
                    return obj instanceof Collection && eqNotOrdered(this, (Collection)obj);
                }
            };
        }
    }

    /**
     * Concatenates 2 collections into one. If {@code copy} flag is {@code true}, then
     * a new collection will be created and these collections will be copied into the
     * new one. The returned collection will be modifiable. If {@code copy} flag is
     * {@code false}, then a read-only view will be created over given collections
     * and no copying will happen.
     *
     * @param cp Copy flag.
     * @param c1 First collection.
     * @param c2 Second collection.
     * @param <T> Element type.
     * @return Concatenated {@code non-null} collection.
     */
    public static <T> Collection<T> concat(boolean cp, @Nullable final Collection<T> c1,
        @Nullable final Collection<T> c2) {
        if (cp) {
            if (isEmpty(c1) && isEmpty(c2))
                return new ArrayList<>(0);

            if (isEmpty(c1))
                return new ArrayList<>(c2);

            if (isEmpty(c2))
                return new ArrayList<>(c1);

            assert c1 != null && c2 != null;

            Collection<T> c = new ArrayList<>(c1.size() + c2.size());

            c.addAll(c1);
            c.addAll(c2);

            return c;
        }
        else {
            if (isEmpty(c1) && isEmpty(c2))
                return Collections.emptyList();

            if (isEmpty(c1) || isEmpty(c2)) {
                Collection<T> c = isEmpty(c1) ? c2 : c1;

                assert c != null;

                return c;
            }

            assert c1 != null && c2 != null;

            return new GridSerializableCollection<T>() {
                @NotNull
                @Override public Iterator<T> iterator() {
                    return new GridSerializableIterator<T>() {
                        private Iterator<T> it1 = c1.iterator();
                        private Iterator<T> it2 = c2.iterator();

                        @Override public boolean hasNext() {
                            if (it1 != null)
                                if (!it1.hasNext())
                                    it1 = null;
                                else
                                    return true;

                            return it2.hasNext();
                        }

                        @Override public T next() {
                            return it1 != null ? it1.next() : it2.next();
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override public boolean contains(Object o) {
                    return c1.contains(o) || c2.contains(o);
                }

                @Override public int size() {
                    return c1.size() + c2.size();
                }

                @Override public boolean equals(Object obj) {
                    return obj instanceof Collection && eqNotOrdered(this, (Collection<?>)obj);
                }
            };
        }
    }

    /**
     * Concatenates an elements to an array.
     *
     * @param arr Array.
     * @param obj One or more elements.
     * @return Concatenated array.
     */
    public static <T> T[] concat(@Nullable T[] arr, T... obj) {
        T[] newArr;

        if (arr == null || arr.length == 0)
            newArr = obj;
        else {
            newArr = Arrays.copyOf(arr, arr.length + obj.length);

            System.arraycopy(obj, 0, newArr, arr.length, obj.length);
        }

        return newArr;
    }

    /**
     * Concatenates multiple iterators as single one.
     *
     * @param iters Iterators.
     * @return Single iterator.
     */
    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> concat(Iterator<T> ... iters) {
        if (iters.length == 1)
            return iters[0];

        return concat(asList(iters).iterator());
    }

    /**
     * Concatenates multiple iterators as single one.
     *
     * @param iters Iterator over iterators.
     * @return Single iterator.
     */
    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> concat(final Iterator<Iterator<T>> iters) {
        if (!iters.hasNext())
            return Collections.<T>emptySet().iterator();

        return new Iterator<T>() {
            private Iterator<T> it = iters.next();

            private Iterator<T> last;

            private T next;

            {
                advance();
            }

            private void advance() {
                for (;;) {
                    if (it.hasNext()) {
                        next = it.next();

                        assert next != null;

                        return;
                    }

                    if (!iters.hasNext())
                        return;

                    it = iters.next();
                }
            }

            @Override public boolean hasNext() {
                return next != null;
            }

            @Override public T next() {
                T res = next;

                if (res == null)
                    throw new NoSuchElementException();

                next = null;

                last = it;

                advance();

                return res;
            }

            @Override public void remove() {
                if (last == null)
                    throw new IllegalStateException();

                last.remove();
            }
        };
    }

    /**
     * Loses all elements in input collection that are contained in {@code filter} collection.
     *
     * @param c Input collection.
     * @param cp If {@code true} method creates new collection not modifying input,
     *      otherwise does <tt>in-place</tt> modifications.
     * @param filter Filter collection. If {@code filter} collection is empty or
     *      {@code null} - no elements are lost.
     * @param <T> Type of collections.
     * @return Collection of remaining elements
     */
    public static <T0, T extends T0> Collection<T> lose(Collection<T> c, boolean cp,
        @Nullable Collection<T0> filter) {
        A.notNull(c, "c");

        return lose(c, cp, F0.in(filter));
    }

    /**
     * Loses all elements in input collection that are evaluated to {@code true} by
     * all given predicates.
     *
     * @param c Input collection.
     * @param cp If {@code true} method creates new collection without modifying the input one,
     *      otherwise does <tt>in-place</tt> modifications.
     * @param p Predicates to filter by. If no predicates provided - no elements are lost.
     * @param <T> Type of collections.
     * @return Collection of remaining elements.
     */
    public static <T> Collection<T> lose(Collection<T> c, boolean cp, @Nullable IgnitePredicate<? super T>... p) {
        A.notNull(c, "c");

        Collection<T> res;

        if (!cp) {
            res = c;

            if (isEmpty(p))
                res.clear();
            else if (!isAlwaysFalse(p))
                for (Iterator<T> iter = res.iterator(); iter.hasNext();)
                    if (isAll(iter.next(), p))
                        iter.remove();
        }
        else {
            res = new LinkedList<>();

            if (!isEmpty(p) && !isAlwaysTrue(p))
                for (T t : c)
                    if (!isAll(t, p))
                        res.add(t);
        }

        return res;
    }

    /**
     * Loses up to first {@code num} elements of the input collection.
     *
     * @param c Input collection.
     * @param cp If {@code true} method creates new collection not modifying input,
     *      otherwise does <tt>in-place</tt> modifications.
     * @param num Maximum number of elements to lose (the actual number can be
     *      less if the input collection contains less elements).
     * @param <T> Type of the collections.
     * @return Collection of remaining elements.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> Collection<T> lose(Collection<? extends T> c, boolean cp, int num) {
        A.notNull(c, "c");
        A.ensure(num >= 0, "num >= 0");

        Collection<T> res;

        if (!cp) {
            res = (Collection<T>)c;

            if (num >= c.size())
                res.clear();
            else {
                int i = 0;

                for (Iterator<T> iter = res.iterator(); iter.hasNext();) {
                    iter.next();

                    if (i++ < num)
                        iter.remove();
                    else
                        break;
                }
            }
        }
        else {
            if (num >= c.size())
                return Collections.emptyList();

            res = new ArrayList<>(c.size() - num);

            int i = 0;

            for (T t : c) {
                if (i++ >= num)
                    res.add(t);
            }
        }

        return res;
    }

    /**
     * Loses all entries in input map that are evaluated to {@code true} by all given predicates.
     *
     * @param m Map to filter.
     * @param cp If {@code true} method creates new map not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param p Optional set of predicates to use for filtration. If none provided - original map
     *  will (or its copy) be returned.
     * @param <K> Type of the free variable for the predicate and type of map's keys.
     * @param <V> Type of the free variable for the predicate and type of map's values.
     * @return Filtered map.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> Map<K, V> lose(Map<K, V> m, boolean cp,
        @Nullable IgnitePredicate<? super Map.Entry<K, V>>... p) {
        A.notNull(m, "m");

        Map<K, V> res;

        if (!cp) {
            res = m;

            if (isEmpty(p))
                res.clear();
            else if (!isAlwaysFalse(p))
                for (Iterator<Map.Entry<K, V>> iter = m.entrySet().iterator(); iter.hasNext();)
                    if (isAll(iter.next(), p))
                        iter.remove();
        }
        else {
            res = U.newHashMap(m.size());

            if (!isEmpty(p) && !isAlwaysTrue(p))
                for (Map.Entry<K, V> e : m.entrySet())
                    if (!F.isAll(e, p))
                        res.put(e.getKey(), e.getValue());
        }

        return res;
    }

    /**
     * Loses all entries in input map which keys are evaluated to {@code true} by all
     * given predicates.
     *
     * @param m Map to filter.
     * @param cp If {@code true} method creates new map not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param p Optional set of predicates to use for filtration. If none provided - original
     *      map (or its copy) will be returned.
     * @param <K> Type of the free variable for the predicate and type of map's keys.
     * @param <V> Type of map's values.
     * @return Filtered map.
     */
    public static <K, V> Map<K, V> loseKeys(
        Map<K, V> m,
        boolean cp,
        @Nullable final IgnitePredicate<? super K>... p
    ) {
        return lose(m, cp, new P1<Map.Entry<K, V>>() {
            @Override public boolean apply(Map.Entry<K, V> e) {
                return isAll(e.getKey(), p);
            }
        });
    }

    /**
     * Loses all entries in input map which values are evaluated to {@code true} by all
     * given predicates.
     *
     * @param m Map to filter.
     * @param cp If {@code true} method creates new map not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param p Optional set of predicates to use for filtration. If none provided - original
     *      map (or its copy) will be returned.
     * @param <K> Type of the free variable for the predicate and type of map's keys.
     * @param <V> Type of map's values.
     * @return Filtered map.
     */
    public static <K, V> Map<K, V> loseValues(Map<K, V> m, boolean cp,
        @Nullable final IgnitePredicate<? super V>... p) {
        return lose(m, cp, new P1<Map.Entry<K, V>>() {
            @Override public boolean apply(Map.Entry<K, V> e) {
                return isAll(e.getValue(), p);
            }
        });
    }

    /**
     * Loses all elements in input list that are contained in {@code filter} collection.
     *
     * @param c Input list.
     * @param cp If {@code true} method creates new list not modifying input,
     *      otherwise does <tt>in-place</tt> modifications.
     * @param filter Filter collection. If {@code filter} collection is empty or
     *      {@code null} - no elements are lost.
     * @param <T> Type of list.
     * @return List of remaining elements
     */
    public static <T> List<T> loseList(List<T> c, boolean cp, @Nullable Collection<? super T> filter) {
        A.notNull(c, "c");

        List<T> res;

        if (!cp) {
            res = c;

            if (filter != null)
                res.removeAll(filter);
        }
        else {
            res = new LinkedList<>();

            for (T t : c) {
                if (filter == null || !filter.contains(t))
                    res.add(t);
            }
        }

        return res;
    }

    /**
     * Loses all elements in input list for which any of the predicates evaluate to {@code true}.
     *
     * @param c Input list.
     * @param cp If {@code true} method creates new list not modifying input,
     *      otherwise does <tt>in-place</tt> modifications.
     * @param p Looses all elements for which any of the predicates evaluate to {@code true}.
     * @param <T> Type of list.
     * @return List of remaining elements
     */
    public static <T> List<T> filterList(List<T> c, boolean cp, @Nullable IgnitePredicate<T>... p) {
        A.notNull(c, "c");

        List<T> res;

        if (!cp) {
            res = c;

            if (p != null)
                for (Iterator<T> it = c.iterator(); it.hasNext();)
                    if (isAny(it.next(), p))
                        it.remove();
        }
        else {
            res = new ArrayList<>(c.size());

            for (T t : c)
                if (!isAny(t, p))
                    res.add(t);
        }

        return res;
    }

    /**
     * Loses all elements in input set that are contained in {@code filter} collection.
     *
     * @param c Input set.
     * @param cp If {@code true} method creates new list not modifying input,
     *      otherwise does <tt>in-place</tt> modifications.
     * @param filter Filter collection. If {@code filter} collection is empty or
     *      {@code null} - no elements are lost.
     * @param <T> Type of set.
     * @return Set of remaining elements
     */
    public static <T> Set<T> loseSet(Set<T> c, boolean cp, @Nullable Collection<? super T> filter) {
        A.notNull(c, "c");

        Set<T> res;

        if (!cp) {
            res = c;

            if (filter != null)
                res.removeAll(filter);
        }
        else {
            res = new LinkedHashSet<>();

            for (T t : c) {
                if (filter == null || !filter.contains(t))
                    res.add(t);
            }
        }

        return res;
    }

    /**
     * Removes elements <tt>in-place</tt> from given collection. All elements for which
     * all given predicates, if any, evaluate to {@code true} will be removed from the
     * passed in collection. Note that collection must support {@link Iterator#remove() removal}.
     *
     * @param c Input collection that should support {@link Iterator#remove() removal}.
     * @param p Optional set of predicates. If no predicates provided - no elements will be
     *      dropped and input collection will be returned.
     * @param <T> Type of the collection.
     * @return Input collection with some elements potentially removed.
     */
    public static <T> Iterable<T> drop(Iterable<T> c, @Nullable IgnitePredicate<? super T>... p) {
        A.notNull(c, "c");

        if (isEmpty(p) || isAlwaysFalse(p))
            return c;

        for (Iterator<T> i = c.iterator(); i.hasNext();)
            if (isAll(i.next(), p))
                i.remove();

        return c;
    }

    /**
     * Gets closure which converts node to node ID.
     *
     * @return Closure which converts node to node ID.
     */
    public static IgniteClosure<ClusterNode, UUID> node2id() {
        return NODE2ID;
    }

    /**
     * Gets closure which converts node to node ID8 representation (shorter and good enough).
     *
     * @return Closure which converts node to node ID8 representation (shorter and good enough).
     */
    public static IgniteClosure<ClusterNode, String> node2id8() {
        return NODE2ID8;
    }

    /**
     * Gets closure which converts node ID to node ID8 representation (shorter and good enough).
     *
     * @return Closure which converts node ID to node ID8 representation (shorter and good enough).
     */
    public static IgniteClosure<UUID, String> id2id8() {
        return ID2ID8;
    }

    /**
     * Creates grid node predicate evaluating on the given node ID.
     *
     * @param nodeId Node ID for which returning predicate will evaluate to {@code true}.
     * @return Grid node predicate evaluating on the given node ID.
     * @see #idForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static <T extends ClusterNode> IgnitePredicate<T> nodeForNodeId(final UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        return new P1<T>() {
            @Override public boolean apply(ClusterNode e) {
                return e.id().equals(nodeId);
            }
        };
    }

    /**
     * Creates grid node predicate evaluating on the given node IDs.
     *
     * @param nodeIds Collection of node IDs.
     * @return Grid node predicate evaluating on the given node IDs.
     * @see #idForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static <T extends ClusterNode> IgnitePredicate<T> nodeForNodeIds(@Nullable final Collection<UUID>
        nodeIds) {
        if (isEmpty(nodeIds))
            return alwaysFalse();

        assert nodeIds != null;

        return new P1<T>() {
            @Override public boolean apply(ClusterNode e) {
                return nodeIds.contains(e.id());
            }
        };
    }

    /**
     * Creates grid node predicate evaluating on the given node IDs.
     *
     * @param nodeIds Collection of node IDs.
     * @return Grid node predicate evaluating on the given node IDs.
     * @see #idForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static <T extends ClusterNode> IgnitePredicate<T> nodeForNodeIds(@Nullable final UUID... nodeIds) {
        if (isEmpty(nodeIds))
            return alwaysFalse();

        return new P1<T>() {
            private final UUID[] ids;

            {
                assert nodeIds != null;

                Arrays.sort(nodeIds);

                ids = Arrays.copyOf(nodeIds, nodeIds.length);
            }

            @Override public boolean apply(ClusterNode e) {
                return Arrays.binarySearch(ids, e.id()) >= 0;
            }
        };
    }

    /**
     * Creates {@link UUID} predicate evaluating on the given node ID.
     *
     * @param nodeId Node ID for which returning predicate will evaluate to {@code true}.
     * @return {@link UUID} predicate evaluating on the given node ID.
     * @see #nodeForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static IgnitePredicate<UUID> idForNodeId(final UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        return new P1<UUID>() {
            @Override public boolean apply(UUID id) {
                return id.equals(nodeId);
            }
        };
    }

    /**
     * Creates {@link UUID} predicate evaluating on the given node IDs.
     *
     * @param nodeIds Collection of node IDs.
     * @return {@link UUID} predicate evaluating on the given node IDs.
     * @see #nodeForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static IgnitePredicate<UUID> idForNodeIds(@Nullable final Collection<UUID> nodeIds) {
        if (isEmpty(nodeIds))
            return alwaysFalse();

        assert nodeIds != null;

        return new P1<UUID>() {
            @Override public boolean apply(UUID id) {
                return nodeIds.contains(id);
            }
        };
    }

    /**
     * Creates {@link UUID} predicate evaluating on the given node IDs.
     *
     * @param nodeIds Collection of node IDs.
     * @return {@link UUID} predicate evaluating on the given node IDs.
     * @see #nodeForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static IgnitePredicate<UUID> idForNodeIds(@Nullable final UUID... nodeIds) {
        if (isEmpty(nodeIds))
            return alwaysFalse();

        return new P1<UUID>() {
            private final UUID[] ids;

            {
                assert nodeIds != null;

                Arrays.sort(nodeIds);

                ids = Arrays.copyOf(nodeIds, nodeIds.length);
            }

            @Override public boolean apply(UUID id) {
                return Arrays.binarySearch(ids, id) >= 0;
            }
        };
    }

    /**
     * Creates predicates that evaluates to {@code true} for each node in given collection.
     * Note that is collection is empty the result predicate will always evaluate to {@code false}.
     * Implementation simply creates {@link GridNodePredicate} instance.
     *
     * @param nodes Collection of nodes. If none provided - result predicate will always
     *      return {@code false}.
     * @return Predicates that evaluates to {@code true} for each node in given collection.
     */
    public static IgnitePredicate<ClusterNode> nodeForNodes(@Nullable Collection<? extends ClusterNode> nodes) {
        return new GridNodePredicate(nodeIds(nodes));
    }

    /**
     * Creates predicates that evaluates to {@code true} for each node in given collection.
     * Note that if collection is empty the result predicate will always evaluate to {@code false}.
     * Implementation simply creates {@link GridNodePredicate} instance.
     *
     * @param nodes Collection of nodes. If none provided - result predicate will always
     *      return {@code false}.
     * @return Predicates that evaluates to {@code true} for each node in given collection.
     */
    public static IgnitePredicate<ClusterNode> nodeForNodes(ClusterNode... nodes) {
        return new GridNodePredicate(nodes);
    }

    /**
     * Retains all elements in input collection that are contained in {@code filter}.
     *
     * @param c Input collection.
     * @param cp If {@code true} method creates collection not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param filter Filter collection. If filter collection is {@code null} or empty -
     *      an empty collection will be returned.
     * @param <T> Type of collections.
     * @return Collection of retain elements.
     */
    public static <T0, T extends T0> Collection<T> retain(Collection<T> c, boolean cp,
        @Nullable Collection<? extends T0> filter) {
        A.notNull(c, "c");

        return retain(c, cp, F0.in(filter));
    }

    /**
     * Retains all elements in input collection that are evaluated to {@code true}
     * by all given predicates.
     *
     * @param c Input collection.
     * @param cp If {@code true} method creates collection not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param p Predicates to filter by. If no predicates provides - all elements
     *      will be retained.
     * @param <T> Type of collections.
     * @return Collection of retain elements.
     */
    public static <T> Collection<T> retain(Collection<T> c, boolean cp, @Nullable IgnitePredicate<? super T>... p) {
        A.notNull(c, "c");

        return lose(c, cp, not(p));
    }

    /**
     * Retains only up to first {@code num} elements in the input collection.
     *
     * @param c Input collection.
     * @param cp If {@code true} method creates collection not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param num Maximum number of elements to retain (the actual number can be
     *      less if the input collection contains less elements).
     * @param <T> Type of the collections.
     * @return Collection contains up to {@code num} first elements from the input collection.
     */
    public static <T> Collection<T> retain(Collection<T> c, boolean cp, int num) {
        A.notNull(c, "c");
        A.ensure(num >= 0, "num >= 0");

        Collection<T> res;

        if (!cp) {
            res = c;

            if (num < res.size()) {
                int i = 0;

                for (Iterator<T> iter = res.iterator(); iter.hasNext();) {
                    iter.next();

                    if (i++ >= num)
                        iter.remove();
                }
            }
        }
        else {
            res = new ArrayList<>(num);

            Iterator<? extends T> iter = c.iterator();

            for (int i = 0; i < num && iter.hasNext(); i++)
                res.add(iter.next());
        }

        return res;
    }

    /**
     * For a given map retains all map entries that satisfy all provided predicates. If
     * no predicates provided - all entries will be retain.
     *
     * @param m Map to retain entries from.
     * @param cp If {@code true} method creates new map not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param p Optional set of predicate to use for filtration. If none provided - original
     *      map (or its copy) will be returned.
     * @param <K> Type of the free variable for the predicate and type of map's keys.
     * @param <V> Type of map's values.
     * @return Filtered map.
     */
    public static <K, V> Map<K, V> retain(Map<K, V> m, boolean cp,
        @Nullable IgnitePredicate<? super Map.Entry<K, V>>... p) {
        return lose(m, cp, F.not(p));
    }

    /**
     * For a given map retains all map entries which keys satisfy all provided predicates. If
     * no predicates provided - all entries will be retain.
     *
     * @param m Map to retain entries from.
     * @param cp If {@code true} method creates new map not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param p Optional set of predicate to use for filtration. If none provided - original
     *      map (or its copy) will be returned.
     * @param <K> Type of the free variable for the predicate and type of map's keys.
     * @param <V> Type of map's values.
     * @return Filtered map.
     */
    public static <K, V> Map<K, V> retainKeys(Map<K, V> m, boolean cp, @Nullable IgnitePredicate<? super K>... p) {
        return loseKeys(m, cp, F.not(p));
    }

    /**
     * For a given map retains all map entries which values satisfy all provided predicates.
     * If no predicates provided - all entries will be retain and the same map will be returned.
     *
     * @param m Map to retain entries from.
     * @param cp If {@code true} method creates new map not modifying input, otherwise does
     *      <tt>in-place</tt> modifications.
     * @param p Optional set of predicate to use for filtration. If none provided - original
     *      map (or its copy) will be returned.
     * @param <K> Type of the free variable for the predicate and type of map's keys.
     * @param <V> Type of map's values.
     * @return Filtered map.
     */
    public static <K, V> Map<K, V> retainValues(Map<K, V> m, boolean cp, @Nullable IgnitePredicate<? super V>... p) {
        return loseValues(m, cp, F.not(p));
    }

    /**
     * Converts given future into the closure. When result closure's {@code apply}
     * method is called it will call {@link Future#get()} method.
     *
     * @param fut Future to convert.
     * @param <T> Type of the future and closure.
     * @return Out closure that wraps given future.
     */
    public static <T> IgniteOutClosure as(final Future<T> fut) {
        A.notNull(fut, "fut");

        return new CO<T>() {
            @Override public T apply() {
                //noinspection CatchGenericClass
                try {
                    return fut.get();
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        };
    }

    /**
     * Converts predicate with two separate values to a predicate with tuple.
     *
     * @param p Predicate to convert.
     * @param <E1> Type of the 1st value.
     * @param <E2> Type of the 2nd value.
     * @return Converted predicate.
     */
    public static <E1, E2> IgnitePredicate<IgniteBiTuple<E1, E2>> as0(final IgniteBiPredicate<? super E1, ? super E2> p) {
        return new P1<IgniteBiTuple<E1, E2>>() {
            @Override public boolean apply(IgniteBiTuple<E1, E2> e) {
                return p.apply(e.get1(), e.get2());
            }
        };
    }

    /**
     * Curries given closure.
     *
     * @param f Closure.
     * @param e Parameter.
     * @param <T> Input type.
     * @param <R> Output type.
     * @return Curried closure.
     */
    public static <T, R> IgniteOutClosure<R> curry(final IgniteClosure<? super T, R> f, final T e) {
        return new IgniteOutClosure<R>() {
            @Override public R apply() {
                return f.apply(e);
            }
        };
    }

    /**
     * Curries given closure.
     *
     * @param f Closure.
     * @param e Parameter.
     * @param <T1> Input type1.
     * @param <T2> Input type2.
     * @param <R> Output type.
     * @return Curried closure.
     */
    public static <T1, T2, R> IgniteClosure<T2, R> curry(final IgniteBiClosure<? super T1, ? super T2, R> f, final T1 e) {
        return new IgniteClosure<T2, R>() {
            @Override public R apply(T2 t) {
                return f.apply(e, t);
            }
        };
    }

    /**
     * Curries given closure.
     *
     * @param f Closure.
     * @param e Parameter.
     * @param <T> Input type.
     * @return Curried closure.
     */
    public static <T> GridAbsPredicate curry(final IgnitePredicate<? super T> f, final T e) {
        return new GridAbsPredicate() {
            @Override public boolean apply() {
                return f.apply(e);
            }
        };
    }

    /**
     * Curries given closure.
     *
     * @param f Closure.
     * @param e Parameter.
     * @param <T> Input type.
     * @return Curried closure.
     */
    public static <T> GridAbsClosure curry(final IgniteInClosure<? super T> f, final T e) {
        return new GridAbsClosure() {
            @Override public void apply() {
                f.apply(e);
            }
        };
    }

    /**
     * Given collection of items and a closure this method returns
     * read only collection of closures where each closure is closed on an element
     * of the initial collection.
     *
     * @param c Input collection of elements.
     * @param f Closure to close on each element of input collection.
     * @param <T> Type of the input collection.
     * @param <R> Type of the return value for the closure.
     * @return Read only collection of closures closed on each element of input collection.
     */
    public static <T, R> Collection<IgniteOutClosure<R>> yield(Collection<? extends T> c,
        final IgniteClosure<? super T, R> f) {
        A.notNull(c, "c", f, "f");

        return viewReadOnly(c, new C1<T, IgniteOutClosure<R>>() {
            @Override public IgniteOutClosure<R> apply(final T e) {
                return curry(f, e);
            }
        });
    }

    /**
     * Given collection of items and a closure this method returns read-only collection of
     * closures where each closure is closed on an element of the initial collection.
     *
     * @param c Input collection of elements.
     * @param f Closure to close on each element of input collection.
     * @param <T> Type of the input collection.
     * @return Read only collection of closures closed on each element of input collection.
     */
    public static <T> Collection<GridAbsClosure> yield(Collection<? extends T> c, final IgniteInClosure<? super T> f) {
        A.notNull(c, "c", f, "f");

        return viewReadOnly(c, new C1<T, GridAbsClosure>() {
            @Override public GridAbsClosure apply(T e) {
                return curry(f, e);
            }
        });
    }

    /**
     * Given collection of items and a closure this method returns collection
     * of closures where each closure is closed on an element of the initial collection.
     *
     * @param c Input collection of elements.
     * @param f Closure to close on each element of input collection.
     * @param <T> Type of the input collection.
     * @return Collection of closures closed on each element of input collection.
     */
    public static <T> Collection<GridAbsClosure> yield(T[] c, IgniteInClosure<? super T> f) {
        A.notNull(c, "c", f, "f");

        return yield(asList(c), f);
    }

    /**
     * Given array of items and a closure this method returns collection
     * of closures where each closure is closed on a element of the array.
     *
     * @param c Input array of elements.
     * @param f Closure to close on each element of array.
     * @param <T> Type of the input collection.
     * @param <R> Type of the return value for the closure.
     * @return Collection of closures closed on each element of array.
     */
    public static <T, R> Collection<IgniteOutClosure<R>> yield(T[] c, IgniteClosure<? super T, R> f) {
        A.notNull(c, "c", f, "f");

        return yield(asList(c), f);
    }

    /**
     * Converts given iterator into instance of {@link Iterable} interface.
     *
     * @param iter Iterator to convert.
     * @param <T> Type of the iterator.
     * @return Iterable over given iterator.
     */
    public static <T> GridIterable<T> as(Iterator<T> iter) {
        A.notNull(iter, "iter");

        return new GridIterableAdapter<>(iter);
    }

    /**
     * Converts array to {@link List}. Note that resulting list cannot
     * be altered in size, as it it based on the passed in array -
     * only current elements can be changed.
     * <p>
     * Note that unlike {@link Arrays#asList(Object[])}, this method is
     * {@code null}-safe. If {@code null} is passed in, then empty list
     * will be returned.
     *
     * @param vals Array of values
     * @param <T> Array type.
     * @return {@link List} instance for array.
     */
    public static <T> List<T> asList(@Nullable T... vals) {
        return isEmpty(vals) ? Collections.<T>emptyList() : Arrays.asList(vals);
    }

    /**
     * Creates new empty iterator.
     *
     * @param <T> Type of the iterator.
     * @return Newly created empty iterator.
     */
    public static <T> GridIterator<T> emptyIterator() {
        return new GridEmptyIterator<>();
    }

    /**
     * Flattens collection-of-collections and returns collection over the
     * elements of the inner collections. This method doesn't create any
     * new collections or copies any elements.
     * <p>
     * Note that due to non-copying nature of implementation, the
     * {@link Collection#size() size()} method of resulting collection will have to
     * iterate over all elements to produce size. Method {@link Collection#isEmpty() isEmpty()},
     * however, is constant time and is much more preferable to use instead
     * of {@code 'size()'} method when checking if list is not empty.
     *
     * @param c Input collection of collections.
     * @param <T> Type of the inner collections.
     * @return Iterable over the elements of the inner collections.
     */
    public static <T> Collection<T> flatCollections(@Nullable final Collection<? extends Collection<T>> c) {
        if (F.isEmpty(c))
            return Collections.emptyList();

        return new GridSerializableCollection<T>() {
            @NotNull
            @Override public Iterator<T> iterator() {
                return flat((Iterable<? extends Iterable<T>>)c);
            }

            @Override public int size() {
                return F.size(iterator());
            }

            @Override public boolean isEmpty() {
                return !iterator().hasNext();
            }
        };
    }

    /**
     * Flattens iterable-of-iterables and returns iterable over the
     * elements of the inner collections. This method doesn't create any
     * new collections or copies any elements.
     *
     * @param c Input collection of collections.
     * @param <T> Type of the inner collections.
     * @return Iterable over the elements of the inner collections.
     */
    public static <T> GridIterator<T> flat(@Nullable final Iterable<? extends Iterable<T>> c) {
        return isEmpty(c) ? GridFunc.<T>emptyIterator() : new GridIteratorAdapter<T>() {
            /** */
            private Iterator<? extends Iterable<T>> a = c.iterator();

            /** */
            private Iterator<T> b;

            /** */
            private boolean moved = true;

            /** */
            private boolean more;

            @Override public boolean hasNextX() {
                if (!moved)
                    return more;

                moved = false;

                if (b != null && b.hasNext())
                    return more = true;

                while (a.hasNext()) {
                    b = a.next().iterator();

                    if (b.hasNext())
                        return more = true;
                }

                return more = false;
            }

            @Override public T nextX() {
                if (hasNext()) {
                    moved = true;

                    return b.next();
                }

                throw new NoSuchElementException();
            }

            @Override public void removeX() {
                assert b != null;

                b.remove();
            }
        };
    }

    /**
     * Flattens iterable-of-iterators and returns iterator over the
     * elements of the inner collections. This method doesn't create any
     * new collections or copies any elements.
     *
     * @param c Input iterable of iterators.
     * @return Iterator over the elements of given iterators.
     */
    public static <T> Iterator<T> flatIterators(@Nullable final Iterable<Iterator<T>> c) {
        return isEmpty(c) ? GridFunc.<T>emptyIterator() : new GridIteratorAdapter<T>() {
            /** */
            private Iterator<? extends Iterator<T>> a = c.iterator();

            /** */
            private Iterator<T> b;

            /** */
            private boolean moved = true;

            /** */
            private boolean more;

            @Override public boolean hasNextX() {
                if (!moved)
                    return more;

                moved = false;

                if (b != null && b.hasNext())
                    return more = true;

                while (a.hasNext()) {
                    b = a.next();

                    if (b.hasNext())
                        return more = true;
                }

                return more = false;
            }

            @Override public T nextX() {
                if (hasNext()) {
                    moved = true;

                    return b.next();
                }

                throw new NoSuchElementException();
            }

            @Override public void removeX() {
                assert b != null;

                b.remove();
            }
        };
    }

    /**
     * Flattens given set objects into a single collection. Unrolls {@link Collection},
     * {@link Iterable} and {@code Object[]} objects.
     *
     * @param objs Objects to flatten.
     * @return Flattened collection.
     */
    @SuppressWarnings("unchecked")
    public static Collection<Object> flat0(@Nullable Object... objs) {
        if (isEmpty(objs))
            return Collections.emptyList();

        assert objs != null;

        Collection<Object> c = new LinkedList<>();

        for (Object obj : objs)
            if (obj instanceof Collection)
                c.addAll((Collection<Object>)obj);
            else if (obj instanceof Iterable)
                for (Object o : (Iterable)obj)
                    c.add(o);
            else if (obj instanceof Object[])
                for (Object o : Arrays.asList((Object[])obj))
                    c.add(o);
            else
                c.add(obj);

        return c;
    }

    /**
     * @param col Collection.
     * @param limit Limit.
     * @return Collection limited up to {@code limit} elements.
     */
    public static <E> Collection<E> limit(final Collection<E> col, final int limit) {
        assert col != null;
        assert limit >= 0;

        if (limit == 0)
            return Collections.emptyList();

        return new AbstractCollection<E>() {
            @Override public Iterator<E> iterator() {
                return new Iterator<E>() {
                    private Iterator<E> iter = col.iterator();

                    private int cnt;

                    @Override public boolean hasNext() {
                        return cnt < limit && iter.hasNext();
                    }

                    @Override public E next() {
                        cnt++;

                        return iter.next();
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override public int size() {
                return col.size() > limit ? limit : col.size();
            }
        };
    }
    /**
     * @param col Collection.
     * @param limit Limit.
     * @return Collection limited up to {@code limit} elements.
     */
    public static <E> Set<E> limit(final Set<E> col, final int limit) {
        assert col != null;
        assert limit >= 0;

        if (limit == 0)
            return Collections.emptySet();

        return new AbstractSet<E>() {
            @Override public Iterator<E> iterator() {
                return new Iterator<E>() {
                    private Iterator<E> iter = col.iterator();

                    private int cnt;

                    @Override public boolean hasNext() {
                        return cnt < limit && iter.hasNext();
                    }

                    @Override public E next() {
                        cnt++;

                        return iter.next();
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override public int size() {
                return col.size() > limit ? limit : col.size();
            }
        };
    }

    /**
     * Converts given runnable to an absolute closure.
     *
     * @param r Runnable to convert to closure. If {@code null} - no-op closure is returned.
     * @return Closure that wraps given runnable. Note that wrapping closure always returns {@code null}.
     */
    public static GridAbsClosure as(@Nullable final Runnable r) {
        return new CA() {
            @Override public void apply() {
                if (r != null)
                    r.run();
            }
        };
    }

    /**
     * Returns closure that converts {@link Runnable} to {@link GridAbsClosure}.
     *
     * @return closure that converts {@link Runnable} to {@link GridAbsClosure}.
     */
    public static IgniteClosure<Runnable, GridAbsClosure> r2c() {
        return R2C;
    }

    /**
     * Returns closure that converts {@link Callable} to {@link org.apache.ignite.lang.IgniteOutClosure}.
     *
     * @return closure that converts {@link Callable} to {@link org.apache.ignite.lang.IgniteOutClosure}.
     */
    public static <T> IgniteClosure<Callable<T>, IgniteOutClosure<T>> c2c() {
        return new C1<Callable<T>, IgniteOutClosure<T>>() {
            @Override public IgniteOutClosure<T> apply(Callable<T> c) {
                return as0(c);
            }
        };
    }

    /**
     * Converts given callable to an out-closure.
     *
     * @param c Callable to convert to closure.
     * @return Out-closure that wraps given callable. Note that if callable throw
     *      exception the wrapping closure will re-throw it as {@link IgniteException}.
     */
    public static <R> IgniteOutClosure<R> as0(final Callable<R> c) {
        A.notNull(c, "c");

        return new CO<R>() {
            @Override public R apply() {
                try {
                    return c.call();
                }
                catch (Exception e) {
                    // No other way...
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Gets size of the given array with provided optional predicates.
     *
     * @param c Array to size.
     * @param p Optional predicates that filters out elements from count.
     * @param <T> Type of the array.
     * @return Number of elements in the array for which all given predicates
     *      evaluates to {@code true}. If no predicates is provided - all elements are counted.
     */
    public static <T> int size(T[] c, @Nullable IgnitePredicate<? super T>... p) {
        A.notNull(c, "c");

        return size(asList(c), p);
    }

    /**
     * Gets size of the given collection with provided optional predicates.
     *
     * @param c Collection to size.
     * @param p Optional predicates that filters out elements from count.
     * @param <T> Type of the iterator.
     * @return Number of elements in the collection for which all given predicates
     *      evaluates to {@code true}. If no predicates is provided - all elements are counted.
     */
    public static <T> int size(@Nullable Collection<? extends T> c, @Nullable IgnitePredicate<? super T>... p) {
        return c == null || c.isEmpty() ? 0 : isEmpty(p) || isAlwaysTrue(p) ? c.size() : size(c.iterator(), p);
    }

    /**
     * Gets size of the given iterator with provided optional predicates. Iterator
     * will be traversed to get the count.
     *
     * @param it Iterator to size.
     * @param p Optional predicates that filters out elements from count.
     * @param <T> Type of the iterator.
     * @return Number of elements in the iterator for which all given predicates
     *      evaluates to {@code true}. If no predicates is provided - all elements are counted.
     */
    public static <T> int size(@Nullable Iterator<? extends T> it, @Nullable IgnitePredicate<? super T>... p) {
        if (it == null)
            return 0;

        int n = 0;

        if (!isAlwaysFalse(p)) {
            while (it.hasNext()) {
                if (isAll(it.next(), p))
                    n++;
            }
        }

        return n;
    }

    /**
     * Creates write-through light-weight view on given collection with provided predicates. Resulting
     * collection will only "have" elements for which all provided predicates, if any, evaluate
     * to {@code true}. Note that only wrapping collection will be created and no duplication of
     * data will occur. Also note that if array of given predicates is not empty then method
     * {@code size()} uses full iteration through the collection.
     *
     * @param c Input collection that serves as a base for the view.
     * @param p Optional predicates. If predicates are not provided - all elements will be in the view.
     * @param <T> Type of the collection.
     * @return Light-weight view on given collection with provided predicate.
     */
    public static <T> Collection<T> view(@Nullable final Collection<T> c,
        @Nullable final IgnitePredicate<? super T>... p) {
        if (isEmpty(c) || isAlwaysFalse(p))
            return Collections.emptyList();

        assert c != null;

        return isEmpty(p) || isAlwaysTrue(p) ? c : new GridSerializableCollection<T>() {
            // Pass through (will fail for readonly).
            @Override public boolean add(T e) {
                return isAll(e, p) && c.add(e);
            }

            @NotNull
            @Override public Iterator<T> iterator() {
                return F.iterator0(c, false, p);
            }

            @Override public int size() {
                return F.size(c, p);
            }

            @Override public boolean isEmpty() {
                return F.isEmpty(p) ? c.isEmpty() : !iterator().hasNext();
            }
        };
    }

    /**
     * Creates read-only light-weight view on given collection with transformation and provided
     * predicates. Resulting collection will only "have" {@code transformed} elements for which
     * all provided predicate, if any, evaluates to {@code true}. Note that only wrapping
     * collection will be created and no duplication of data will occur. Also note that if array
     * of given predicates is not empty then method {@code size()} uses full iteration through
     * the collection.
     *
     * @param c Input collection that serves as a base for the view.
     * @param trans Transformation closure.
     * @param p Optional predicated. If predicates are not provided - all elements will be in the view.
     * @param <T1> Type of the collection.
     * @return Light-weight view on given collection with provided predicate.
     */
    @SuppressWarnings("RedundantTypeArguments")
    public static <T1, T2> Collection<T2> viewReadOnly(@Nullable final Collection<? extends T1> c,
        final IgniteClosure<? super T1, T2> trans, @Nullable final IgnitePredicate<? super T1>... p) {
        A.notNull(trans, "trans");

        if (isEmpty(c) || isAlwaysFalse(p))
            return Collections.emptyList();

        assert c != null;

        return new GridSerializableCollection<T2>() {
            @NotNull
            @Override public Iterator<T2> iterator() {
                return F.<T1, T2>iterator(c, trans, true, p);
            }

            @Override public int size() {
                return F.isEmpty(p) ? c.size() : F.size(iterator());
            }

            @Override public boolean isEmpty() {
                return F.isEmpty(p) ? c.isEmpty() : !iterator().hasNext();
            }
        };
    }

    /**
     * Creates read-only light-weight view on given list with provided transformation.
     * Resulting list will only "have" {@code transformed} elements. Note that only wrapping
     * list will be created and no duplication of data will occur.
     *
     * @param c Input list that serves as a base for the view.
     * @param trans Transformation closure.
     * @param <T1> Type of the list.
     * @return Light-weight view on given list with provided transformation.
     */
    @SuppressWarnings("RedundantTypeArguments")
    public static <T1, T2> List<T2> viewListReadOnly(@Nullable final List<? extends T1> c,
        final IgniteClosure<? super T1, T2> trans) {
        A.notNull(trans, "trans");

        if (isEmpty(c))
            return Collections.emptyList();

        assert c != null;

        return new GridSerializableList<T2>() {
            @Override public T2 get(int idx) {
                return trans.apply(c.get(idx));
            }

            @NotNull
            @Override public Iterator<T2> iterator() {
                return F.<T1, T2>iterator(c, trans, true);
            }

            @Override public int size() {
                return c.size();
            }

            @Override public boolean isEmpty() {
                return c.isEmpty();
            }
        };
    }

    /**
     * Creates a view on given list with provided transformer and predicates.
     * Resulting list will only "have" elements for which all provided predicates, if any,
     * evaluate to {@code true}. Note that a new collection will be created and data will
     * be copied.
     *
     * @param c Input list that serves as a base for the view.
     * @param trans Transforming closure from T1 to T2.
     * @param p Optional predicates. If predicates are not provided - all elements will be in the view.
     * @return View on given list with provided predicate.
     */
    public static <T1, T2> List<T2> transformList(Collection<? extends T1> c,
        IgniteClosure<? super T1, T2> trans, @Nullable IgnitePredicate<? super T1>... p) {
        A.notNull(c, "c", trans, "trans");

        if (isAlwaysFalse(p))
            return Collections.emptyList();

        return new ArrayList<>(transform(retain(c, true, p), trans));
    }

    /**
     * Creates a view on given set with provided transformer and predicates. Resulting set
     * will only "have" elements for which all provided predicates, if any, evaluate to {@code true}.
     * Note that a new collection will be created and data will be copied.
     *
     * @param c Input set that serves as a base for the view.
     * @param trans Transforming closure from T1 to T2.
     * @param p Optional predicates. If predicates are not provided - all elements will be in the view.
     * @return View on given set with provided predicate.
     */
    public static <T1, T2> Set<T2> transformSet(Collection<? extends T1> c,
        IgniteClosure<? super T1, T2> trans, @Nullable IgnitePredicate<? super T1>... p) {
        A.notNull(c, "c", trans, "trans");

        if (isAlwaysFalse(p))
            return Collections.emptySet();

        return new HashSet<>(transform(retain(c, true, p), trans));
    }

    /**
     * Creates light-weight view on given map with provided predicates. Resulting map will
     * only "have" keys for which all provided predicates, if any, evaluates to {@code true}.
     * Note that only wrapping map will be created and no duplication of data will occur.
     * Also note that if array of given predicates is not empty then method {@code size()}
     * uses full iteration through the entry set.
     *
     * @param m Input map that serves as a base for the view.
     * @param p Optional predicates. If predicates are not provided - all will be in the view.
     * @param <K> Type of the key.
     * @param <V> Type of the value.
     * @return Light-weight view on given map with provided predicate.
     */
    public static <K0, K extends K0, V0, V extends V0> Map<K, V> view(@Nullable final Map<K, V> m,
        @Nullable final IgnitePredicate<? super K>... p) {
        if (isEmpty(m) || isAlwaysFalse(p))
            return Collections.emptyMap();

        assert m != null;

        return isEmpty(p) || isAlwaysTrue(p) ? m : new GridSerializableMap<K, V>() {
            /** Entry predicate. */
            private IgnitePredicate<Entry<K, V>> ep = new P1<Map.Entry<K, V>>() {
                @Override public boolean apply(Entry<K, V> e) {
                    return isAll(e.getKey(), p);
                }
            };

            @NotNull
            @Override public Set<Entry<K, V>> entrySet() {
                return new GridSerializableSet<Map.Entry<K, V>>() {
                    @NotNull
                    @Override public Iterator<Entry<K, V>> iterator() {
                        return iterator0(m.entrySet(), false, ep);
                    }

                    @Override public int size() {
                        return F.size(m.keySet(), p);
                    }

                    @SuppressWarnings({"unchecked"})
                    @Override public boolean remove(Object o) {
                        return F.isAll((Map.Entry<K, V>)o, ep) && m.entrySet().remove(o);
                    }

                    @SuppressWarnings({"unchecked"})
                    @Override public boolean contains(Object o) {
                        return F.isAll((Map.Entry<K, V>)o, ep) && m.entrySet().contains(o);
                    }

                    @Override public boolean isEmpty() {
                        return !iterator().hasNext();
                    }
                };
            }

            @Override public boolean isEmpty() {
                return entrySet().isEmpty();
            }

            @SuppressWarnings({"unchecked"})
            @Nullable @Override public V get(Object key) {
                return isAll((K)key, p) ? m.get(key) : null;
            }

            @Nullable @Override public V put(K key, V val) {
                V oldVal = get(key);

                if (isAll(key, p))
                    m.put(key, val);

                return oldVal;
            }

            @SuppressWarnings({"unchecked"})
            @Override public boolean containsKey(Object key) {
                return isAll((K)key, p) && m.containsKey(key);
            }
        };
    }

    /**
     * Read-only view on map that supports transformation of values and key filtering. Resulting map will
     * only "have" keys for which all provided predicates, if any, evaluates to {@code true}.
     * Note that only wrapping map will be created and no duplication of data will occur.
     * Also note that if array of given predicates is not empty then method {@code size()}
     * uses full iteration through the entry set.
     *
     * @param m Input map that serves as a base for the view.
     * @param trans Transformer for map value transformation.
     * @param p Optional predicates. If predicates are not provided - all will be in the view.
     * @param <K> Type of the key.
     * @param <V> Type of the input map value.
     * @param <V1> Type of the output map value.
     * @return Light-weight view on given map with provided predicate and transformer.
     */
    public static <K0, K extends K0, V0, V extends V0, V1> Map<K, V1> viewReadOnly(@Nullable final Map<K, V> m,
        final IgniteClosure<V, V1> trans, @Nullable final IgnitePredicate<? super K>... p) {
        A.notNull(trans, "trans");

        if (isEmpty(m) || isAlwaysFalse(p))
            return Collections.emptyMap();

        assert m != null;

        return new GridSerializableMap<K, V1>() {
            /** Entry predicate. */
            private IgnitePredicate<Entry<K, V>> ep = new P1<Map.Entry<K, V>>() {
                @Override public boolean apply(Entry<K, V> e) {
                    return isAll(e.getKey(), p);
                }
            };

            @NotNull
            @Override public Set<Entry<K, V1>> entrySet() {
                return new GridSerializableSet<Map.Entry<K, V1>>() {
                    @NotNull
                    @Override public Iterator<Entry<K, V1>> iterator() {
                        return new Iterator<Entry<K, V1>>() {
                            private Iterator<Entry<K, V>> it = iterator0(m.entrySet(), true, ep);

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override public Entry<K, V1> next() {
                                final Entry<K, V> e = it.next();

                                return new Entry<K, V1>() {
                                    @Override public K getKey() {
                                        return e.getKey();
                                    }

                                    @Override public V1 getValue() {
                                        return trans.apply(e.getValue());
                                    }

                                    @Override public V1 setValue(V1 val) {
                                        throw new UnsupportedOperationException("Put is not supported for readonly map view.");
                                    }
                                };
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException("Remove is not support for readonly map view.");
                            }
                        };
                    }

                    @Override public int size() {
                        return F.size(m.keySet(), p);
                    }

                    @SuppressWarnings({"unchecked"})
                    @Override public boolean remove(Object o) {
                        throw new UnsupportedOperationException("Remove is not support for readonly map view.");
                    }

                    @SuppressWarnings({"unchecked"})
                    @Override public boolean contains(Object o) {
                        return F.isAll((Map.Entry<K, V>)o, ep) && m.entrySet().contains(o);
                    }

                    @Override public boolean isEmpty() {
                        return !iterator().hasNext();
                    }
                };
            }

            @Override public boolean isEmpty() {
                return entrySet().isEmpty();
            }

            @SuppressWarnings({"unchecked"})
            @Nullable @Override public V1 get(Object key) {
                if (isAll((K)key, p)) {
                    V v = m.get(key);

                    if (v != null)
                        return trans.apply(v);
                }

                return null;
            }

            @Nullable @Override public V1 put(K key, V1 val) {
                throw new UnsupportedOperationException("Put is not supported for readonly map view.");
            }

            @Override public V1 remove(Object key) {
                throw new UnsupportedOperationException("Remove is not supported for readonly map view.");
            }

            @SuppressWarnings({"unchecked"})
            @Override public boolean containsKey(Object key) {
                return isAll((K)key, p) && m.containsKey(key);
            }
        };
    }

    /**
     * Read-only view on map that supports transformation of values and key filtering. Resulting map will
     * only "have" keys for which all provided predicates, if any, evaluates to {@code true}.
     * Note that only wrapping map will be created and no duplication of data will occur.
     * Also note that if array of given predicates is not empty then method {@code size()}
     * uses full iteration through the entry set.
     *
     * @param m Input map that serves as a base for the view.
     * @param trans Transformer for map value transformation.
     * @param p Optional predicates. If predicates are not provided - all will be in the view.
     * @param <K> Type of the key.
     * @param <V> Type of the input map value.
     * @param <V1> Type of the output map value.
     * @return Light-weight view on given map with provided predicate and transformer.
     */
    public static <K0, K extends K0, V0, V extends V0, V1> Map<K, V1> viewReadOnly(@Nullable final Map<K, V> m,
        final IgniteBiClosure<K, V, V1> trans, @Nullable final IgnitePredicate<? super K>... p) {
        A.notNull(trans, "trans");

        if (isEmpty(m) || isAlwaysFalse(p))
            return Collections.emptyMap();

        assert m != null;

        return new GridSerializableMap<K, V1>() {
            /** Entry predicate. */
            private IgnitePredicate<Entry<K, V>> ep = new P1<Map.Entry<K, V>>() {
                @Override public boolean apply(Entry<K, V> e) {
                    return isAll(e.getKey(), p);
                }
            };

            @NotNull
            @Override public Set<Entry<K, V1>> entrySet() {
                return new GridSerializableSet<Map.Entry<K, V1>>() {
                    @NotNull
                    @Override public Iterator<Entry<K, V1>> iterator() {
                        return new Iterator<Entry<K, V1>>() {
                            private Iterator<Entry<K, V>> it = iterator0(m.entrySet(), true, ep);

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override public Entry<K, V1> next() {
                                final Entry<K, V> e = it.next();

                                return new Entry<K, V1>() {
                                    @Override public K getKey() {
                                        return e.getKey();
                                    }

                                    @Override public V1 getValue() {
                                        return trans.apply(e.getKey(), e.getValue());
                                    }

                                    @Override public V1 setValue(V1 val) {
                                        throw new UnsupportedOperationException(
                                            "Put is not supported for readonly map view.");
                                    }
                                };
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException("Remove is not support for readonly map view.");
                            }
                        };
                    }

                    @Override public int size() {
                        return F.size(m.keySet(), p);
                    }

                    @SuppressWarnings({"unchecked"})
                    @Override public boolean remove(Object o) {
                        throw new UnsupportedOperationException("Remove is not support for readonly map view.");
                    }

                    @SuppressWarnings({"unchecked"})
                    @Override public boolean contains(Object o) {
                        return F.isAll((Map.Entry<K, V>)o, ep) && m.entrySet().contains(o);
                    }

                    @Override public boolean isEmpty() {
                        return !iterator().hasNext();
                    }
                };
            }

            @Override public boolean isEmpty() {
                return entrySet().isEmpty();
            }

            @SuppressWarnings({"unchecked"})
            @Nullable @Override public V1 get(Object key) {
                if (isAll((K)key, p)) {
                    V v = m.get(key);

                    if (v != null)
                        return trans.apply((K)key, v);
                }

                return null;
            }

            @Nullable @Override public V1 put(K key, V1 val) {
                throw new UnsupportedOperationException("Put is not supported for readonly map view.");
            }

            @Override public V1 remove(Object key) {
                throw new UnsupportedOperationException("Remove is not supported for readonly map view.");
            }

            @SuppressWarnings({"unchecked"})
            @Override public boolean containsKey(Object key) {
                return isAll((K)key, p) && m.containsKey(key);
            }
        };
    }

    /**
     * Read-only map view of a collection. Resulting map is a lightweight view of an input collection,
     * with filtered elements of an input collection as keys, and closure execution results
     * as values. The map will only contain keys for which all provided predicates, if any, evaluate
     * to {@code true}. Note that only wrapping map will be created and no duplication of data will occur.
     * Also note that if array of given predicates is not empty then method {@code size()}
     * uses full iteration through the entry set.
     *
     * @param c Input collection.
     * @param mapClo Mapping closure, that maps key to value.
     * @param p Optional predicates to filter input collection. If predicates are not provided - all
     *          elements will be in the view.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Light-weight view on given map with provided predicates and mapping.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public static <K0, K extends K0, V0, V extends V0> Map<K, V> viewAsMap(@Nullable final Set<K> c,
        final IgniteClosure<? super K, V> mapClo, @Nullable final IgnitePredicate<? super K>... p) {
        A.notNull(mapClo, "trans");

        if (isEmpty(c) || isAlwaysFalse(p))
            return Collections.emptyMap();

        assert c != null;

        return new GridSerializableMap<K, V>() {
            /** Entry predicate. */
            private IgnitePredicate<K> ep = new P1<K>() {
                @Override public boolean apply(K e) {
                    return isAll(e, p);
                }
            };

            @NotNull @Override public Set<Entry<K, V>> entrySet() {
                return new GridSerializableSet<Entry<K, V>>() {
                    @NotNull @Override public Iterator<Entry<K, V>> iterator() {
                        return new Iterator<Entry<K, V>>() {
                            private Iterator<K> it = iterator0(c, true, ep);

                            @Override public boolean hasNext() {
                                return it.hasNext();
                            }

                            @Override public Entry<K, V> next() {
                                final K e = it.next();

                                return new Entry<K, V>() {
                                    @Override public K getKey() {
                                        return e;
                                    }

                                    @Override public V getValue() {
                                        return mapClo.apply(e);
                                    }

                                    @Override public V setValue(V val) {
                                        throw new UnsupportedOperationException(
                                            "Put is not supported for readonly collection view.");
                                    }
                                };
                            }

                            @Override public void remove() {
                                throw new UnsupportedOperationException(
                                    "Remove is not support for readonly collection view.");
                            }
                        };
                    }

                    @Override public int size() {
                        return F.size(c, p);
                    }

                    @Override public boolean remove(Object o) {
                        throw new UnsupportedOperationException("Remove is not support for readonly collection view.");
                    }

                    @Override public boolean isEmpty() {
                        return !iterator().hasNext();
                    }
                };
            }

            @Override public boolean isEmpty() {
                return entrySet().isEmpty();
            }

            @Nullable @Override public V get(Object key) {
                if (containsKey(key))
                    return mapClo.apply((K)key);

                return null;
            }

            @Nullable @Override public V put(K key, V val) {
                throw new UnsupportedOperationException("Put is not supported for readonly collection view.");
            }

            @Override public V remove(Object key) {
                throw new UnsupportedOperationException("Remove is not supported for readonly collection view.");
            }

            @Override public boolean containsKey(Object key) {
                return isAll((K)key, p) && c.contains(key);
            }
        };
    }

    /**
     * Tests if given string is {@code null} or empty.
     *
     * @param s String to test.
     * @return Whether or not the given string is {@code null} or empty.
     */
    public static boolean isEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Tests if the given array is either {@code null} or empty.
     *
     * @param c Array to test.
     * @return Whether or not the given array is {@code null} or empty.
     */
    public static <T> boolean isEmpty(@Nullable T[] c) {
        return c == null || c.length == 0;
    }

    /**
     * Tests if the given array is {@code null}, empty or contains only {@code null} values.
     *
     * @param c Array to test.
     * @return Whether or not the given array is {@code null}, empty or contains only {@code null} values.
     */
    public static <T> boolean isEmptyOrNulls(@Nullable T[] c) {
        if (isEmpty(c))
            return true;

        for (T element : c)
            if (element != null)
                return false;

        return true;
    }

    /**
     * Tests if the given array is either {@code null} or empty.
     *
     * @param c Array to test.
     * @return Whether or not the given array is {@code null} or empty.
     */
    public static boolean isEmpty(@Nullable int[] c) {
        return c == null || c.length == 0;
    }

    /**
     * Tests if the given array is either {@code null} or empty.
     *
     * @param c Array to test.
     * @return Whether or not the given array is {@code null} or empty.
     */
    public static boolean isEmpty(@Nullable byte[] c) {
        return c == null || c.length == 0;
    }

    /**
     * Tests if the given array is either {@code null} or empty.
     *
     * @param c Array to test.
     * @return Whether or not the given array is {@code null} or empty.
     */
    public static boolean isEmpty(@Nullable long[] c) {
        return c == null || c.length == 0;
    }

    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param c Collection to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean isEmpty(@Nullable Iterable<?> c) {
        return c == null || (c instanceof Collection<?> ? ((Collection<?>) c).isEmpty() : !c.iterator().hasNext());
    }

    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param c Collection to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean isEmpty(@Nullable Collection<?> c) {
        return c == null || c.isEmpty();
    }

    /**
     * Tests if the given map is either {@code null} or empty.
     *
     * @param m Map to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean isEmpty(@Nullable Map<?, ?> m) {
        return m == null || m.isEmpty();
    }

    /**
     * Utility map getter. This method analogous to {@link #addIfAbsent(Map, Object, Callable)}
     * method but this one doesn't put the default value into the map when key is not found.
     *
     * @param map Map to get value from.
     * @param key Map key (can be {@code null}).
     * @param c Optional factory closure for the default value to be returned in
     *      when {@code key} is not found. If closure is not provided - {@code null} will be returned.
     * @param <K> Map key type.
     * @param <V> Map value type.
     * @return Value for the {@code key} or default value produced by {@code c} if key is not
     *      found (or {@code null} if key is not found and closure is not provided).
     * @throws GridClosureException Thrown in case when callable throws exception.
     * @see #newLinkedList()
     * @see #newList()
     * @see #newSet()
     * @see #newMap()
     * @see #newAtomicLong()
     * @see #newAtomicInt()
     * @see #newAtomicRef()
     * @see #newAtomicBoolean()
     */
    @Nullable
    public static <K, V> V returnIfAbsent(Map<? extends K, ? extends V> map, @Nullable K key,
        @Nullable Callable<V> c) {
        A.notNull(map, "map");

        try {
            return !map.containsKey(key) ? c == null ? null : c.call() : map.get(key);
        }
        catch (Exception e) {
            throw wrap(e);
        }
    }

    /**
     * Returns a factory closure that creates new {@link ConcurrentLinkedDeque8} instance.
     * Note that this method does not create a new closure but returns a static one.
     *
     * @param <T> Type parameters for the created {@link List}.
     * @return Factory closure that creates new {@link List} instance every
     *      time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteCallable<ConcurrentLinkedDeque8<T>> newDeque() {
        return (IgniteCallable<ConcurrentLinkedDeque8<T>>)DEQUE_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link List} instance. Note that this
     * method does not create a new closure but returns a static one.
     *
     * @param <T> Type parameters for the created {@link List}.
     * @return Factory closure that creates new {@link List} instance every
     *      time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteCallable<List<T>> newList() {
        return (IgniteCallable<List<T>>)LIST_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link AtomicInteger} instance
     * initialized to {@code zero}. Note that this method does not create a new
     * closure but returns a static one.
     *
     * @return Factory closure that creates new {@link AtomicInteger} instance
     *      initialized to {@code zero} every time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    public static IgniteCallable<AtomicInteger> newAtomicInt() {
        return ATOMIC_INT_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link AtomicLong} instance
     * initialized to {@code zero}. Note that this method does not create a new
     * closure but returns a static one.
     *
     * @return Factory closure that creates new {@link AtomicLong} instance
     *      initialized to {@code zero} every time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    public static IgniteCallable<AtomicLong> newAtomicLong() {
        return ATOMIC_LONG_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link AtomicReference} instance
     * initialized to {@code null}. Note that this method does not create a new closure
     * but returns a static one.
     *
     * @param <T> Type of the atomic reference.
     * @return Factory closure that creates new {@link AtomicReference} instance
     *      initialized to {@code null} every time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteCallable<AtomicReference<T>> newAtomicRef() {
        return (IgniteCallable<AtomicReference<T>>)ATOMIC_REF_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link AtomicBoolean} instance
     * initialized to {@code false}. Note that this method does not create a new
     * closure but returns a static one.
     *
     * @return Factory closure that creates new {@link AtomicBoolean} instance
     *      initialized to {@code false} every time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    public static IgniteCallable<AtomicBoolean> newAtomicBoolean() {
        return ATOMIC_BOOL_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link LinkedList} instance.
     * Note that this method does not create a new closure but returns a static one.
     *
     * @param <T> Type parameters for the created {@link LinkedList}.
     * @return Factory closure that creates new {@link LinkedList} instance every time its {@link
     *         org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteCallable<LinkedList<T>> newLinkedList() {
        return (IgniteCallable<LinkedList<T>>)LINKED_LIST_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link Set} instance. Note that this
     * method does not create a new closure but returns a static one.
     *
     * @param <T> Type parameters for the created {@link Set}.
     * @return Factory closure that creates new {@link Set} instance every time
     *      its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteCallable<Set<T>> newSet() {
        return (IgniteCallable<Set<T>>)SET_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link Map} instance. Note
     * that this method does not create a new closure but returns a static one.
     *
     * @param <K> Type of the key for the created {@link Map}.
     * @param <V> Type of the value for the created {@link Map}.
     * @return Factory closure that creates new {@link Map} instance every
     *      time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> IgniteCallable<Map<K, V>> newMap() {
        return (IgniteCallable<Map<K, V>>)MAP_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link ConcurrentMap} instance.
     * Note that this method does not create a new closure but returns a static one.
     *
     * @param <K> Type of the key for the created {@link ConcurrentMap}.
     * @param <V> Type of the value for the created {@link ConcurrentMap}.
     * @return Factory closure that creates new {@link Map} instance every
     *      time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> IgniteCallable<ConcurrentMap<K, V>> newCMap() {
        return (IgniteCallable<ConcurrentMap<K, V>>)CONCURRENT_MAP_FACTORY;
    }

    /**
     * Returns a factory closure that creates new {@link GridConcurrentHashSet} instance.
     * Note that this method does not create a new closure but returns a static one.
     *
     * @return Factory closure that creates new {@link GridConcurrentHashSet} instance every
     *      time its {@link org.apache.ignite.lang.IgniteOutClosure#apply()} method is called.
     */
    @SuppressWarnings("unchecked")
    public static <E> IgniteCallable<Set<E>> newCSet() {
        return (IgniteCallable<Set<E>>)CONCURRENT_SET_FACTORY;
    }

    /**
     * Creates and returns iterable from given collection and optional filtering predicates.
     * Returned iterable collection will only have elements for which all given predicates
     * evaluates to {@code true}. Note that this method will not create new collection but
     * will simply "skip" elements in the provided collection that given predicates doesn't
     * evaluate to {@code true} for.
     *
     * @param c Input collection.
     * @param p Optional filtering predicates.
     * @param <T> Type of the collection elements.
     * @return Iterable from given collection and optional filtering predicates.
     */
    public static <T> GridIterable<T> iterable(Iterable<? extends T> c, IgnitePredicate<? super T>... p) {
        return new GridIterableAdapter<>(F.iterator0(c, false, p));
    }

    /**
     * Creates and returns iterator from given collection and optional filtering predicates.
     * Returned iterator will only have elements for which all given predicates evaluates to
     * {@code true} (if provided). Note that this method will not create new collection but
     * will simply "skip" elements in the provided collection that given predicates doesn't
     * evaluate to {@code true} for.
     *
     * @param c Input collection.
     * @param readOnly If {@code true}, then resulting iterator will not allow modifications
     *      to the underlying collection.
     * @param p Optional filtering predicates.
     * @param <T> Type of the collection elements.
     * @return Iterator from given collection and optional filtering predicate.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> GridIterator<T> iterator0(Iterable<? extends T> c, boolean readOnly,
        IgnitePredicate<? super T>... p) {
        return F.iterator(c, IDENTITY, readOnly, p);
    }

    /**
     * Creates and returns transforming iterator from given collection and optional
     * filtering predicates. Returned iterator will only have elements for which all
     * given predicates evaluates to {@code true} ( if provided). Note that this method
     * will not create new collection but will simply "skip" elements in the provided
     * collection that given predicates doesn't evaluate to {@code true} for.
     *
     * @param c Input collection.
     * @param trans Transforming closure to convert from T1 to T2.
     * @param readOnly If {@code true}, then resulting iterator will not allow modifications
     *      to the underlying collection.
     * @param p Optional filtering predicates.
     * @param <T1> Type of the collection elements.
     * @param <T2> Type of returned elements.
     * @return Iterator from given collection and optional filtering predicate.
     */
    public static <T1, T2> GridIterator<T2> iterator(final Iterable<? extends T1> c,
        final IgniteClosure<? super T1, T2> trans, final boolean readOnly,
        @Nullable final IgnitePredicate<? super T1>... p) {
        A.notNull(c, "c", trans, "trans");

        if (isAlwaysFalse(p))
            return F.emptyIterator();

        return new GridIteratorAdapter<T2>() {
            /** */
            private T1 elem;

            /** */
            private boolean moved = true;

            /** */
            private boolean more;

            /** */
            private Iterator<? extends T1> iter = c.iterator();

            @Override public boolean hasNextX() {
                if (isEmpty(p))
                    return iter.hasNext();
                else {
                    if (!moved)
                        return more;
                    else {
                        more = false;

                        while (iter.hasNext()) {
                            elem = iter.next();

                            boolean isAll = true;

                            for (IgnitePredicate<? super T1> r : p)
                                if (r != null && !r.apply(elem)) {
                                    isAll = false;

                                    break;
                                }

                            if (isAll) {
                                more = true;
                                moved = false;

                                return true;
                            }
                        }

                        elem = null; // Give to GC.

                        return false;
                    }
                }
            }

            @Nullable @Override public T2 nextX() {
                if (isEmpty(p))
                    return trans.apply(iter.next());
                else {
                    if (hasNext()) {
                        moved = true;

                        return trans.apply(elem);
                    }
                    else
                        throw new NoSuchElementException();
                }
            }

            @Override public void removeX() {
                if (readOnly)
                    throw new UnsupportedOperationException("Cannot modify read-only iterator.");

                iter.remove();
            }
        };
    }

    /**
     * @param c Input iterator.
     * @param trans Transforming closure to convert from T1 to T2.
     * @param readOnly If {@code true}, then resulting iterator will not allow modifications
     *      to the underlying collection.
     * @param p Optional filtering predicates.
     * @return Iterator from given iterator and optional filtering predicate.
     */
    public static <T1, T2> Iterator<T2> iterator(final Iterator<? extends T1> c,
        final IgniteClosure<? super T1, T2> trans,
        final boolean readOnly,
        @Nullable final IgnitePredicate<? super T1>... p)
    {
        A.notNull(c, "c", trans, "trans");

        if (isAlwaysFalse(p))
            return F.emptyIterator();

        return new GridIteratorAdapter<T2>() {
            /** */
            private T1 elem;

            /** */
            private boolean moved = true;

            /** */
            private boolean more;

            /** */
            private Iterator<? extends T1> iter = c;

            @Override public boolean hasNextX() {
                if (isEmpty(p))
                    return iter.hasNext();
                else {
                    if (!moved)
                        return more;
                    else {
                        more = false;

                        while (iter.hasNext()) {
                            elem = iter.next();

                            boolean isAll = true;

                            for (IgnitePredicate<? super T1> r : p)
                                if (r != null && !r.apply(elem)) {
                                    isAll = false;

                                    break;
                                }

                            if (isAll) {
                                more = true;
                                moved = false;

                                return true;
                            }
                        }

                        elem = null; // Give to GC.

                        return false;
                    }
                }
            }

            @Nullable @Override public T2 nextX() {
                if (isEmpty(p))
                    return trans.apply(iter.next());
                else {
                    if (hasNext()) {
                        moved = true;

                        return trans.apply(elem);
                    }
                    else
                        throw new NoSuchElementException();
                }
            }

            @Override public void removeX() {
                if (readOnly)
                    throw new UnsupportedOperationException("Cannot modify read-only iterator.");

                iter.remove();
            }
        };
    }

    /**
     * Gets predicate that always returns {@code true}. This method returns
     * constant predicate.
     *
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that always returns {@code true}.
     */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    public static <T> IgnitePredicate<T> alwaysTrue() {
        return (IgnitePredicate<T>)ALWAYS_TRUE;
    }

    /**
     * Gets predicate that always returns {@code false}. This method returns
     * constant predicate.
     *
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that always returns {@code false}.
     */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    public static <T> IgnitePredicate<T> alwaysFalse() {
        return (IgnitePredicate<T>)ALWAYS_FALSE;
    }

    /**
     * Tests whether or not given predicate is the one returned from
     * {@link #alwaysTrue()} method.
     *
     * @param p Predicate to check.
     * @return {@code true} if given predicate is {@code ALWAYS_TRUE} predicate.
     */
    public static boolean isAlwaysTrue(IgnitePredicate p) {
        return p == ALWAYS_TRUE;
    }

    /**
     * Tests whether or not given set of predicates consists only of one predicate returned from
     * {@link #alwaysTrue()} method.
     *
     * @param p Predicate to check.
     * @return {@code true} if given contains only {@code ALWAYS_TRUE} predicate.
     */
    public static boolean isAlwaysTrue(@Nullable IgnitePredicate[] p) {
        return p != null && p.length == 1 && isAlwaysTrue(p[0]);
    }

    /**
     * Tests whether or not given predicate is the one returned from
     * {@link #alwaysFalse()} method.
     *
     * @param p Predicate to check.
     * @return {@code true} if given predicate is {@code ALWAYS_FALSE} predicate.
     */
    public static boolean isAlwaysFalse(IgnitePredicate p) {
        return p == ALWAYS_FALSE;
    }

    /**
     * Tests whether or not given set of predicates consists only of one predicate returned from
     * {@link #alwaysFalse()} method.
     *
     * @param p Predicate to check.
     * @return {@code true} if given contains only {@code ALWAYS_FALSE} predicate.
     */
    public static boolean isAlwaysFalse(@Nullable IgnitePredicate[] p) {
        return p != null && p.length == 1 && isAlwaysFalse(p[0]);
    }

    /**
     * Gets predicate that evaluates to {@code true} if its free variable is {@code null}.
     *
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if its free variable is {@code null}.
     */
    public static <T> IgnitePredicate<T> isNull() {
        return (IgnitePredicate<T>) IS_NULL;
    }

    /**
     * Gets predicate that evaluates to {@code true} if its free variable is not {@code null}.
     *
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if its free variable is not {@code null}.
     */
    public static <T> IgnitePredicate<T> notNull() {
        return (IgnitePredicate<T>) IS_NOT_NULL;
    }

    /**
     * Gets predicate which checks if string is {@code null} or empty.
     *
     * @return Predicate which checks if string is {@code null} or empty.
     */
    public static IgnitePredicate<String> isEmptyString() {
        return EMPTY_STRING;
    }

    /**
     * Gets predicate which checks if string is not {@code null} or empty.
     *
     * @return Predicate which checks if string is not {@code null} or empty.
     */
    public static IgnitePredicate<String> isNotEmptyString() {
        return NOT_EMPTY_STRING;
    }

    /**
     * Gets predicate which checks if collection is {@code null} or empty.
     *
     * @param <T> Type of collection element.
     * @return Predicate which checks if collection is empty.
     */
    public static <T> IgnitePredicate<Collection<T>> isEmptyCollection() {
        return (IgnitePredicate<Collection<T>>)EMPTY_COLLECTION;
    }

    /**
     * Gets predicate which checks if collection is not {@code null} or empty.
     *
     * @param <T> Type of collection element.
     * @return Predicate which checks if collection is not {@code null} or empty.
     */
    public static <T> IgnitePredicate<Collection<T>> isNotEmptyCollection() {
        return (IgnitePredicate<Collection<T>>)NOT_EMPTY_COLLECTION;
    }

    /**
     * Negates given predicates.
     * <p>
     * Gets predicate that evaluates to {@code true} if any of given predicates
     * evaluates to {@code false}. If all predicates evaluate to {@code true} the
     * result predicate will evaluate to {@code false}.
     *
     * @param p Predicate to negate.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Negated predicate.
     */
    public static <T> IgnitePredicate<T> not(@Nullable final IgnitePredicate<? super T>... p) {
        return isAlwaysFalse(p) ? F.<T>alwaysTrue() : isAlwaysTrue(p) ? F.<T>alwaysFalse() : new P1<T>() {
            @Override public boolean apply(T t) {
                return !isAll(t, p);
            }
        };
    }

    /**
     * Gets predicate that evaluates to {@code true} if its free variable is equal
     * to {@code target} or both are {@code null}.
     *
     * @param target Object to compare free variable to.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if its free variable is equal to
     *      {@code target} or both are {@code null}.
     */
    public static <T> IgnitePredicate<T> equalTo(@Nullable final T target) {
        return new P1<T>() {
            @Override public boolean apply(T t) {
                return eq(t, target);
            }
        };
    }

    /**
     * Gets predicate that evaluates to {@code true} if its free variable is not equal
     * to {@code target} or both are {@code null}.
     *
     * @param target Object to compare free variable to.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if its free variable is not equal
     *      to {@code target} or both are {@code null}.
     */
    public static <T> IgnitePredicate<T> notEqualTo(@Nullable final T target) {
        return new P1<T>() {
            @Override public boolean apply(T t) {
                return !eq(t, target);
            }
        };
    }

    /**
     * Gets predicate that evaluates to {@code true} if its free variable is instance of the given class.
     *
     * @param cls Class to compare to.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if its free variable is instance
     *      of the given class.
     */
    public static <T> IgnitePredicate<T> instanceOf(final Class<?> cls) {
        A.notNull(cls, "cls");

        return new P1<T>() {
            @Override public boolean apply(T t) {
                return t != null && cls.isAssignableFrom(t.getClass());
            }
        };
    }

    /**
     * Gets predicate that evaluates to {@code true} if its free variable is not an instance
     * of the given class.
     *
     * @param cls Class to compare to.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if its free variable is not an instance
     *      of the given class.
     */
    public static <T> IgnitePredicate<T> notInstanceOf(final Class<?> cls) {
        A.notNull(cls, "cls");

        return new P1<T>() {
            @Override public boolean apply(T t) {
                return t == null || !cls.isAssignableFrom(t.getClass());
            }
        };
    }

    /**
     * Gets first element from given collection or returns {@code null} if the collection is empty.
     *
     * @param c A collection.
     * @param <T> Type of the collection.
     * @return Collections' first element or {@code null} in case if the collection is empty.
     */
    public static <T> T first(@Nullable Iterable<? extends T> c) {
        if (c == null)
            return null;

        Iterator<? extends T> it = c.iterator();

        return it.hasNext() ? it.next() : null;
    }

    /**
     * Gets first element from given list or returns {@code null} if list is empty.
     *
     * @param list List.
     * @return List' first element or {@code null} in case if list is empty.
     */
    public static <T> T first(List<? extends T> list) {
        if (list.isEmpty())
            return null;

        return list.get(0);
    }

    /**
     * Gets last element from given collection or returns {@code null} if the collection is empty.
     *
     * @param c A collection.
     * @param <T> Type of the collection.
     * @return Collections' first element or {@code null} in case if the collection is empty.
     */
    @Nullable public static <T> T last(@Nullable Iterable<? extends T> c) {
        if (c == null)
            return null;

        assert c != null;

        if (c instanceof RandomAccess && c instanceof List) {
            List<T> l = (List<T>)c;

            return l.get(l.size() - 1);
        }
        else if (c instanceof NavigableSet) {
            NavigableSet<T> s = (NavigableSet<T>)c;

            return s.last();
        }

        T last = null;

        for (T t : c)
            last = t;

        return last;
    }

    /**
     * Gets first value from given map or returns {@code null} if the map is empty.
     *
     * @param m A map.
     * @param <V> Value type.
     * @return Maps' first value or {@code null} in case if the map is empty.
     */
    @Nullable public static <V> V firstValue(Map<?, V> m) {
        Iterator<V> it = m.values().iterator();

        return it.hasNext() ? it.next() : null;
    }

    /**
     * Gets first key from given map or returns {@code null} if the map is empty.
     *
     * @param m A map.
     * @param <K> Key type.
     * @return Maps' first key or {@code null} in case if the map is empty.
     */
    @Nullable public static <K> K firstKey(Map<K, ?> m) {
        Iterator<K> it = m.keySet().iterator();

        return it.hasNext() ? it.next() : null;
    }

    /**
     * Gets first key from given map or returns {@code null} if the map is empty.
     *
     * @param m A map.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Map's first entry or {@code null} in case if the map is empty.
     */
    @Nullable public static <K, V> Map.Entry<K, V> firstEntry(Map<K, V> m) {
        Iterator<Map.Entry<K, V>> it = m.entrySet().iterator();

        return it.hasNext() ? it.next() : null;
    }

    /**
     * Get a predicate that evaluates to {@code true} if each of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code false}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param ps Passed in predicate.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if each of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgnitePredicate<T> and(@Nullable final Collection<? extends IgnitePredicate<? super T>> ps) {
        if (isEmpty(ps))
            return F.alwaysTrue();

        assert ps != null;

        if (F0.isAllNodePredicates(ps)) {
            Set<UUID> ids = new GridLeanSet<>();

            for (IgnitePredicate<? super T> p : ps) {
                Collection<UUID> list = ((GridNodePredicate)p).nodeIds();

                if (ids.isEmpty())
                    ids.addAll(list);
                else
                    ids.retainAll(list);
            }

            // T must be <T extends GridNode>.
            return (IgnitePredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    for (IgnitePredicate<? super T> p : ps) {
                        if (!p.apply(t))
                            return false;
                    }

                    return true;
                }
            };
        }
    }

    /**
     * Get a predicate that evaluates to {@code true} if each of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code false}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param p1 Passed in predicates.
     * @param p2 Passed in predicates.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if each of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> IgnitePredicate<T> and(@Nullable final IgnitePredicate<? super T>[] p1,
        @Nullable final IgnitePredicate<? super T>... p2) {
        if (isAlwaysFalse(p1) || isAlwaysFalse(p2))
            return F.alwaysFalse();

        if (isAlwaysTrue(p1) && isAlwaysTrue(p2))
            return F.alwaysTrue();

        final boolean e1 = isEmpty(p1);
        final boolean e2 = isEmpty(p2);

        if (e1 && e2)
            return F.alwaysTrue();

        if (e1 && !e2) {
            assert p2 != null;

            if (p2.length == 1)
                return (IgnitePredicate<T>)p2[0];
        }

        if (!e1 && e2) {
            assert p1 != null;

            if (p1.length == 1)
                return (IgnitePredicate<T>)p1[0];
        }

        if ((e1 || F0.isAllNodePredicates(p1)) && (e2 || F0.isAllNodePredicates(p2))) {
            Set<UUID> ids = new GridLeanSet<>();

            if (!e1) {
                assert p1 != null;

                for (IgnitePredicate<? super T> p : p1) {
                    ids.addAll(((GridNodePredicate)p).nodeIds());
                }
            }

            if (!e2) {
                assert p2 != null;

                for (IgnitePredicate<? super T> p : p2) {
                    ids.addAll(((GridNodePredicate)p).nodeIds());
                }
            }

            // T must be <T extends GridNode>.
            return (IgnitePredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    if (!e1) {
                        assert p1 != null;

                        for (IgnitePredicate<? super T> p : p1) {
                            if (p != null && !p.apply(t))
                                return false;
                        }
                    }

                    if (!e2) {
                        assert p2 != null;

                        for (IgnitePredicate<? super T> p : p2) {
                            if (p != null && !p.apply(t))
                                return false;
                        }
                    }

                    return true;
                }
            };
        }
    }

    /**
     * Get a predicate that evaluates to {@code true} if each of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code false}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param ps Passed in predicate. If none provided - always-{@code false} predicate is
     *      returned.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if each of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgnitePredicate<T> and(@Nullable final IgnitePredicate<? super T>... ps) {
        if (isEmpty(ps))
            return F.alwaysTrue();

        if (isAlwaysFalse(ps))
            return F.alwaysFalse();

        if (isAlwaysTrue(ps))
            return F.alwaysTrue();

        if (F0.isAllNodePredicates(ps)) {
            assert ps != null;

            Set<UUID> ids = new HashSet<>();

            for (IgnitePredicate<? super T> p : ps) {
                if (p != null) {
                    Collection<UUID> list = ((GridNodePredicate)p).nodeIds();

                    if (ids.isEmpty())
                        ids.addAll(list);
                    else
                        ids.retainAll(list);
                }
            }

            // T must be <T extends GridNode>.
            return (IgnitePredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    assert ps != null;

                    for (IgnitePredicate<? super T> p : ps)
                        if (p != null && !p.apply(t))
                            return false;

                    return true;
                }
            };
        }
    }

    /**
     * Get a predicate that evaluates to {@code true} if any of its component
     * predicates evaluates to {@code true}. The components are evaluated in order
     * they are supplied. Evaluation will be stopped as soon as first predicate
     * evaluates to {@code true}. Passed in predicates are NOT copied. If no predicates
     * are passed in the returned predicate will always evaluate to {@code false}.
     *
     * @param ps Passed in predicate.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if any of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgnitePredicate<T> or(@Nullable final Collection<? extends IgnitePredicate<? super T>> ps) {
        if (isEmpty(ps))
            return F.alwaysFalse();
        else {
            assert ps != null;

            if (F0.isAllNodePredicates(ps)) {
                Set<UUID> ids = new GridLeanSet<>();

                for (IgnitePredicate<? super T> p : ps) {
                    if (p != null)
                        ids.addAll(((GridNodePredicate)p).nodeIds());
                }

                // T must be <T extends GridNode>.
                return (IgnitePredicate<T>)new GridNodePredicate(ids);
            }
            else {
                return new P1<T>() {
                    @Override public boolean apply(T t) {
                        for (IgnitePredicate<? super T> p : ps) {
                            if (p != null && p.apply(t))
                                return true;
                        }

                        return false;
                    }
                };
            }
        }
    }

    /**
     * Get a predicate that evaluates to {@code true} if any of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code false}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param p1 Passed in predicates.
     * @param p2 Passed in predicates.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if any of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgnitePredicate<T> or(@Nullable final IgnitePredicate<? super T>[] p1,
        @Nullable final IgnitePredicate<? super T>... p2) {
        if (isEmpty(p1) && isEmpty(p2))
            return F.alwaysFalse();

        if (isAlwaysTrue(p1) || isAlwaysTrue(p2))
            return F.alwaysTrue();

        if (isAlwaysFalse(p1) && isAlwaysFalse(p2))
            return F.alwaysFalse();

        final boolean e1 = isEmpty(p1);
        final boolean e2 = isEmpty(p2);

        if (e1 && e2)
            return F.alwaysFalse();

        if (e1 && !e2) {
            assert p2 != null;

            if (p2.length == 1)
                return (IgnitePredicate<T>)p2[0];
        }

        if (!e1 && e2) {
            assert p1 != null;

            if (p1.length == 1)
                return (IgnitePredicate<T>)p1[0];
        }

        if ((e1 || F0.isAllNodePredicates(p1)) && (e2 || F0.isAllNodePredicates(p2))) {
            Set<UUID> ids = new GridLeanSet<>();

            if (!e1) {
                assert p1 != null;

                for (IgnitePredicate<? super T> p : p1) {
                    ids.addAll(((GridNodePredicate)p).nodeIds());
                }
            }

            if (!e2) {
                assert p2 != null;

                for (IgnitePredicate<? super T> p : p2) {
                    ids.addAll(((GridNodePredicate)p).nodeIds());
                }
            }

            // T must be <T extends GridNode>.
            return (IgnitePredicate<T>)new GridNodePredicate(ids);
        }
        else {
            return new P1<T>() {
                @Override public boolean apply(T t) {
                    if (!e1) {
                        assert p1 != null;

                        for (IgnitePredicate<? super T> p : p1) {
                            if (p != null && p.apply(t))
                                return true;
                        }
                    }

                    if (!e2) {
                        assert p2 != null;

                        for (IgnitePredicate<? super T> p : p2) {
                            if (p != null && !p.apply(t))
                                return true;
                        }
                    }

                    return false;
                }
            };
        }
    }

    /**
     * Get a predicate that evaluates to {@code true} if any of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code true}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param ps Passed in predicate.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if any of its component predicates evaluates
     * to {@code true}.
     */
    @SuppressWarnings("unchecked")
    public static <T> IgnitePredicate<T> or(@Nullable final IgnitePredicate<? super T>... ps) {
        if (isEmpty(ps) || isAlwaysFalse(ps))
            return F.alwaysFalse();
        else {
            if (isAlwaysTrue(ps))
                return F.alwaysTrue();
            else {
                if (F0.isAllNodePredicates(ps)) {
                    Set<UUID> ids = new GridLeanSet<>();

                    assert ps != null;

                    for (IgnitePredicate<? super T> p : ps) {
                        ids.addAll(((GridNodePredicate)p).nodeIds());
                    }

                    // T must be <T extends GridNode>.
                    return (IgnitePredicate<T>)new GridNodePredicate(ids);
                }
                else {
                    return new P1<T>() {
                        @Override public boolean apply(T t) {
                            assert ps != null;

                            for (IgnitePredicate<? super T> p : ps) {
                                if (p != null && p.apply(t))
                                    return true;
                            }

                            return false;
                        }
                    };
                }
            }
        }
    }

    /**
     * Gets a predicate that is composed of given predicate and closure. For every {@code x}
     * it returns predicate {@code p(f(x))}. Note that predicate and closure must be loaded
     * by the same class loader.
     *
     * @param p Predicate.
     * @param f Closure.
     * @param <X> Type of the free variable for the closure.
     * @param <Y> Type of the closure's return value.
     * @return Predicate that is composed of given predicate and closure.
     */
    @SuppressWarnings({"JavaDoc"})
    public static <X, Y> IgnitePredicate<X> compose(final IgnitePredicate<? super Y> p,
        final IgniteClosure<? super X, ? extends Y> f) {
        A.notNull(p, "p", f, "f");

        return isAlwaysFalse(p) ? F.<X>alwaysFalse() : isAlwaysTrue(p) ? F.<X>alwaysTrue() : new P1<X>() {
            @Override public boolean apply(X x) {
                return p.apply(f.apply(x));
            }
        };
    }

    /**
     * Gets closure that returns constant value.
     *
     * @param val Constant value to return.
     * @param <T1> Type of the free variable for the closure.
     * @param <R> Type of the closure's return value.
     * @return Closure that returns constant value.
     */
    public static <T1, R> IgniteClosure<T1, R> constant1(@Nullable final R val) {
        return new C1<T1, R>() {
            @Nullable @Override public R apply(T1 t1) {
                return val;
            }
        };
    }

    /**
     * Gets closure that returns constant value.
     *
     * @param val Constant value to return.
     * @param <T1> Type of the free variable for the closure.
     * @param <T2> Type of the free variable for the closure.
     * @param <R> Type of the closure's return value.
     * @return Closure that returns constant value.
     */
    public static <T1, T2, R> IgniteBiClosure<T1, T2, R> constant2(@Nullable final R val) {
        return new C2<T1, T2, R>() {
            @Nullable @Override public R apply(T1 t1, T2 t2) {
                return val;
            }
        };
    }

    /**
     * Gets closure that returns constant value.
     *
     * @param val Constant value to return.
     * @param <T1> Type of the free variable for the closure.
     * @param <T2> Type of the free variable for the closure.
     * @param <T3> Type of the free variable for the closure.
     * @param <R> Type of the closure's return value.
     * @return Closure that returns constant value.
     */
    public static <T1, T2, T3, R> GridClosure3<T1, T2, T3, R> constant3(@Nullable final R val) {
        return new C3<T1, T2, T3, R>() {
            @Nullable @Override public R apply(T1 t1, T2 t2, T3 t3) {
                return val;
            }
        };
    }

    /**
     * Gets closure that returns constant value.
     *
     * @param val Constant value to return.
     * @param <R> Type of the closure's return value.
     * @return Closure that returns constant value.
     */
    public static <R> IgniteOutClosure<R> constant(@Nullable final R val) {
        return new CO<R>() {
            @Nullable @Override public R apply() {
                return val;
            }
        };
    }

    /**
     * Creates and returns new factory closure for the given type.
     *
     * @param cls Class of factory.
     * @param <T> Type of factory.
     * @return Factory closure for the given type.
     */
    public static <T> IgniteOutClosure<T> factory(final Class<T> cls) {
        A.notNull(cls, "cls");

        return new CO<T>() {
            @Override public T apply() {
                try {
                    return cls.newInstance();
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        };
    }

    /**
     * Gets identity closure, i.e. the closure that returns its variable value.
     *
     * @param <T> Type of the variable and return value for the closure.
     * @return Identity closure, i.e. the closure that returns its variable value.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> IgniteClosure<T, T> identity() {
        return IDENTITY;
    }

    /**
     * Gets identity predicate, i.e. the predicate that returns its argument value.
     *
     * @return Identity predicate.
     */
    public static IgnitePredicate<Boolean> identityPredicate() {
        return IDENTITY_PRED;
    }

    /**
     * Converts given closure to predicate.
     *
     * @param c Closure to convert.
     * @return Closure converted to predicate.
     */
    public static GridAbsPredicate as(final Callable<Boolean> c) {
        A.notNull(c, "c");

        return new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return c.call();
                }
                catch (Exception e) {
                    throw wrap(e);
                }
            }
        };
    }

    /**
     * Converts given closure to predicate.
     *
     * @param c Closure to convert.
     * @return Closure converted to predicate.
     */
    public static <T> IgnitePredicate<T> as(final IgniteClosure<? super T, Boolean> c) {
        A.notNull(c, "c");

        return new P1<T>() {
            @Override public boolean apply(T t) {
                return c.apply(t);
            }
        };
    }

    /**
     * Converts given closure to predicate.
     *
     * @param c Closure to convert.
     * @return Closure converted to predicate.
     */
    public static <T1, T2> IgniteBiPredicate<T1, T2> as(final IgniteBiClosure<? super T1, ? super T2, Boolean> c) {
        A.notNull(c, "c");

        return new P2<T1, T2>() {
            @Override public boolean apply(T1 t1, T2 t2) {
                return c.apply(t1, t2);
            }
        };
    }

    /**
     * Converts given closure to predicate.
     *
     * @param c Closure to convert.
     * @return Closure converted to predicate.
     */
    public static <T1, T2, T3> GridPredicate3<T1, T2, T3> as(final GridClosure3<? super T1, ? super T2, ? super T3,
        Boolean> c) {
        A.notNull(c, "c");

        return new P3<T1, T2, T3>() {
            @Override public boolean apply(T1 t1, T2 t2, T3 t3) {
                return c.apply(t1, t2, t3);
            }
        };
    }

    /**
     * Converts given predicate to closure.
     *
     * @param p Predicate to convert.
     * @return Predicate converted to closure.
     */
    public static IgniteOutClosure<Boolean> as(final GridAbsPredicate p) {
        A.notNull(p, "p");

        return new CO<Boolean>() {
            @Override public Boolean apply() {
                return p.apply();
            }
        };
    }

    /**
     * Converts given predicate to closure.
     *
     * @param p Predicate to convert.
     * @param <X> Type of the free variable for the predicate.
     * @return Predicate converted to closure.
     */
    public static <X> IgniteClosure<X, Boolean> as(final IgnitePredicate<? super X> p) {
        A.notNull(p, "p");

        return new C1<X, Boolean>() {
            @Override public Boolean apply(X x) {
                return p.apply(x);
            }
        };
    }

    /**
     * Converts given predicate to closure.
     *
     * @param p Predicate to convert.
     * @param <X1> Type of the free variable for the predicate.
     * @param <X2> Type of the free variable for the predicate.
     * @return Predicate converted to closure.
     */
    public static <X1, X2> IgniteBiClosure<X1, X2, Boolean> as(final IgniteBiPredicate<? super X1, ? super X2> p) {
        A.notNull(p, "p");

        return new C2<X1, X2, Boolean>() {
            @Override public Boolean apply(X1 x1, X2 x2) {
                return p.apply(x1, x2);
            }
        };
    }

    /**
     * Converts given predicate to closure.
     *
     * @param p Predicate to convert.
     * @param <X1> Type of the free variable for the predicate.
     * @param <X2> Type of the free variable for the predicate.
     * @param <X3> Type of the free variable for the predicate.
     * @return Predicate converted to closure.
     */
    public static <X1, X2, X3> GridClosure3<X1, X2, X3, Boolean> as(
        final GridPredicate3<? super X1, ? super X2, ? super X3> p) {
        A.notNull(p, "p");

        return new C3<X1, X2, X3, Boolean>() {
            @Override public Boolean apply(X1 x1, X2 x2, X3 x3) {
                return p.apply(x1, x2, x3);
            }
        };
    }

    /**
     * Composes two closures into one. If {@code a} is a free variable the
     * result closure is {@code g.apply(f.apply(a))}. Note that both closures
     * must be loaded by the same class loader.
     *
     * @param f First closure.
     * @param g Second closure.
     * @param <D> Type of the free variable for the closure.
     * @param <B> Type of return value and of the free variable for the closure.
     * @param <C> Type of the return value for the closure.
     * @return Composition closure.
     */
    @SuppressWarnings({"JavaDoc"})
    public static <D, B, C> IgniteClosure<D, C> compose(final IgniteClosure<? super D, ? extends B> f,
        final IgniteClosure<? super B, C> g) {
        A.notNull(f, "f", g, "g");

        return new C1<D, C>() {
            @Override public C apply(D a) {
                return g.apply(f.apply(a));
            }
        };
    }

    /**
     * Gets closure that wraps given map access, i.e. returns map's value assuming that
     * free variable is a map's key.
     *
     * @param m Source map.
     * @param <K> Type of the free variable for the closure and type of the map keys.
     * @param <V> Type of the closure's return value and type of the map values.
     * @return Closure that wraps given map access.
     */
    public static <K, V> IgniteClosure<K, V> forMap(final Map<? extends K, ? extends V> m) {
        A.notNull(m, "m");

        return new C1<K, V>() {
            @Override public V apply(K k) {
                return m.get(k);
            }
        };
    }

    /**
     * Gets closure that wraps given map's access, i.e. returns map's value assuming that
     * free variable is a map's key or default value produced by optional factory closure
     * if key has no mapping.
     *
     * @param m Source map.
     * @param c Optional factory closure to produce default value to return from result closure if
     *      key has no mapping in given map. If factory closure is {@code null} - the default value
     *      will be {@code null}.
     * @param <K> Type of the free variable for the closure and type of the map keys.
     * @param <V> Type of the closure's return value and type of the map values.
     * @return Closure that wraps given map's access.
     */
    public static <K, V> IgniteClosure<K, V> forMap(final Map<? extends K, ? extends V> m,
        @Nullable final Callable<V> c) {
        A.notNull(m, "m");

        return new C1<K, V>() {
            @Nullable
            @Override public V apply(K k) {
                return returnIfAbsent(m, k, c);
            }
        };
    }

    /**
     * Gets closure that return {@code toString()} value for its free variable.
     *
     * @param <T> Type of the free variable for the closure.
     * @return Closure that return {@code toString()} value for its free variable.
     */
    public static <T> IgniteClosure<T, String> string() {
        return new C1<T, String>() {
            @Override public String apply(@Nullable T t) {
                return String.valueOf(t); // This is null-safe.
            }
        };
    }

    /**
     * Gets predicate that returns {@code true} if its free variable is contained
     * in given collection.
     *
     * @param c Collection to check for containment.
     * @param <T> Type of the free variable for the predicate and type of the
     *      collection elements.
     * @return Predicate that returns {@code true} if its free variable is
     *      contained in given collection.
     */
    public static <T> IgnitePredicate<T> in(@Nullable final Collection<? extends T> c) {
        return isEmpty(c) ? GridFunc.<T>alwaysFalse() : new P1<T>() {
            @Override public boolean apply(T t) {
                assert c != null;

                return c.contains(t);
            }
        };
    }

    /**
     * Gets predicate that returns {@code true} if its free variable is not
     * contained in given collection.
     *
     * @param c Collection to check for containment.
     * @param <T> Type of the free variable for the predicate and type of the
     *      collection elements.
     * @return Predicate that returns {@code true} if its free variable is not
     *      contained in given collection.
     */
    public static <T> IgnitePredicate<T> notIn(@Nullable final Collection<? extends T> c) {
        return isEmpty(c) ? GridFunc.<T>alwaysTrue() : new P1<T>() {
            @Override public boolean apply(T t) {
                assert c != null;

                return !c.contains(t);
            }
        };
    }

    /**
     * Gets the value with given key. If that value does not exist, calls given
     * closure to get the default value, puts it into the map and returns it. If
     * closure is {@code null} return {@code null}.
     *
     * @param map Concurrent hash map.
     * @param key Key to get the value for.
     * @param c Default value producing closure.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Value for the key or the value produced by the closure if key
     *      does not exist in the map. Return {@code null} if key is not found and
     *      closure is {@code null}.
     */
    public static <K, V>  V addIfAbsent(ConcurrentMap<K, V> map, K key, @Nullable Callable<V> c) {
        A.notNull(map, "map", key, "key");

        V v = map.get(key);

        if (v == null && c != null) {
            try {
                v = c.call();
            }
            catch (Exception e) {
                throw F.wrap(e);
            }

            V v0 = map.putIfAbsent(key, v);

            if (v0 != null)
                v = v0;
        }

        return v;
    }

    /**
     * Gets the value with given key. If that value does not exist, puts given
     * value into the map and returns it.
     *
     * @param map Map.
     * @param key Key.
     * @param val Value to put if one does not exist.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Current mapping for a given key.
     */
    public static <K, V> V addIfAbsent(ConcurrentMap<K, V> map, K key, V val) {
        A.notNull(map, "map", key, "key", val, "val");

        V v = map.putIfAbsent(key, val);

        if (v != null)
            val = v;

        return val;
    }

    /**
     * Utility map getter.
     *
     * @param map Map to get value from.
     * @param key Map key (can be {@code null}).
     * @param c Optional factory closure for the default value to be put in when {@code key}
     *      is not found. If closure is not provided - {@code null} will be put into the map.
     * @param <K> Map key type.
     * @param <V> Map value type.
     * @return Value for the {@code key} or default value produced by {@code c} if key is not
     *      found (or {@code null} if key is not found and closure is not provided). Note that
     *      in case when key is not found the default value will be put into the map.
     * @throws GridClosureException Thrown in case when callable throws exception.
     * @see #newLinkedList()
     * @see #newList()
     * @see #newSet()
     * @see #newMap()
     * @see #newAtomicLong()
     * @see #newAtomicInt()
     * @see #newAtomicRef()
     * @see #newAtomicBoolean()
     */
    @Nullable public static <K, V> V addIfAbsent(Map<? super K, V> map, @Nullable K key,
        @Nullable Callable<? extends V> c) {
        A.notNull(map, "map");

        try {
            if (!map.containsKey(key)) {
                V v = c == null ? null : c.call();

                map.put(key, v);

                return v;
            }
            else
                return map.get(key);
        }
        catch (Exception e) {
            throw wrap(e);
        }
    }

    /**
     * Utility map getter.
     *
     * @param map Map to get value from.
     * @param key Map key (can be {@code null}).
     * @param v Optional value to be put in when {@code key} is not found.
     *      If not provided - {@code null} will be put into the map.
     * @param <K> Map key type.
     * @param <V> Map value type.
     * @return Value for the {@code key} or default value {@code c} if key is not
     *      found (or {@code null} if key is not found and value is not provided). Note that
     *      in case when key is not found the default value will be put into the map.
     */
    @Nullable public static <K, V> V addIfAbsent(Map<K, V> map, @Nullable K key, @Nullable V v) {
        A.notNull(map, "map");

        try {
            if (!map.containsKey(key)) {
                map.put(key, v);

                return v;
            }
            else
                return map.get(key);
        }
        catch (Exception e) {
            throw wrap(e);
        }
    }

    /**
     * Gets predicate that returns {@code true} if its free variable is contained
     * in given array.
     *
     * @param c Array to check for containment.
     * @param <T> Type of the free variable for the predicate and type of the
     *      array elements.
     * @return Predicate that returns {@code true} if its free variable is
     *      contained in given array.
     */
    public static <T> IgnitePredicate<T> in(@Nullable T[] c) {
        return isEmpty(c) ? GridFunc.<T>alwaysFalse() : in(asList(c));
    }

    /**
     * Gets predicate that returns {@code true} if its free variable is not
     * contained in given array.
     *
     * @param c Array to check for containment.
     * @param <T> Type of the free variable for the predicate and type of the
     *      array elements.
     * @return Predicate that returns {@code true} if its free variable is not
     *      contained in given array.
     */
    public static <T> IgnitePredicate<T> notIn(@Nullable T[] c) {
        return isEmpty(c) ? GridFunc.<T>alwaysTrue() : notIn(asList(c));
    }

    /**
     * Reduces collection into single value using given for-all closure.
     *
     * @param c Collection to reduce.
     * @param f For-all closure used for reduction.
     * @param <X> Type of the free variable for the closure and type of the
     *      collection elements.
     * @param <Y> Type of the closure's return value.
     * @return Single value as a result of collection reduction.
     */
    public static <X, Y> Y reduce(Iterable<? extends X> c, IgniteReducer<? super X, Y> f) {
        A.notNull(c, "c", f, "f");

        for (X x : c)
            if (!f.collect(x))
                break;

        return f.reduce();
    }

    /**
     * Reduces given map into single value using given for-all closure.
     *
     * @param m Map to reduce.
     * @param f For-all closure used for reduction.
     * @param <X> Type of the free variable for the closure and type of the map keys.
     * @param <Y> Type of the closure's return value and type of the map values.
     * @param <R> Type of the return value for the closure.
     * @return Single value as a result of map reduction.
     */
    @Nullable public static <X, Y, R> R reduce(Map<? extends X, ? extends Y> m,
        IgniteReducer2<? super X, ? super Y, R> f) {
        A.notNull(m, "m", f, "f");

        for (Map.Entry<? extends X, ? extends Y> e : m.entrySet())
            if (!f.collect(e.getKey(), e.getValue()))
                break;

        return f.apply();
    }

    /**
     * Calls given {@code side-effect only} closure over the each element of the provided
     * collection.
     *
     * @param c Collection to call closure over.
     * @param f Side-effect only closure to call over the collection.
     * @param p Optional set of predicates. Only if collection element evaluates
     *      to {@code true} for given predicates the closure will be applied to it.
     *      If no predicates provided - closure will be applied to all collection
     *      elements.
     * @param <X> Type of the free variable for the closure and type of the
     *      collection elements.
     */
    public static <X> void forEach(Iterable<? extends X> c, IgniteInClosure<? super X> f,
        @Nullable IgnitePredicate<? super X>... p) {
        A.notNull(c, "c", f, "f");

        for (X x : c)
            if (isAll(x, p))
                f.apply(x);
    }

    /**
     * Calls given {@code side-effect only} closure over the each element of the provided array.
     *
     * @param c Array to call closure over.
     * @param f Side-effect only closure to call over the array.
     * @param p Optional set of predicates. Only if collection element evaluates
     *      to {@code true} for given predicates the closure will be applied to it.
     *      If no predicates provided - closure will be applied to all collection
     *      elements.
     * @param <X> Type of the free variable for the closure and type of the array
     *      elements.
     */
    @SuppressWarnings("RedundantTypeArguments")
    public static <X> void forEach(X[] c, IgniteInClosure<? super X> f, @Nullable IgnitePredicate<? super X>... p) {
        A.notNull(c, "c", f, "f");

        F.<X>forEach(asList(c), f, p);
    }

    /**
     * Adds (copies) to given collection all elements in <tt>'from'</tt> array.
     *
     * @param to Collection to copy to.
     * @param from Array to copy from.
     * @param <T> Type of the free variable for the predicate and type of the collection elements.
     * @return Collection to copy to.
     */
    public static <T> Collection<T> copy(Collection<T> to, T... from) {
        A.notNull(to, "to", from, "from");

        copy(to, asList(from));

        return to;
    }

    /**
     * Adds (copies) to given collection using provided predicates. Element is copied if all
     * predicates evaluate to {@code true}.
     *
     * @param to Collection to copy to.
     * @param from Collection to copy from.
     * @param p Optional set of predicates to use for filtration.
     * @param <T> Type of the free variable for the predicate and type of the collection elements.
     * @return Collection to copy to.
     */
    public static <T> Collection<T> copy(Collection<T> to, Iterable<? extends T> from,
        @Nullable IgnitePredicate<? super T>... p) {
        A.notNull(to, "to", from, "from");

        if (!isAlwaysFalse(p)) {
            for (T t : from) {
                if (isAll(t, p))
                    to.add(t);
            }
        }

        return to;
    }

    /**
     * Transforms one collection to another using provided closure and predicate. Value from
     * the initial collection will be transformed only if all predicates evaluate to {@code true}
     * for this value. Note that no new collection will be created.
     *
     * @param to Destination collection to transform to.
     * @param from Initial collection to transform.
     * @param f Closure to use for transformation.
     * @param p Optional predicates to use for transformation.
     * @param <X> Type of the free variable for the closure and type of the collection elements.
     * @param <Y> Type of the closure's return value.
     * @return Destination collection.
     */
    public static <X, Y> Collection<Y> transform(Collection<Y> to, Iterable<? extends X> from,
        IgniteClosure<? super X, Y> f, @Nullable IgnitePredicate<? super X>... p) {
        A.notNull(to, "to", from, "from", f, "f");

        if (!isAlwaysFalse(p)) {
            for (X x : from) {
                if (isAll(x, p))
                    to.add(f.apply(x));
            }
        }

        return to;
    }

    /**
     * Calls given {@code side-effect only} closure (if provided) over the each element of the
     * provided map. If provided closure is {@code null} this method is no-op.
     *
     * @param m Map to call closure over.
     * @param f Optional closure to call over the map.
     * @param p Optional set of predicates. Only if map entry evaluates to {@code true} for given
     *      predicates the closure will be applied to it. If no predicates provided - closure will
     *      be applied to all map entries.
     * @param <K> Type of the free variable for the closure and type of the map keys.
     * @param <V> Type of the closure's return value and type of the map values.
     */
    public static <K, V> void forEach(Map<? extends K, ? extends V> m, IgniteInClosure<? super IgniteBiTuple<K, V>> f,
        @Nullable IgnitePredicate<? super IgniteBiTuple<K, V>>... p) {
        A.notNull(m, "m");

        if (!isAlwaysFalse(p)) {
            if (f != null) {
                for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                    IgniteBiTuple<K, V> t = F.t(e.getKey(), e.getValue());

                    if (isAll(t, p))
                        f.apply(t);
                }
            }
        }
    }

    /**
     * Transforms one collection to another using provided closure. New collection will be created.
     *
     * @param c Initial collection to transform.
     * @param f Closure to use for transformation.
     * @param <X> Type of the free variable for the closure and type of the collection elements.
     * @param <Y> Type of the closure's return value.
     * @return Transformed newly created collection.
     */
    public static <X, Y> Collection<Y> transform(Collection<? extends X> c, IgniteClosure<? super X, Y> f) {
        A.notNull(c, "c", f, "f");

        Collection<Y> d = new ArrayList<>(c.size());

        for (X x : c)
            d.add(f.apply(x));

        return d;
    }

    /**
     * Upcasts collection type.
     *
     * @param c Initial collection.
     * @return Resulting collection.
     */
    @SuppressWarnings("unchecked")
    public static <T extends R, R> Collection<R> upcast(Collection<T> c) {
        A.notNull(c, "c");

        return (Collection<R>)c;
    }

    /**
     * Transforms an array to read only collection using provided closure.
     *
     * @param c Initial array to transform.
     * @param f Closure to use for transformation.
     * @param <X> Type of the free variable for the closure and type of the array elements.
     * @param <Y> Type of the closure's return value.
     * @return Transformed read only collection.
     */
    public static <X, Y> Collection<Y> transform(X[] c, IgniteClosure<? super X, Y> f) {
        A.notNull(c, "c", f, "f");

        return viewReadOnly(asList(c), f);
    }

    /**
     * Tests if all provided predicates evaluate to {@code true} for given value. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code false} is found.
     *
     * @param t Value to test.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code true}.
     * @param <T> Type of the value and free variable of the predicates.
     * @return Returns {@code true} if given set of predicates is {@code null}, is empty, or all predicates
     *      evaluate to {@code true} for given value, {@code false} otherwise.
     */
    public static <T> boolean isAll(@Nullable T t, @Nullable IgnitePredicate<? super T>... p) {
        if (p != null)
            for (IgnitePredicate<? super T> r : p)
                if (r != null && !r.apply(t))
                    return false;

        return true;
    }

    /**
     * Tests if all provided predicates evaluate to {@code true} for given value. Note that evaluation will be
     * short-circuit when first predicate evaluated to {@code false} is found.
     *
     * @param t Value to test.
     * @param p Optional set of predicates to use for evaluation.
     * @param <T> Type of the value and free variable of the predicates.
     * @return Returns {@code true} if given set of predicates is {@code null}, is empty, or all predicates
     *      evaluate to {@code true} for given value, {@code false} otherwise.
     */
    public static <T> boolean isAll(@Nullable T t, @Nullable Iterable<? extends IgnitePredicate<? super T>> p) {
        if (p != null)
            for (IgnitePredicate<? super T> r : p)
                if (r != null && !r.apply(t))
                    return false;

        return true;
    }

    /**
     * Tests if all provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code false} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code true}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @return Returns {@code true} if all predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B> boolean isAll2(@Nullable A a, @Nullable B b,
        @Nullable IgniteBiPredicate<? super A, ? super B> p) {
        return p == null || isAll2(a, b, p == null ? Collections.<IgniteBiPredicate<? super A,? super B>>emptyList() :
            Collections.singletonList(p));
    }

    /**
     * Tests if all provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code false} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code true}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @return Returns {@code true} if all predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B> boolean isAll2(@Nullable A a, @Nullable B b,
        @Nullable IgniteBiPredicate<? super A, ? super B>[] p) {
        return isEmpty(p) || isAll2(a, b, asList(p));
    }

    /**
     * Tests if all provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code false} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code true}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @return Returns {@code true} if all predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B> boolean isAll2(@Nullable A a, @Nullable B b,
        @Nullable Iterable<? extends IgniteBiPredicate<? super A, ? super B>> p) {
        if (!isEmpty(p)) {
            assert p != null;

            for (IgniteBiPredicate<? super A, ? super B> r : p) {
                if (r != null && !r.apply(a, b))
                    return false;
            }
        }

        return true;
    }

    /**
     * Tests if all provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code false} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param c 3d value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code true}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @param <C> Type of the 3d value for the predicates.
     * @return Returns {@code true} if all predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B, C> boolean isAll3(@Nullable A a, @Nullable B b, @Nullable C c,
        @Nullable GridPredicate3<? super A, ? super B, ? super C>... p) {
        return isEmpty(p) || isAll3(a, b, c, asList(p));
    }

    /**
     * Tests if all provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code false} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param c 3d value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code true}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @param <C> Type of the 3d value for the predicates.
     * @return Returns {@code true} if all predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B, C> boolean isAll3(@Nullable A a, @Nullable B b, @Nullable C c,
        @Nullable Iterable<? extends GridPredicate3<? super A, ? super B, ? super C>> p) {
        if (!isEmpty(p)) {
            assert p != null;

            for (GridPredicate3<? super A, ? super B, ? super C> r : p) {
                if (r != null && !r.apply(a, b, c))
                    return false;
            }
        }

        return true;
    }

    /**
     * Tests if any of provided predicates evaluate to {@code true} for given value. Note
     * that evaluation will be short-circuit when first predicate evaluated to {@code true}
     * is found.
     *
     * @param t Value to test.
     * @param p Optional set of predicates to use for evaluation.
     * @param <T> Type of the value and free variable of the predicates.
     * @return Returns {@code true} if any of predicates evaluates to {@code true} for given
     *      value, {@code false} otherwise. Returns {@code false} if given set of predicates
     *      is {@code null} or empty.
     */
    public static <T> boolean isAny(@Nullable T t, @Nullable IgnitePredicate<? super T>... p) {
        if (p != null)
            for (IgnitePredicate<? super T> r : p)
                if (r != null && r.apply(t))
                    return true;

        return false;
    }

    /**
     * Tests if any of provided predicates evaluate to {@code true} for given value. Note
     * that evaluation will be short-circuit when first predicate evaluated to {@code true}
     * is found.
     *
     * @param t Value to test.
     * @param p Optional set of predicates to use for evaluation.
     * @param <T> Type of the value and free variable of the predicates.
     * @return Returns {@code true} if any of predicates evaluates to {@code true} for given
     *      value, {@code false} otherwise. Returns {@code false} if given set of predicates
     *      is {@code null} or empty.
     */
    public static <T> boolean isAny(@Nullable T t, @Nullable Iterable<? extends IgnitePredicate<? super T>> p) {
        if (p != null)
            for (IgnitePredicate<? super T> r : p)
                if (r != null && r.apply(t))
                    return true;

        return false;
    }

    /**
     * Tests if any of provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code true} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code false}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @return Returns {@code true} if any of predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B> boolean isAny2(@Nullable A a, @Nullable B b,
        @Nullable IgniteBiPredicate<? super A, ? super B>... p) {
        return !isEmpty(p) && isAny2(a, b, asList(p));
    }

    /**
     * Tests if any of provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code true} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code false}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @return Returns {@code true} if any of predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B> boolean isAny2(@Nullable A a, @Nullable B b,
        @Nullable Iterable<? extends IgniteBiPredicate<? super A, ? super B>> p) {
        if (!isEmpty(p)) {
            assert p != null;

            for (IgniteBiPredicate<? super A, ? super B> r : p) {
                if (r != null && r.apply(a, b))
                    return true;
            }
        }

        return false;
    }

    /**
     * Tests if any of provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code true} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param c 3d value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code false}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @param <C> Type of the 3d value for the predicates.
     * @return Returns {@code true} if any of predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B, C> boolean isAny3(@Nullable A a, @Nullable B b, @Nullable C c,
        @Nullable GridPredicate3<? super A, ? super B, ? super C>... p) {
        return !isEmpty(p) && isAny3(a, b, c, asList(p));
    }

    /**
     * Tests if any of provided predicates evaluate to {@code true} for given values. Note that
     * evaluation will be short-circuit when first predicate evaluated to {@code true} is found.
     *
     * @param a 1st value for predicate.
     * @param b 2nd value for predicate.
     * @param c 3d value for predicate.
     * @param p Optional set of predicates to use for evaluation. If no predicates provides
     *      this method will always return {@code false}.
     * @param <A> Type of the 1st value for the predicates.
     * @param <B> Type of the 2nd value for the predicates.
     * @param <C> Type of the 3d value for the predicates.
     * @return Returns {@code true} if any of predicates evaluate to {@code true} for given
     *      values, {@code false} otherwise.
     */
    public static <A, B, C> boolean isAny3(@Nullable A a, @Nullable B b, @Nullable C c,
        @Nullable Iterable<? extends GridPredicate3<? super A, ? super B, ? super C>> p) {
        if (!isEmpty(p)) {
            assert p != null;

            for (GridPredicate3<? super A, ? super B, ? super C> r : p) {
                if (r != null && r.apply(a, b, c))
                    return true;
            }
        }

        return false;
    }

    /**
     * Transforms one collection to another using provided closure and optional set of
     * predicates. Value from the initial collection will be transformed only if all
     * predicates evaluate to {@code true} for this value. Note that new collection will
     * be created.
     *
     * @param c Initial collection to transform.
     * @param f Closure to use for transformation.
     * @param p Optional set of predicate to use for transformation.
     * @param <X> Type of the free variable for the closure and type of the collection elements.
     * @param <Y> Type of the closure's return value.
     * @return Transformed newly created collection.
     */
    public static <X, Y> Collection<Y> transform(Collection<? extends X> c, IgniteClosure<? super X, Y> f,
        IgnitePredicate<? super X>... p) {
        A.notNull(c, "c", f, "f");

        Collection<Y> d = new ArrayList<>(c.size());

        if (!isAlwaysFalse(p)) {
            for (X x : c) {
                if (isAll(x, p))
                    d.add(f.apply(x));
            }
        }

        return d;
    }

    /**
     * Gets absolute (no-arg) predicate that returns {@code true} first {@code n}-times
     * it's called - and {@code false} thereafter.
     *
     * @param n Number of calls this predicate will return {@code true}.
     * @return Predicate that returns {@code true} first {@code n}-times it's
     *      called - and {@code false} thereafter.
     */
    public static GridAbsPredicate limit(final int n) {
        return new GridAbsPredicate() {
            private AtomicInteger cnt = new AtomicInteger(0);

            @Override public boolean apply() {
                return cnt.incrementAndGet() < n;
            }
        };
    }

    /**
     * Creates an absolute (no-arg) closure that does nothing.
     *
     * @return Absolute (no-arg) closure that does nothing.
     */
    public static GridAbsClosure noop() {
        return NOOP;
    }

    /**
     * Finds and returns first element in given collection for which any of the
     * provided predicates evaluates to {@code true}.
     *
     * @param c Input collection.
     * @param dfltVal Default value to return when no element is found.
     * @param p Optional set of finder predicates.
     * @param <V> Type of the collection elements.
     * @return First element in given collection for which predicate evaluates to
     *      {@code true} - or {@code null} if such element cannot be found.
     */
    @Nullable public static <V> V find(Iterable<? extends V> c, @Nullable V dfltVal,
        @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        if (!isEmpty(p) && !isAlwaysFalse(p)) {
            for (V v : c) {
                if (isAny(v, p))
                    return v;
            }
        }

        return dfltVal;
    }

    /**
     * Finds and returns first element in given array for which any of the provided
     * predicates evaluates to {@code true}.
     *
     * @param c Input array.
     * @param dfltVal Default value to return when no element is found.
     * @param p Optional set of finder predicates.
     * @param <V> Type of the array elements.
     * @return First element in given array for which predicate evaluates to
     *      {@code true} - or {@code null} if such element cannot be found.
     */
    @Nullable public static <V> V find(V[] c, @Nullable V dfltVal, @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        return find(asList(c), dfltVal, p);
    }

    /**
     * Finds, transforms and returns first element in given collection for which any of
     * the provided predicates evaluates to {@code true}.
     *
     * @param c Input collection.
     * @param dfltVal Default value to return when no element is found.
     * @param f Transforming closure.
     * @param p Optional set of finder predicates.
     * @param <V> Type of the collection elements.
     * @return First element in given collection for which predicate evaluates to
     *      {@code true} - or {@code null} if such element cannot be found.
     */
    public static <V, Y> Y find(Iterable<? extends V> c, @Nullable Y dfltVal, IgniteClosure<? super V, Y> f,
        @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c", f, "f");

        if (isAlwaysTrue(p) && c.iterator().hasNext())
            return f.apply(c.iterator().next());

        if (!isEmpty(p) && !isAlwaysFalse(p)) {
            for (V v : c) {
                if (isAny(v, p))
                    return f.apply(v);
            }
        }

        return dfltVal;
    }

    /**
     * Finds, transforms and returns first element in given array for which any of the
     * provided predicates evaluates to {@code true}.
     *
     * @param c Input array.
     * @param dfltVal Default value to return when no element is found.
     * @param f Transforming closure.
     * @param p Optional set of finder predicates.
     * @param <V> Type of the array elements.
     * @return First element in given collection for which predicate evaluates to
     *      {@code true} - or {@code null} if such element cannot be found.
     */
    @SuppressWarnings("RedundantTypeArguments")
    public static <V, Y> Y find(V[] c, @Nullable Y dfltVal, IgniteClosure<? super V, Y> f,
        @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c", f, "f");

        return F.<V, Y>find(asList(c), dfltVal, f, p);
    }

    /**
     * Checks if collection {@code c1} contains any elements from collection {@code c2}.
     *
     * @param c1 Collection to check for containment. If {@code null} - this method returns {@code false}.
     * @param c2 Collection of elements to check. If {@code null} - this method returns {@code false}.
     * @param <T> Type of the elements.
     * @return {@code true} if collection {@code c1} contains at least one element from collection
     *      {@code c2}.
     */
    public static <T> boolean containsAny(@Nullable Collection<? extends T> c1, @Nullable Iterable<? extends T> c2) {
        if (c1 != null && !c1.isEmpty() && c2 != null && c2.iterator().hasNext())
            for (T t : c2)
                if (c1.contains(t))
                    return true;

        return false;
    }

    /**
     * Checks if collection {@code c1} contains any elements from array {@code c2}.
     *
     * @param c1 Collection to check for containment. If {@code null} - this method returns {@code false}.
     * @param c2 Collection of elements to check. If {@code null} - this method returns {@code false}.
     * @param <T> Type of the elements.
     * @return {@code true} if collection {@code c1} contains at least one element from collection
     *      {@code c2}.
     */
    public static <T> boolean containsAny(@Nullable Collection<? extends T> c1, @Nullable T... c2) {
        if (c1 != null && !c1.isEmpty() && c2 != null && c2.length > 0)
            for (T t : c2)
                if (c1.contains(t))
                    return true;

        return false;
    }

    /**
     * Checks if collection {@code c1} contains all elements from collection {@code c2}.
     *
     * @param c1 Collection to check for containment. If {@code null} - this method returns {@code false}.
     * @param c2 Collection of elements to check. If {@code null} - this method returns {@code true}
     *      meaning that {@code null}-collection is treated as empty collection.
     * @param <T> Type of the elements.
     * @return {@code true} if collection {@code c1} contains all elements from collection
     *      {@code c2}.
     */
    public static <T> boolean containsAll(@Nullable Collection<? extends T> c1, @Nullable Iterable<? extends T> c2) {
        if (c1 == null)
            return false;

        if (c2 != null)
            for (T t : c2)
                if (!c1.contains(t))
                    return false;

        return true;
    }

    /**
     * Splits given collection in two using provided set of predicates.
     *
     * @param c Collection to split. If {@code null} - pair of {@code null}s will be returned.
     *      If it's empty - pair of empty collections will be returned.
     * @param p Optional filter. All elements that satisfy all provided predicates
     *      will go into 1st collection in pair. The rest will be in 2nd collection.
     *      If not provided, pair of original and empty collection will be returned.
     * @param <T> Type of the collection.
     * @return Pair of two collections.
     */
    public static <T0, T extends T0> IgnitePair<Collection<T>> split(@Nullable Collection<T> c,
        @Nullable IgnitePredicate<? super T>... p) {
        if (c == null)
            return pair(null, null);

        if (c.isEmpty())
            return F.<Collection<T>>pair(Collections.<T>emptyList(), Collections.<T>emptyList());

        if (isEmpty(p))
            return pair(c, Collections.<T>emptyList());

        if (isAlwaysTrue(p))
            return pair(c, Collections.<T>emptyList());

        if (isAlwaysFalse(p))
            return pair(Collections.<T>emptyList(), c);

        Collection<T> c1 = new LinkedList<>();
        Collection<T> c2 = new LinkedList<>();

        for (T t : c) {
            if (isAll(t, p))
                c1.add(t);
            else
                c2.add(t);
        }

        return pair(c1, c2);
    }

    /**
     * Creates pair out of given two objects.
     *
     * @param t1 First object in pair.
     * @param t2 Second object in pair.
     * @param <T> Type of objects in pair.
     * @return Pair of objects.
     */
    public static <T> IgnitePair<T> pair(@Nullable T t1, @Nullable T t2) {
        return new IgnitePair<>(t1, t2);
    }

    /**
     * Creates triple out of given three objects.
     *
     * @param t1 First object in triple.
     * @param t2 Second object in triple.
     * @param t3 Third object in triple.
     * @param <T> Type of objects in triple.
     * @return Triple of objects.
     */
    public static <T> GridTriple<T> triple(@Nullable T t1, @Nullable T t2, @Nullable T t3) {
        return new GridTriple<>(t1, t2, t3);
    }

    /**
     * Partitions input collection in two: first containing elements for which given
     * predicate evaluates to {@code true} - and second containing the elements for which
     * predicate evaluates to {@code false}.
     *
     * @param c Input collection.
     * @param p Partitioning predicate.
     * @param <V> Type of the collection elements.
     * @return Tuple of two collections: first containing elements for which given predicate
     *      evaluates to {@code true} - and second containing the elements for which predicate
     *      evaluates to {@code false}.
     */
    public static <V> IgniteBiTuple<Collection<V>, Collection<V>> partition(Iterable<? extends V> c,
        IgnitePredicate<? super V> p) {
        A.notNull(c, "c", p, "p");

        Collection<V> c1 = new LinkedList<>();
        Collection<V> c2 = new LinkedList<>();

        for (V v : c) {
            if (p.apply(v))
                c1.add(v);
            else
                c2.add(v);
        }

        return t(c1, c2);
    }

    /**
     * Partitions input array in two: first containing elements for which given predicate
     * evaluates to {@code true} - and second containing the elements for which predicate
     * evaluates to {@code false}.
     *
     * @param c Input array.
     * @param p Partitioning predicate.
     * @param <V> Type of the array elements.
     * @return Tuple of two collections: first containing elements for which given predicate
     *      evaluates to {@code true} - and second containing the elements for which predicate
     *      evaluates to {@code false}.
     */
    public static <V> IgniteBiTuple<Collection<V>, Collection<V>> partition(V[] c, IgnitePredicate<? super V> p) {
        A.notNull(c, "c", p, "p");

        return partition(asList(c), p);
    }

    /**
     * Partitions input map in two: first containing entries for which given predicate evaluates
     * to {@code true} - and second containing the entries for which predicate evaluates to {@code false}.
     *
     * @param m Input map.
     * @param p Partitioning predicate.
     * @param <K> Type of the map keys.
     * @param <V> Type of the map values.
     * @return Tuple of two maps: first containing entries for which given predicate evaluates to
     *      {@code true} - and second containing the entries for which predicate evaluates to {@code false}.
     */
    public static <K, V> IgniteBiTuple<Map<K, V>, Map<K, V>> partition(Map<? extends K, ? extends V> m,
        IgniteBiPredicate<? super K, ? super V> p) {
        A.notNull(m, "m", p, "p");

        Map<K, V> m1 = new HashMap<>();
        Map<K, V> m2 = new HashMap<>();

        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            if (p.apply(e.getKey(), e.getValue()))
                m1.put(e.getKey(), e.getValue());
            else
                m2.put(e.getKey(), e.getValue());
        }

        return t(m1, m2);
    }

    /**
     * Checks for existence of the element in input collection for which all provided predicates
     * evaluate to {@code true}.
     *
     * @param c Input collection.
     * @param p Optional set of checking predicates.
     * @param <V> Type of the collection elements.
     * @return {@code true} if input collection contains element for which all the provided
     *      predicates evaluates to {@code true} - otherwise returns {@code false}.
     */
    public static <V> boolean exist(Iterable<? extends V> c, @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        if (isAlwaysFalse(p))
            return false;
        else if (isAlwaysTrue(p))
            return true;
        else if (isEmpty(p))
            return true;
        else
            for (V v : c)
                if (isAll(v, p))
                    return true;

        return false;
    }

    /**
     * Checks for existence of the element in input array for which all provided predicates
     * evaluate to {@code true}.
     *
     * @param c Input array.
     * @param p Optional set of checking predicates.
     * @param <V> Type of the array elements.
     * @return {@code true} if input array contains element for which all the provided predicates
     *      valuates to {@code true} - otherwise returns {@code false}.
     */
    public static <V> boolean exist(V[] c, @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        return exist(asList(c), p);
    }

    /**
     * Applies all given predicates to all elements in given input collection and returns
     * {@code true} if all of them evaluate to {@code true} for all elements. Returns
     * {@code false} otherwise.
     *
     * @param c Input collection.
     * @param p Optional set of checking predicates. If none provided - {@code true} is returned.
     * @param <V> Type of the collection element.
     * @return Returns {@code true} if all given predicates evaluate to {@code true} for
     *      all elements. Returns {@code false} otherwise.
     */
    public static <V> boolean forAll(Iterable<? extends V> c, @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        if (isAlwaysFalse(p))
            return false;
        else if (isAlwaysTrue(p))
            return true;
        else if (!isEmpty(p)) {
            for (V v : c) {
                if (!isAll(v, p))
                    return false;
            }
        }

        return true;
    }

    /**
     * Applies all given predicates to all elements in given input array and returns {@code true}
     * if all of them evaluate to {@code true} for all elements. Returns {@code false} otherwise.
     *
     * @param c Input array.
     * @param p Optional set of checking predicates.
     * @param <V> Type of the array element.
     * @return Returns {@code true} if all given predicates evaluate to {@code true} for all elements.
     *      Returns {@code false} otherwise.
     */
    public static <V> boolean forAll(V[] c, @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        return forAll(asList(c), p);
    }

    /**
     * Checks for existence of the entry in input map for which all provided predicates
     * evaluate to {@code true}.
     *
     * @param m Input map.
     * @param p Optional set of checking predicate.
     * @param <K> Type of the map keys.
     * @param <V> Type of the map values.
     * @return {@code true} if input map contains entry for which all provided predicates
     *      evaluate to {@code true} - otherwise returns {@code false}.
     */
    public static <K1, K extends K1, V1, V extends V1> boolean exist(Map<K, V> m,
        @Nullable IgnitePredicate<? super Map.Entry<K, V>>... p) {
        A.notNull(m, "m");

        if (isAlwaysFalse(p))
            return false;
        else if (isAlwaysTrue(p))
            return true;
        else if (isEmpty(p))
            return true;
        else
            for (Map.Entry<K, V> e : m.entrySet())
                if (isAll(e, p))
                    return true;

        return false;
    }

    /**
     * Applies given predicates to all entries in given input map and returns {@code true}
     * if all of them evaluates to {@code true} for all entries. Returns {@code false} otherwise.
     *
     * @param m Input map.
     * @param p Optional set of checking predicate.
     * @param <K> Type of the map keys.
     * @param <V> Type of the map values.
     * @return Returns {@code true} if all given predicate evaluates to {@code true} for all
     *      entries. Returns {@code false} otherwise.
     */
    public static <K1, K extends K1, V1, V extends V1> boolean forAll(Map<K, V> m,
        @Nullable IgnitePredicate<? super Map.Entry<K, V>>... p) {
        A.notNull(m, "m");

        if (isAlwaysFalse(p))
            return false;
        else if (isAlwaysTrue(p))
            return true;
        else if (!isEmpty(p))
            for (Map.Entry<K, V> e : m.entrySet())
                if (!isAll(e, p))
                    return false;

        return true;
    }

    /**
     * Applies all given predicates to all elements in given input collection and returns
     * {@code true} if all predicates evaluate to {@code true} for at least one element. Returns
     * {@code false} otherwise. Processing will short-circuit after first element evaluates to
     * {@code true} for all predicates.
     *
     * @param c Input collection.
     * @param p Optional set of checking predicates. If none provided - {@code true} is returned.
     * @param <V> Type of the collection element.
     * @return Returns {@code true} if all given predicates evaluate to {@code true} for
     *      at least one element. Returns {@code false} otherwise.
     */
    public static <V> boolean forAny(Iterable<? extends V> c, @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        if (!c.iterator().hasNext())
            return false;
        else if (isEmpty(p))
            return true;
        else if (isAlwaysFalse(p))
            return false;
        else if (isAlwaysTrue(p))
            return true;
        else {
            for (V v : c)
                if (isAll(v, p))
                    return true;

            return false;
        }
    }

    /**
     * Applies all given predicates to all elements in given input array and returns {@code true}
     * if all of them evaluate to {@code true} for at least one element. Returns {@code false}
     * otherwise. Processing will short-circuit after first element evaluates to
     * {@code true} for all predicates.
     *
     * @param c Input array.
     * @param p Optional set of checking predicates.
     * @param <V> Type of the array element.
     * @return Returns {@code true} if all given predicates evaluate to {@code true} for at
     *      least one element. Returns {@code false} otherwise.
     */
    public static <V> boolean forAny(V[] c, @Nullable IgnitePredicate<? super V>... p) {
        A.notNull(c, "c");

        return forAny(asList(c), p);
    }

    /**
     * Applies given predicates to all entries in given input map and returns {@code true}
     * if all of them evaluates to {@code true} for at least one entry. Returns {@code false}
     * otherwise. Processing will short-circuit after first entry evaluates to
     * {@code true} for all predicates.
     *
     * @param m Input map.
     * @param p Optional set of checking predicate.
     * @param <K> Type of the map keys.
     * @param <V> Type of the map values.
     * @return Returns {@code true} if all given predicate evaluates to {@code true} for at
     *      least one entry. Returns {@code false} otherwise.
     */
    public static <K1, K extends K1, V1, V extends V1> boolean forAny(Map<K, V> m,
        @Nullable IgnitePredicate<? super Map.Entry<K, V>>... p) {
        A.notNull(m, "m");

        if (isAlwaysFalse(p))
            return false;
        else if (isAlwaysTrue(p))
            return true;
        else if (!isEmpty(p)) {
            for (Map.Entry<K, V> e : m.entrySet()) {
                if (isAll(e, p))
                    return true;
            }

            return false;
        }

        return true;
    }

    /**
     * Folds-right given collection using provided closure. If input collection contains <tt>a<sub>1</sub>,
     * a<sub>2</sub>, ..., a<sub>n</sub></tt> result value will be
     * <tt>...f(f(f(b,a<sub>1</sub>),a<sub>2</sub>),a<sub>3</sub>)...</tt>
     * where {@code f(x)} is the result of applying each closure from {@code fs} set to the
     * element {@code x} of the input collection {@code c}.
     * <p>
     * For example:
     * <pre name="code" class="java">
     * ...
     *
     * Collection&lt;Integer&gt; nums = new ArrayList&lt;Integer&gt;(size);
     *
     * // Search max value.
     * Integer max = F.fold(nums, F.first(nums), new C2&lt;Integer, Integer, Integer&gt;() {
     *     public Integer apply(Integer n, Integer max) { return Math.max(n, max); }
     * });
     *
     * ...
     * </pre>
     *
     * @param c Input collection.
     * @param b Optional first folding pair element.
     * @param fs Optional set of folding closures.
     * @param <D> Type of the input collection elements and type of the free variable for the closure.
     * @param <B> Type of the folding value and return type of the closure.
     * @return Value representing folded collection.
     */
    @Nullable public static <D, B> B fold(Iterable<? extends D> c, @Nullable B b,
        @Nullable IgniteBiClosure<? super D, ? super B, B>... fs) {
        A.notNull(c, "c");

        if (!isEmpty(fs))
            for (D e : c) {
                assert fs != null;

                for (IgniteBiClosure<? super D, ? super B, B> f : fs)
                    b = f.apply(e, b);
            }

        return b;
    }

    /**
     * Folds-right given array using provided closure. If input array contains
     * <tt>a<sub>1</sub>, a<sub>2</sub>, ...,
     * a<sub>n</sub></tt> result value will be <tt>...f(f(f(b,a<sub>1</sub>),a<sub>2</sub>),a<sub>3</sub>)...</tt>
     * where {@code f(x)} is the result of applying each closure from {@code fs} set to
     * the element {@code x} of the input collection {@code c}.
     *
     * @param c Input array.
     * @param b Optional first folding pair element.
     * @param fs Optional set of folding closures.
     * @param <D> Type of the input array elements and type of the free variable for the closure.
     * @param <B> Type of the folding value and return type of the closure.
     * @return Value representing folded array.
     */
    @Nullable
    public static <D, B> B fold(D[] c, @Nullable B b, @Nullable IgniteBiClosure<? super D, ? super B, B>... fs) {
        A.notNull(c, "c");

        return fold(asList(c), b, fs);
    }

    /**
     * Factory method returning new tuple with given parameter.
     *
     * @param v Parameter for tuple.
     * @param <V> Type of the tuple.
     * @return Newly created tuple.
     */
    public static <V> GridTuple<V> t(@Nullable V v) {
        return new GridTuple<>(v);
    }

    /**
     * Factory method returning empty tuple.
     *
     * @param <V> Type of the tuple.
     * @return Newly created empty tuple.
     */
    public static <V> GridTuple<V> t1() {
        return new GridTuple<>();
    }

    /**
     * Factory method returning new tuple with given parameters.
     *
     * @param v1 1st parameter for tuple.
     * @param v2 2nd parameter for tuple.
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @return Newly created tuple.
     */
    public static <V1, V2> IgniteBiTuple<V1, V2> t(@Nullable V1 v1, @Nullable V2 v2) {
        return new IgniteBiTuple<>(v1, v2);
    }

    /**
     * Factory method returning new empty tuple.
     *
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @return Newly created empty tuple.
     */
    public static <V1, V2> IgniteBiTuple<V1, V2> t2() {
        return new IgniteBiTuple<>();
    }

    /**
     * Factory method returning new tuple with given parameters.
     *
     * @param v1 1st parameter for tuple.
     * @param v2 2nd parameter for tuple.
     * @param v3 3rd parameter for tuple.
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @return Newly created tuple.
     */
    public static <V1, V2, V3> GridTuple3<V1, V2, V3> t(@Nullable V1 v1, @Nullable V2 v2, @Nullable V3 v3) {
        return new GridTuple3<>(v1, v2, v3);
    }

    /**
     * Factory method returning new tuple with given parameters.
     *
     * @param v1 1st parameter for tuple.
     * @param v2 2nd parameter for tuple.
     * @param v3 3rd parameter for tuple.
     * @param v4 4th parameter for tuple.
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @param <V4> Type of the 4th tuple parameter.
     * @return Newly created tuple.
     */
    public static <V1, V2, V3, V4> GridTuple4<V1, V2, V3, V4> t(@Nullable V1 v1, @Nullable V2 v2, @Nullable V3 v3,
        @Nullable V4 v4) {
        return new GridTuple4<>(v1, v2, v3, v4);
    }

    /**
     * Factory method returning new tuple with given parameters.
     *
     * @param v1 1st parameter for tuple.
     * @param v2 2nd parameter for tuple.
     * @param v3 3rd parameter for tuple.
     * @param v4 4th parameter for tuple.
     * @param v5 5th parameter for tuple.
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @param <V4> Type of the 4th tuple parameter.
     * @param <V5> Type of the 5th tuple parameter.
     * @return Newly created tuple.
     */
    public static <V1, V2, V3, V4, V5> GridTuple5<V1, V2, V3, V4, V5> t(@Nullable V1 v1, @Nullable V2 v2,
        @Nullable V3 v3, @Nullable V4 v4, @Nullable V5 v5) {
        return new GridTuple5<>(v1, v2, v3, v4, v5);
    }

    /**
     * Factory method returning new tuple with given parameters.
     *
     * @param v1 1st parameter for tuple.
     * @param v2 2nd parameter for tuple.
     * @param v3 3rd parameter for tuple.
     * @param v4 4th parameter for tuple.
     * @param v5 5th parameter for tuple.
     * @param v6 5th parameter for tuple.
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @param <V4> Type of the 4th tuple parameter.
     * @param <V5> Type of the 5th tuple parameter.
     * @param <V6> Type of the 6th tuple parameter.
     * @return Newly created tuple.
     */
    public static <V1, V2, V3, V4, V5, V6> GridTuple6<V1, V2, V3, V4, V5, V6> t(@Nullable V1 v1, @Nullable V2 v2,
        @Nullable V3 v3, @Nullable V4 v4, @Nullable V5 v5, @Nullable V6 v6) {
        return new GridTuple6<>(v1, v2, v3, v4, v5, v6);
    }

    /**
     * Creates vararg tuple with given values.
     *
     * @param objs Values for vararg tuple.
     * @return Vararg tuple with given values.
     */
    public static GridTupleV tv(Object... objs) {
        assert objs != null;

        return new GridTupleV(objs);
    }

    /**
     * Factory method returning new empty tuple.
     *
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @return Newly created empty tuple.
     */
    public static <V1, V2, V3> GridTuple3<V1, V2, V3> t3() {
        return new GridTuple3<>();
    }

    /**
     * Factory method returning new empty tuple.
     *
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @param <V4> Type of the 4th tuple parameter.
     * @return Newly created empty tuple.
     */
    public static <V1, V2, V3, V4> GridTuple4<V1, V2, V3, V4> t4() {
        return new GridTuple4<>();
    }

    /**
     * Factory method returning new empty tuple.
     *
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @param <V4> Type of the 4th tuple parameter.
     * @param <V5> Type of the 5th tuple parameter.
     * @return Newly created empty tuple.
     */
    public static <V1, V2, V3, V4, V5> GridTuple5<V1, V2, V3, V4, V5> t5() {
        return new GridTuple5<>();
    }

    /**
     * Factory method returning new empty tuple.
     *
     * @param <V1> Type of the 1st tuple parameter.
     * @param <V2> Type of the 2nd tuple parameter.
     * @param <V3> Type of the 3rd tuple parameter.
     * @param <V4> Type of the 4th tuple parameter.
     * @param <V5> Type of the 5th tuple parameter.
     * @param <V6> Type of the 6th tuple parameter.
     * @return Newly created empty tuple.
     */
    public static <V1, V2, V3, V4, V5, V6> GridTuple6<V1, V2, V3, V4, V5, V6> t6() {
        return new GridTuple6<>();
    }

    /**
     * Converts collection to map using collection values as keys and
     * passed in default value as values.
     *
     * @param keys Map keys.
     * @param dfltVal Default value.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Resulting map.
     */
    public static <K, V> Map<K, V> zip(Collection<? extends K> keys, V dfltVal) {
        A.notNull(keys, "keys");

        Map<K, V> m = new HashMap<>(keys.size(), 1.0f);

        for (K k : keys) {
            m.put(k, dfltVal);
        }

        return m;
    }

    /**
     * Converts 2 collections into map using first collection as keys and second
     * collection as values. Note that collections must be of equal length.
     *
     * @param keys Map keys.
     * @param vals Map values.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Resulting map.
     */
    public static <K, V> Map<K, V> zip(Collection<? extends K> keys, Collection<? extends V> vals) {
        A.notNull(keys, "keys", vals, "vals");
        A.ensure(keys.size() == vals.size(), "keys.size() == vals.size()");

        Map<K, V> m = new HashMap<>(keys.size(), 1.0f);

        Iterator<? extends V> it = vals.iterator();

        for (K k : keys) {
            m.put(k, it.next());
        }

        return m;
    }

    /**
     * Creates map with given values.
     *
     * @param k Key.
     * @param v Value.
     * @param <K> Key's type.
     * @param <V> Value's type.
     * @return Created map.
     */
    public static <K, V> Map<K, V> asMap(K k, V v) {
        Map<K, V> map = new GridLeanMap<>(1);

        map.put(k, v);

        return map;
    }

    /**
     * Creates map with given values.
     *
     * @param k1 Key 1.
     * @param v1 Value 1.
     * @param k2 Key 2.
     * @param v2 Value 2.
     * @param <K> Key's type.
     * @param <V> Value's type.
     * @return Created map.
     */
    public static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2) {
        Map<K, V> map = new GridLeanMap<>(2);

        map.put(k1, v1);
        map.put(k2, v2);

        return map;
    }

    /**
     * Creates map with given values.
     *
     * @param k1 Key 1.
     * @param v1 Value 1.
     * @param k2 Key 2.
     * @param v2 Value 2.
     * @param k3 Key 3.
     * @param v3 Value 3.
     * @param <K> Key's type.
     * @param <V> Value's type.
     * @return Created map.
     */
    public static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2, K k3, V v3) {
        Map<K, V> map = new GridLeanMap<>(3);

        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);

        return map;
    }

    /**
     * Creates map with given values.
     *
     * @param k1 Key 1.
     * @param v1 Value 1.
     * @param k2 Key 2.
     * @param v2 Value 2.
     * @param k3 Key 3.
     * @param v3 Value 3.
     * @param k4 Key 4.
     * @param v4 Value 4.
     * @param <K> Key's type.
     * @param <V> Value's type.
     * @return Created map.
     */
    public static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        Map<K, V> map = new GridLeanMap<>(4);

        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);

        return map;
    }

    /**
     * Creates map with given values.
     *
     * @param k1 Key 1.
     * @param v1 Value 1.
     * @param k2 Key 2.
     * @param v2 Value 2.
     * @param k3 Key 3.
     * @param v3 Value 3.
     * @param k4 Key 4.
     * @param v4 Value 4.
     * @param k5 Key 5.
     * @param v5 Value 5.
     * @param <K> Key's type.
     * @param <V> Value's type.
     * @return Created map.
     */
    public static <K, V> Map<K, V> asMap(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        Map<K, V> map = new GridLeanMap<>(5);

        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        map.put(k4, v4);
        map.put(k5, v5);

        return map;
    }

    /**
     * Convenience method to convert multiple elements into array.
     *
     * @param t Elements to convert into array.
     * @param <T> Element type.
     * @return Array of elements.
     */
    public static <T> T[] asArray(T... t) {
        return t;
    }

    /**
     * Creates read-only list with given values.
     *
     * @param t Element (if {@code null}, then empty list is returned).
     * @param <T> Element's type.
     * @return Created list.
     */
    public static <T> List<T> asList(@Nullable T t) {
        return t == null ? Collections.<T>emptyList() : Collections.singletonList(t);
    }

    /**
     * Creates read-only set with given value.
     *
     * @param t Element (if {@code null}, then empty set is returned).
     * @param <T> Element's type.
     * @return Created set.
     */
    public static <T> Set<T> asSet(@Nullable T t) {
        return t == null ? Collections.<T>emptySet() : Collections.singleton(t);
    }

    /**
     * Creates read-only set with given values.
     *
     * @param t Element (if {@code null}, then empty set is returned).
     * @param <T> Element's type.
     * @return Created set.
     */
    @SuppressWarnings( {"RedundantTypeArguments"})
    public static <T> Set<T> asSet(@Nullable T... t) {
        if (t == null || t.length == 0)
            return Collections.<T>emptySet();

        if (t.length == 1)
            return Collections.singleton(t[0]);

        return new GridLeanSet<>(asList(t));
    }

    /**
     * Checks if value is contained in the collection passed in. If the collection
     * is {@code null}, then {@code false} is returned.
     *
     * @param c Collection to check.
     * @param t Value to check for containment.
     * @param <T> Value type.
     * @return {@code True} if collection is not {@code null} and contains given
     *      value, {@code false} otherwise.
     */
    public static <T> boolean contains(@Nullable Collection<T> c, @Nullable T t) {
        return c != null && c.contains(t);
    }

    /**
     * Provides predicate which returns {@code true} if it receives an element
     * that is contained in the passed in collection.
     *
     * @param c Collection used for predicate filter.
     * @param <T> Element type.
     * @return Predicate which returns {@code true} if it receives an element
     *  that is contained in the passed in collection.
     */
    public static <T> IgnitePredicate<T> contains(@Nullable final Collection<T> c) {
        return c == null || c.isEmpty() ? GridFunc.<T>alwaysFalse() : new P1<T>() {
            @Override public boolean apply(T t) {
                return c.contains(t);
            }
        };
    }

    /**
     * Provides predicate which returns {@code true} if it receives an element
     * that is not contained in the passed in collection.
     *
     * @param c Collection used for predicate filter.
     * @param <T> Element type.
     * @return Predicate which returns {@code true} if it receives an element
     *  that is not contained in the passed in collection.
     */
    public static <T> IgnitePredicate<T> notContains(@Nullable final Collection<T> c) {
        return c == null || c.isEmpty() ? GridFunc.<T>alwaysTrue() : new P1<T>() {
            @Override public boolean apply(T t) {
                return !c.contains(t);
            }
        };
    }

    /**
     * @param arr Array.
     * @param val Value to find.
     * @return {@code True} if array contains given value.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public static boolean contains(int[] arr, int val) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == val)
                return true;
        }

        return false;
    }

    /**
     * @param arr Array.
     * @param val Value to find.
     * @return {@code True} if array contains given value.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public static boolean contains(long[] arr, long val) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == val)
                return true;
        }

        return false;
    }

    /**
     * @param arr Array.
     * @param val Value to find.
     * @return {@code True} if array contains given value.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public static boolean contains(Integer[] arr, Integer val) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].equals(val))
                return true;
        }

        return false;
    }

    /**
     * Gets utility predicate that accepts {@link java.util.Map.Entry} value and compares
     * its value to the given value.
     *
     * @param val Value to compare entry's value.
     * @param <K> Map key type.
     * @param <V> Map value type.
     * @return Predicate that accepts {@link java.util.Map.Entry} value and compares its value
     *      to the given value.
     */
    public static <K, V> IgnitePredicate<Map.Entry<K, V>> mapValue(@Nullable final V val) {
        return new P1<Map.Entry<K, V>>() {
            @Override public boolean apply(Map.Entry<K, V> e) {
                return e.getValue().equals(val);
            }
        };
    }

    /**
     * Gets utility predicate that accepts {@code java.util.Map.Entry} value and compares its key
     * to the given value.
     *
     * @param key Value to compare entry's key.
     * @param <K> Map key type.
     * @param <V> Map value type.
     * @return Predicate that accepts {@code java.util.Map.Entry} value and compares its key
     *      to the given value.
     */
    public static <K, V> IgnitePredicate<Map.Entry<K, V>> mapKey(@Nullable final K key) {
        return new P1<Map.Entry<K, V>>() {
            @Override public boolean apply(Map.Entry<K, V> e) {
                return e.getKey().equals(key);
            }
        };
    }

    /**
     * Tests whether specified arguments are equal, or both {@code null}.
     *
     * @param o1 Object to compare.
     * @param o2 Object to compare.
     * @return Returns {@code true} if the specified arguments are equal, or both {@code null}.
     */
    public static boolean eq(@Nullable Object o1, @Nullable Object o2) {
        return o1 == null ? o2 == null : o2 != null && (o1 == o2 || o1.equals(o2));
    }

    /**
     * Checks if both collections have equal elements in the same order.
     *
     * @param c1 First collection.
     * @param c2 Second collection.
     * @return {@code True} if both collections have equal elements in the same order.
     */
    public static boolean eqOrdered(@Nullable Collection<?> c1, @Nullable Collection<?> c2) {
        if (c1 == c2)
            return true;

        if (c1 == null || c2 == null)
            return false;

        if (c1.size() != c2.size())
            return false;

        Iterator<?> it1 = c1.iterator();
        Iterator<?> it2 = c2.iterator();

        while (it1.hasNext() && it2.hasNext())
            if (!eq(it1.next(), it2.next()))
                return false;

        return it1.hasNext() == it2.hasNext();
    }

    /**
     * Checks if both collections have equal elements in any order.
     * <p>
     * Implementation of this method takes optimization steps based on the actual type
     * of the collections. Dups are allowed in the collection. Note that if collections
     * allow fixed-time random access (implementing {@link RandomAccess} marker interface
     * - having them pre-sorted dramatically improves the performance.
     *
     * @param c1 First collection.
     * @param c2 Second collection.
     * @return {@code True} if both collections have equal elements in any order.
     */
    public static boolean eqNotOrdered(@Nullable Collection<?> c1, @Nullable Collection<?> c2) {
        if (c1 == c2)
            return true;

        if (c1 == null || c2 == null)
            return false;

        if (c1.size() != c2.size())
            return false;

        // Random index access is fixed time (at least for one), dups are allowed.
        // Optimize to 'n * log(n)' complexity. Pre-sorting of random accessed collection
        // helps HUGE as it will reduce complexity to 'n'.
        if (c1 instanceof RandomAccess || c2 instanceof RandomAccess) {
            Collection<?> col;
            List<?> lst;

            if (c1 instanceof RandomAccess) {
                col = c2;
                lst = (List<?>)c1;
            }
            else {
                col = c1;
                lst = (List<?>)c2;
            }

            int p = 0;

            // Collections are of the same size.
            int size = c1.size();

            for (Object o1 : col) {
                boolean found = false;

                for (int i = p; i < size; i++) {
                    if (F.eq(lst.get(i), o1)) {
                        found = true;

                        if (i == p)
                            p++;

                        break;
                    }
                }

                if (!found)
                    return false;
            }
        }
        else if (c1 instanceof Set && c2 instanceof Set) {
            // Sets don't have dups - we can skip cross checks.
            // Access is fixed-time - can do 'k*n' complexity.
            for (Object o : c1)
                if (!c2.contains(o))
                    return false;
        }
        // Non-sets, non random access and collections of different types
        // will do full cross-checks of '2k*n*n' complexity.
        else {
            // Dups are possible and no fixed-time random access...
            // Have to do '2k*n*n' complexity in worst case.
            for (Object o : c1)
                if (!c2.contains(o))
                    return false;

            for (Object o : c2)
                if (!c1.contains(o))
                    return false;

        }

        return true;
    }

    /**
     * Compares two maps. Unlike {@code java.util.AbstractMap#equals(...)} method this implementation
     * checks not only entry sets, but also the keys. Some optimization checks are also used.
     *
     * @param m1 First map to check.
     * @param m2 Second map to check
     * @return {@code True} is maps are equal, {@code False} otherwise.
     */
    public static <K, V> boolean eqNotOrdered(@Nullable Map<K, V> m1, @Nullable Map<K, V> m2) {
        if (m1 == m2)
            return true;

        if (m1 == null || m2 == null)
            return false;

        if (m1.size() != m2.size())
            return false;

        for (Map.Entry<K, V> e : m1.entrySet()) {
            V v1 = e.getValue();
            V v2 = m2.get(e.getKey());

            if (v1 == v2)
                return true;

            if (v1 == null || v2 == null)
                return false;

            if (v1 instanceof Collection && v2 instanceof Collection) {
                if (!eqNotOrdered((Collection)v1, (Collection)v2))
                    return false;
            }
            else {
                if (v1 instanceof Map && v2 instanceof Map) {
                    if (!eqNotOrdered((Map)v1, (Map)v2))
                        return false;
                }
                else {
                    if (!eq(v1, v2))
                        return false;
                }
            }
        }

        return true;
    }

    /**
     * Compares two arrays. Unlike {@code Arrays#equals(...)} method this implementation
     * checks two arrays as sets allowing the same elements to be in different indexes.
     *
     * @param a1 First array to check.
     * @param a2 Second array to check.
     * @param sorted Tells whether or not both arrays are pre-sorted so that binary
     *      search could be used instead of iteration.
     * @param dups Tells whether or not arrays can contain duplicates. If arrays contain
     *      duplicate the implementation will have to do double work.
     * @return {@code True} if arrays are equal, {@code false} otherwise.
     */
    public static boolean eqArray(Object[] a1, Object[] a2, boolean sorted, boolean dups) {
        if (a1 == a2)
            return true;

        if (a1 == null || a2 == null || a1.length != a2.length)
            return false;

        // Short circuit.
        if (a1.length == 1)
            return eq(a1[0], a2[0]);

        for (Object o1 : a1) {
            boolean found = false;

            if (sorted)
                found = Arrays.binarySearch(a2, o1) >= 0;
            else {
                for (Object o2 : a2) {
                    if (eq(o1, o2)) {
                        found = true;

                        break;
                    }
                }
            }

            if (!found)
                return false;
        }

        // If there are no dups - we can't skip checking seconds array
        // against first one.
        if (dups) {
            for (Object o2 : a2) {
                boolean found = false;

                if (sorted)
                    found = Arrays.binarySearch(a1, o2) >= 0;
                else {
                    for (Object o1 : a1) {
                        if (eq(o2, o1)) {
                            found = true;

                            break;
                        }
                    }
                }

                if (!found)
                    return false;
            }
        }

        return true;
    }

    /**
     * Compares two {@link org.apache.ignite.cluster.ClusterNode} instances for equality.
     * <p>
     * Since introduction of {@link org.apache.ignite.cluster.ClusterNode} in Apache Ignite 3.0 the semantic of equality between
     * grid nodes has changed. Since rich node wraps thin node instance and in the same time
     * implements {@link org.apache.ignite.cluster.ClusterNode} interface, the proper semantic of comparing two grid node is
     * to ignore their runtime types and compare only by their IDs. This method implements this logic.
     * <p>
     * End users rarely, if ever, need to directly compare two grid nodes for equality. This method is
     * intended primarily for discovery SPI developers that provide implementations of {@link org.apache.ignite.cluster.ClusterNode}
     * interface.
     *
     * @param n1 Grid node 1.
     * @param n2 Grid node 2
     * @return {@code true} if two grid node have the same IDs (ignoring their runtime types),
     *      {@code false} otherwise.
     */
    public static boolean eqNodes(Object n1, Object n2) {
        return n1 == n2 || !(n1 == null || n2 == null) && !(!(n1 instanceof ClusterNode) || !(n2 instanceof ClusterNode))
            && ((ClusterNode)n1).id().equals(((ClusterNode)n2).id());
    }

    /**
     * Curries collection of closures with given argument.
     *
     * @param iter Collection to curry.
     * @param arg Argument to curry with.
     * @param <T> Type of closure argument.
     * @param <R> Type of closure return value.
     * @return Read only collection of curried closures.
     */
    public static <T, R> Collection<IgniteOutClosure<R>> curry(Collection<? extends IgniteClosure<? super T, R>> iter,
        final T arg) {
        A.notNull(iter, "iter", arg, "arg");

        return viewReadOnly(iter, new C1<IgniteClosure<? super T, R>, IgniteOutClosure<R>>() {
            @Override public IgniteOutClosure<R> apply(IgniteClosure<? super T, R> c) {
                return curry(c, arg);
            }
        });
    }

    /**
     * Curries collection of closures with given collection of arguments.
     *
     * @param closures Collection to curry.
     * @param args Collection of arguments to curry with.
     * @param <T> Type of closure argument.
     * @param <R> Type of closure return value.
     * @return Collection of curried closures.
     */
    public static <T, R> Collection<IgniteOutClosure<R>> curry(Collection<? extends IgniteClosure<? super T, R>> closures,
        Collection<? extends T> args) {
        A.notNull(closures, "in", args, "args");
        A.ensure(closures.size() == args.size(), "closures.size() == args.size()");

        Collection<IgniteOutClosure<R>> ret = new ArrayList<>(closures.size());

        Iterator<? extends T> iter = args.iterator();

        for (IgniteClosure<? super T, R> c : closures) {
            ret.add(curry(c, iter.next()));
        }

        return ret;
    }

    /**
     * Curries collection of in closures with given collection of arguments. Note that name is
     * different due to type erasure and subsequent name conflict with other {@code curry} methods.
     *
     * @param in Collection to curry.
     * @param args Collection of arguments to curry with.
     * @param <T> Type of closure argument.
     * @return Collection of curried closures.
     */
    public static <T> Collection<GridAbsClosure> curry0(Collection<? extends IgniteInClosure<? super T>> in,
        Collection<? extends T> args) {
        A.notNull(in, "in", args, "args");
        A.ensure(in.size() == args.size(), "in.size() == args.size()");

        Collection<GridAbsClosure> ret = new ArrayList<>(in.size());

        Iterator<? extends T> iter = args.iterator();

        for (IgniteInClosure<? super T> c : in) {
            ret.add(curry(c, iter.next()));
        }

        return ret;
    }

    /**
     * Curries collection of closures with given collection of arguments.
     *
     * @param c Closure to curry.
     * @param args Collection of arguments to curry with.
     * @param <T> Type of closure argument.
     * @param <R> Type of closure return value.
     * @return Collection of curried closures.
     */
    public static <T, R> Collection<IgniteOutClosure<R>> curry(IgniteClosure<? super T, R> c,
        Collection<? extends T> args) {
        A.notNull(c, "c", args, "args");

        Collection<IgniteOutClosure<R>> ret = new ArrayList<>(args.size());

        for (T arg : args) {
            ret.add(curry(c, arg));
        }

        return ret;
    }

    /**
     * Curries collection of closures with given collection of arguments.
     *
     * @param cnt Number of closures to produce.
     * @param c Closure to curry.
     * @param pdc Producer of arguments to curry with.
     * @param <T> Type of closure argument.
     * @param <R> Type of closure return value.
     * @return Collection of curried closures.
     */
    public static <T, R> Collection<IgniteOutClosure<R>> curry(int cnt, IgniteClosure<? super T, R> c,
        IgniteOutClosure<T> pdc) {
        A.notNull(c, "c", pdc, "pdc");
        A.ensure(cnt > 0, "cnt > 0");

        Collection<IgniteOutClosure<R>> ret = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            ret.add(curry(c, pdc.apply()));

        return ret;
    }

    /**
     * Curries collection of closures with given collection of arguments.
     *
     * @param c Closure to curry.
     * @param args Collection of arguments to curry with.
     * @param <T> Type of closure argument.
     * @return Collection of curried closures.
     */
    public static <T> Collection<GridAbsClosure> curry(IgniteInClosure<? super T> c, Collection<? extends T> args) {
        A.notNull(c, "c", args, "args");

        Collection<GridAbsClosure> ret = new ArrayList<>(args.size());

        for (T arg : args) {
            ret.add(curry(c, arg));
        }

        return ret;
    }

    /**
     * Curries collection of closures with given collection of arguments.
     *
     * @param cnt Number of closures to produce.
     * @param c Closure to curry.
     * @param pdc Producer of arguments to curry with.
     * @param <T> Type of closure argument.
     * @return Collection of curried closures.
     */
    public static <T> Collection<GridAbsClosure> curry(int cnt, IgniteInClosure<? super T> c, IgniteOutClosure<T> pdc) {
        A.notNull(c, "c", pdc, "pdc");
        A.ensure(cnt > 0, "cnt > 0");

        Collection<GridAbsClosure> ret = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            ret.add(curry(c, pdc.apply()));

        return ret;
    }

    /**
     * Gets closure that returns key for an entry. The closure internally
     * delegates to {@link java.util.Map.Entry#getKey()} method.
     *
     * @param <K> Key type.
     * @return Closure that returns key for an entry.
     */
    @SuppressWarnings({"unchecked"})
    public static <K> IgniteClosure<Map.Entry<K, ?>, K> mapEntry2Key() {
        return (IgniteClosure<Map.Entry<K, ?>, K>)MAP_ENTRY_KEY;
    }

    /**
     * Gets closure that returns key for cache entry. The closure internally
     * delegates to {@link javax.cache.Cache.Entry#getKey()} method.
     *
     * @param <K> Key type.
     * @return Closure that returns key for an entry.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<Cache.Entry<K, V>, K> cacheEntry2Key() {
        return (IgniteClosure<Cache.Entry<K, V>, K>)CACHE_ENTRY_KEY;
    }

    /**
     * Gets closure that returns value for an entry. The closure internally
     * delegates to {@link javax.cache.Cache.Entry#getValue()} method.
     *
     * @param <V> Value type.
     * @return Closure that returns key for an entry.
     */
    @SuppressWarnings({"unchecked"})
    public static <V> IgniteClosure<Map.Entry<?, V>, V> mapEntry2Value() {
        return (IgniteClosure<Map.Entry<?, V>, V>)MAP_ENTRY_VAL;
    }

    /**
     * Gets closure that returns value for an entry. The closure internally
     * delegates to {@link javax.cache.Cache.Entry#get(Object)} method.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Closure that returns value for an entry.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgniteClosure<Cache.Entry<K, V>, V> cacheEntry2Get() {
        return (IgniteClosure<Cache.Entry<K, V>, V>)CACHE_ENTRY_VAL_GET;
    }

    /**
     * Gets predicate which returns {@code true} if entry's key is contained in given collection.
     * Note that if collection of provided keys is empty this method returns predicate that
     * evaluates to {@code false} when applying.
     *
     * @param keys Keys to check.
     * @param <K> Cache key type.
     * @param <V> Cache value type.
     * @return Predicate which returns {@code true} if entry's key is contained in given collection.
     */
    public static <K, V> IgnitePredicate<Cache.Entry<K, V>> cacheHasKeys(
        @Nullable final Collection<? extends K> keys) {
        return isEmpty(keys) ? F.<Cache.Entry<K, V>>alwaysFalse() :
            new IgnitePredicate<Cache.Entry<K, V>>() {
                @Override public boolean apply(Cache.Entry<K, V> e) {
                    return keys != null && keys.contains(e.getKey());
                }
            };
    }

    /**
     * Gets predicate which returns {@code true} if entry has peek value.
     *
     * @param <K> Cache key type.
     * @param <V> Cache value type.
     * @return Predicate which returns {@code true} if entry has peek value.
     */
    @SuppressWarnings({"unchecked"})
    public static <K, V> IgnitePredicate<Cache.Entry<K, V>> cacheHasPeekValue() {
        return (IgnitePredicate<Cache.Entry<K, V>>)CACHE_ENTRY_HAS_PEEK_VAL;
    }

    /**
     * Gets event predicate that returns {@code true} only if node that produced the event
     * satisfies all given predicates. Note that if array of provided node predicates is
     * {@code null} or empty this method returns predicate that evaluates to {@code true}
     * assuming that any event produced by any node is ok.
     *
     * @param gridName Grid name.
     * @param p Node predicates.
     * @return Event predicate.
     */
    public static IgnitePredicate<Event> eventNode(@Nullable final String gridName,
        @Nullable final IgnitePredicate<ClusterNode>... p) {
        return isEmpty(p) || isAlwaysTrue(p) ? F.<Event>alwaysTrue() : isAlwaysFalse(p) ? F.<Event>alwaysFalse() :
            new IgnitePredicate<Event>() {
                @Override public boolean apply(Event e) {
                    assert e != null;

                    try {
                        ClusterNode node = G.ignite(gridName).cluster().node(e.node().id());

                        return isAll(node, p);
                    }
                    catch (IllegalStateException ex) {
                        throw new IgniteException("Invalid grid name: " + gridName, ex);
                    }
                }
            };
    }

    /**
     * Gets event predicate that returns {@code true} only if node that produced the event
     * is one of the given. Note that if array of provided nodes is {@code null} or empty
     * this method returns predicate that evaluates to {@code false} when applying.
     *
     * @param nodes Nodes.
     * @return Event predicate.
     */
    public static IgnitePredicate<Event> eventNode(@Nullable final Collection<? extends ClusterNode> nodes) {
        return isEmpty(nodes) ? F.<Event>alwaysFalse() : new IgnitePredicate<Event>() {
            @Override public boolean apply(Event e) {
                assert e != null;

                return !forAll(nodes, not(nodeForNodeId(e.node().id())));
            }
        };
    }

    /**
     * Shortcut method that creates an instance of {@link GridClosureException}.
     *
     * @param e Exception to wrap.
     * @return Newly created instance of {@link GridClosureException}.
     */
    public static GridClosureException wrap(Throwable e) {
        return new GridClosureException(e);
    }

    /**
     * Checks if two collections passed in intersect.
     *
     * @param <E> Element type.
     * @param s1 Set1.
     * @param s2 Set2.
     * @return {@code True} if there is an intersection, {@code false} otherwise.
     */
    public static <E> boolean intersects(Iterable<E> s1, Collection<E>... s2) {
        for (E e1 : s1) {
            for (Collection<E> s : s2) {
                if (s.contains(e1))
                    return true;
            }
        }

        return false;
    }

    /**
     * Waits until all passed futures will be executed.
     *
     * @param futs Futures. If none provided - this method is no-op.
     * @throws IgniteCheckedException If any of the futures failed.
     */
    public static <T> void awaitAll(@Nullable IgniteInternalFuture<T>... futs) throws IgniteCheckedException {
        if (!isEmpty(futs))
            awaitAll(asList(futs));
    }

    /**
     * Waits until all passed futures will be executed.
     *
     * @param futs Futures. If none provided - this method is no-op.
     * @throws IgniteCheckedException If any of the futures failed.
     */
    public static <T> void awaitAll(@Nullable Collection<IgniteInternalFuture<T>> futs) throws IgniteCheckedException {
        awaitAll(0, null, futs);
    }

    /**
     * Waits until all passed futures will be executed.
     *
     * @param timeout Timeout for waiting ({@code 0} for forever).
     * @param futs Futures. If none provided - this method is no-op.
     * @throws IgniteCheckedException If any of the futures failed.
     */
    public static <T> void awaitAll(long timeout, @Nullable Collection<IgniteInternalFuture<T>> futs) throws IgniteCheckedException {
        awaitAll(timeout, null, futs);
    }

    /**
     * Awaits for all futures to complete and optionally reduces all results into one.
     *
     * @param timeout Timeout for waiting ({@code 0} for forever).
     * @param rdc Optional reducer. If not {@code null}, then results will be reduced into one.
     * @param futs List of futures to wait for.
     * @param <T> Return type of the futures.
     * @param <R> Return type of the reducer.
     * @return Reduced result if reducer is provided, {@code null} otherwise.
     * @throws IgniteCheckedException If any of the futures failed.
     */
    @Nullable public static <T, R> R awaitAll(long timeout, @Nullable IgniteReducer<T, R> rdc,
        @Nullable Collection<IgniteInternalFuture<T>> futs) throws IgniteCheckedException {
        if (futs == null || futs.isEmpty())
            return null;

        long end = timeout == 0 ? Long.MAX_VALUE : U.currentTimeMillis() + timeout;

        // Overflow.
        if (end < 0)
            end = Long.MAX_VALUE;

        // Note that it is important to wait in the natural order of collection and
        // not via listen method, because caller may actually add to this collection
        // concurrently while this method is in progress.
        for (IgniteInternalFuture<T> fut : futs) {
            T t;

            if (timeout > 0) {
                long left = end - U.currentTimeMillis();

                if (left <= 0 && !fut.isDone())
                    throw new IgniteFutureTimeoutCheckedException("Timed out waiting for all futures: " + futs);

                if (fut.isDone() && left < 0)
                    left = 0;

                t = fut.get(left);
            }
            else
                t = fut.get();

            if (rdc != null)
                rdc.collect(t);
        }

        return rdc == null ? null : rdc.reduce();
    }

    /**
     * Waits for one completed future from passed and returns it.
     *
     * @param futs Futures. If none provided - this method return completed future
     *      with {@code null} value.
     * @param <T> Type of computation result.
     * @return Completed future.
     */
    public static <T> IgniteInternalFuture<T> awaitOne(IgniteInternalFuture<T>... futs) {
        return isEmpty(futs) ? new GridFinishedFuture<T>() : awaitOne(asList(futs));
    }

    /**
     * Waits for the first completed future from passed and returns it.
     *
     * @param futs Futures.
     * @param <T> Type of computation result.
     * @return Completed future.
     */
    public static <T> IgniteInternalFuture<T> awaitOne(Iterable<IgniteInternalFuture<T>> futs) {
        if (F.isEmpty(futs))
            return new GridFinishedFuture<>();

        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicReference<IgniteInternalFuture<T>> t = new AtomicReference<>();

        IgniteInClosure<IgniteInternalFuture<T>> c = null;

        for (IgniteInternalFuture<T> fut : futs) {
            if (fut != null) {
                if (!fut.isDone()) {
                    if (c == null) {
                        c = new CI1<IgniteInternalFuture<T>>() {
                            @Override public void apply(IgniteInternalFuture<T> fut) {
                                if (t.compareAndSet(null, fut))
                                    latch.countDown();
                            }
                        };
                    }

                    fut.listen(c);
                }
                else
                    return fut;
            }
        }

        // Only NULLs have been passed in.
        if (c == null)
            return new GridFinishedFuture<>();

        boolean interrupted = false;

        while (latch.getCount() > 0) {
            try {
                latch.await();
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        IgniteInternalFuture<T> f = t.get();

        assert f != null;

        return f;
    }

    /**
     * Returns predicate for filtering unfinished futures.
     *
     * @return Predicate for filtering unfinished futures.
     */
    public static IgnitePredicate<IgniteInternalFuture<?>> unfinishedFutures() {
        return UNFINISHED_FUTURE;
    }
}
