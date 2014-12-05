/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Tests for {@link GridFunc} class.
 */
@GridCommonTest(group = "Lang")
public class GridFuncSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String[] EMPTY_STR = new String[0];

    /**
     *  Creates test.
     */
    public GridFuncSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * JUnit.
     */
    public void testEqNonOrdered() {
        assert F.eqNotOrdered(Arrays.asList(1, 2, 3, 4), Arrays.asList(1, 2, 3, 4));
        assert !F.eqNotOrdered(Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(1, 2, 3, 4));
        assert !F.eqNotOrdered(Arrays.asList(1, 2, 3, 4), Arrays.asList(1, 2, 6, 4));

        assert !F.eqNotOrdered(Arrays.asList(1, 2, 3, 4), F.<Integer>asSet(1, 2, 6, 4));
        assert F.eqNotOrdered(Arrays.asList(1, 2, 3, 4), F.<Integer>asSet(1, 2, 3, 4));
        assert F.eqNotOrdered(F.<Integer>asSet(1, 2, 3, 4), F.<Integer>asSet(1, 2, 3, 4));
        assert !F.eqNotOrdered(F.<Integer>asSet(1, 2, 3, 4), F.<Integer>asSet(1, 2, 4));
        assert !F.eqNotOrdered(F.<Integer>asSet(1, 2, 3, 4), F.<Integer>asSet(1, 2, 3, 6));

        Collection<Integer> lnkLst = new LinkedList<>(Arrays.asList(1, 2, 3, 4));

        assert F.eqNotOrdered(Arrays.asList(1, 2, 3, 4), lnkLst);
        assert !F.eqNotOrdered(Arrays.asList(1, 3, 3, 4), lnkLst);
        assert !F.eqNotOrdered(Arrays.asList(2, 3, 4), lnkLst);
        assert F.eqNotOrdered(F.<Integer>asSet(1, 2, 3, 4), lnkLst);
        assert !F.eqNotOrdered(F.<Integer>asSet(1, 2, 5, 4), lnkLst);
        assert !F.eqNotOrdered(F.<Integer>asSet(1, 4), lnkLst);

        assert !F.eqNotOrdered(Arrays.asList(1, 2, 2, 3, 4), F.<Integer>asSet(1, 2, 3, 4));
        assert !F.eqNotOrdered(Arrays.asList(1, 2, 3, 4, 4), Arrays.asList(1, 2, 3, 4, 5));
    }

    /**
     * JUnit.
     */
    public void testContainsAny() {
        assert !F.containsAny(null, (String[])null);

        Collection<String> c1 = new ArrayList<>();

        assert !F.containsAny(c1, (String[])null);

        c1.add("a");
        c1.add("b");
        c1.add("c");
        c1.add("d");

        assert !F.containsAny(c1, (String[])null);

        Collection<String> c2 = new ArrayList<>();

        assert !F.containsAny(c1, c2);
        assert !F.containsAny(null, c2);
        assert !F.containsAny(c1, (String[])null);

        c2.add("b");
        c2.add("e");

        assert F.containsAny(c1, c2);

        c2.remove("b");

        assert !F.containsAny(c1, c2);
    }

    /**
     * JUnit.
     */
    public void testContainsAll() {
        assert !F.containsAll(null, null);

        Collection<String> c1 = new ArrayList<>();

        assert F.containsAll(c1, null);

        c1.add("a");
        c1.add("b");
        c1.add("c");
        c1.add("d");

        assert F.containsAll(c1, null);

        Collection<String> c2 = new ArrayList<>();

        assert F.containsAll(c1, c2);

        c2.add("b");
        c2.add("e");

        assert !F.containsAll(c1, c2);

        c2.remove("b");

        assert !F.containsAll(c1, c2);

        assert F.containsAll(c1, c1);
        assert F.containsAll(c2, c2);
        assert F.containsAll(c1, null);
    }

    /**
     * JUnit.
     */
    public void testFlat0() {
        Collection<String> c1 = new ArrayList<>();

        c1.add("test1");
        c1.add("test2");

        Integer[] c2 = new Integer[] {1, 2, 3};

        Collection<Object> flat = F.flat0(c1, c2, "some", 4);

        assert flat.size() == 7;
    }

    /**
     * JUnit.
     */
    public void testOptimizedAndOr() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();
        UUID id4 = UUID.randomUUID();

        GridNodePredicate p1 = new GridNodePredicate(id1, id2);
        GridNodePredicate p2 = new GridNodePredicate(id3, id4);

        IgnitePredicate<ClusterNode> and = F.<ClusterNode>and(p1, p2);
        IgnitePredicate<ClusterNode> or = F.<ClusterNode>or(p1, p2);

        assert and instanceof GridNodePredicate;
        assert or instanceof GridNodePredicate;

        GridNodePredicate andP = (GridNodePredicate)and;
        GridNodePredicate orP = (GridNodePredicate)or;

        assert F.isEmpty(andP.nodeIds());

        assert !F.isEmpty(orP.nodeIds());
        assert orP.nodeIds().size() == 4;
    }

    /**
     * JUnit.
     */
    public void testFlatCollection() {
        Map<Integer, List<Integer>> m = new HashMap<>();

        m.put(1, Arrays.asList(0, 1, 2, 3));
        m.put(2, Arrays.asList(4, 5, 6, 7));

        final List<Integer> r = new ArrayList<>();

        Collection<Integer> flat = F.flatCollections(m.values());

        F.forEach(flat, new IgniteInClosure<Integer>() {
            @Override public void apply(Integer i) {
                r.add(i);
            }
        });

        assert r.size() == 8;

        for (int i = 0; i < 8; i++)
            assert r.get(i) == i;
    }

    /**
     * JUnit.
     */
    public void testFlatIterable() {
        Iterable<List<String>> l = F.asList(F.asList("1", "2"), F.asList("3", "4", "5"), F.asList("6", "7"));

        GridIterator<String> iter = F.flat(l);

        List<String> checkList = F.asList("1", "2", "3", "4", "5", "6", "7");

        int cnt = 0;

        while (iter.hasNext()) {
            String s = iter.next();

            assertEquals(checkList.get(cnt), s);

            cnt++;
        }

        assertEquals(checkList.size(), cnt);

        l = F.asList(Collections.<String>emptyList());

        iter = F.flat(l);

        assert !iter.hasNext();

        cnt = 0;

        for (; iter.hasNext(); iter.next())
            cnt++;

        assertEquals(0, cnt);
    }

    /**
     * JUnit.
     */
    public void testSum() {
        Collection<Integer> c1 = Arrays.asList(1, 2, 3, 4, 5);

        assert F.sumInt(c1) == 15;
    }

    /**
     * JUnit.
     */
    public void testLose() {
        // With predicates (copy).
        Collection<Integer> input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        Collection<Integer> res = F.lose(input, true, new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        });

        assertEquals(3, res.size());

        assert res.containsAll(Arrays.asList(1, 3, 5));

        assertEquals(5, input.size());

        assert input.containsAll(Arrays.asList(1, 2, 3, 4, 5));

        // With predicates (without copy).
        res = F.lose(input, false, new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        });

        assertEquals(3, res.size());

        assert res.containsAll(Arrays.asList(1, 3, 5));

        assert input == res;

        P1<Integer> nullIntPred = null;

        // Null predicates (with copy).
        assertTrue(F.lose(res, true, nullIntPred).isEmpty());
        assertTrue(F.lose(res, true, (P1<Integer>[])null).isEmpty());
        assertEquals(res, F.lose(res, true, F.alwaysFalse(), nullIntPred));

        // Null predicates (without copy).
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));
        assertTrue(F.lose(input, false, nullIntPred).isEmpty());

        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));
        assertTrue(F.lose(input, false, (P1<Integer>[])null).isEmpty());

        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));
        assertEquals(input, F.lose(input, false, F.alwaysFalse(), nullIntPred));

        // With filter collection (copy).
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.lose(input, true, Arrays.asList(2, 4));

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(3);
        assert res.contains(5);

        assertEquals(5, input.size());

        // With filter collection (without copy).
        res = F.lose(input, false, Arrays.asList(2, 4));

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(3);
        assert res.contains(5);

        assert input == res;

        // With number of elements to lose (copy).
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.lose(input, true, 3);

        assertEquals(2, res.size());
        assert res.contains(4);
        assert res.contains(5);

        assertEquals(5, input.size());

        // With number of elements to lose (copy).
        res = F.lose(input, true, 3);

        assert res.size() == 2;
        assert res.contains(4);
        assert res.contains(5);

        // With number of elements to lose (copy).
        res = F.lose(input, true, 10);

        assert res.isEmpty();

        // With number of elements to lose (without copy).
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.lose(input, false, 3);

        assertEquals(2, res.size());
        assert res.contains(4);
        assert res.contains(5);

        assert input == res;

        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.lose(input, false, 10);

        assert res.isEmpty();
        assert input.isEmpty();

        // Map with entry predicates (copy).
        Map<Integer, String> map = F.asMap(1, "1", 2, "2", 3, "3");

        Map<Integer, String> resMap = F.lose(map, true,
            new P1<Map.Entry<Integer, String>>() {
                @Override public boolean apply(Map.Entry<Integer, String> entry) {
                    return entry.getKey() == 2 && "2".equals(entry.getValue());
                }
            });

        assert !resMap.isEmpty();
        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        assertEquals(3, map.size());

        P1<Map.Entry<Integer, String>> nullMapEntryPred = null;

        // Map with null predicates (with copy).
        assertTrue(F.lose(map, true, nullMapEntryPred).isEmpty());
        assertTrue(F.lose(map, true, (P1<Map.Entry<Integer, String>>[])null).isEmpty());
        assertEquals(map, F.lose(map, true, F.alwaysFalse(), nullMapEntryPred));

        // Map with null predicates (without copy).
        map = F.asMap(1, "1", 2, "2", 3, "3");
        assertTrue(F.lose(map, false, nullMapEntryPred).isEmpty());

        map = F.asMap(1, "1", 2, "2", 3, "3");
        assertTrue(F.lose(map, false, (P1<Map.Entry<Integer, String>>[])null).isEmpty());

        map = F.asMap(1, "1", 2, "2", 3, "3");
        assertEquals(map, F.lose(map, false, F.alwaysFalse(), nullMapEntryPred));

        // Map with entry predicates (without copy).
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.lose(map, false,
            new P1<Map.Entry<Integer, String>>() {
                @Override public boolean apply(Map.Entry<Integer, String> entry) {
                    return entry.getKey() == 2 && "2".equals(entry.getValue());
                }
            });

        assert !resMap.isEmpty();
        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        assert map == resMap;

        // Map with key predicates (copy).
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.loseKeys(map, true, new P1<Integer>() {
            @Override public boolean apply(Integer i) {
                return i == 2;
            }
        });

        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        // Map with key predicates (without copy).
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.loseKeys(map, false, new P1<Integer>() {
            @Override public boolean apply(Integer i) {
                return i == 2;
            }
        });

        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        assert map == resMap;

        // Map with value predicates (copy).
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.loseValues(map, true, new P1<String>() {
            @Override public boolean apply(String s) {
                return "2".equals(s);
            }
        });

        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        // Map with value predicates (without copy).
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.loseValues(map, false, new P1<String>() {
            @Override public boolean apply(String s) {
                return "2".equals(s);
            }
        });

        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        assert map == resMap;

        P1<String> nullStrPred = null;

        // Map with value null predicates (with copy).
        assertTrue(F.loseValues(map, true, nullStrPred).isEmpty());
        assertTrue(F.loseValues(map, true, (P1<String>[])null).isEmpty());
        assertEquals(map, F.loseValues(map, true, F.alwaysFalse(), nullStrPred));

        // Map with value null predicates (without copy).
        map = F.asMap(1, "1", 2, "2", 3, "3");
        assertTrue(F.loseValues(map, false, nullStrPred).isEmpty());

        map = F.asMap(1, "1", 2, "2", 3, "3");
        assertTrue(F.loseValues(map, false, (P1<String>[])null).isEmpty());

        map = F.asMap(1, "1", 2, "2", 3, "3");
        assertEquals(map, F.loseValues(map, false, F.alwaysFalse(), nullStrPred));
    }

    /**
     * JUnit.
     */
    public void testLoseList() {
        // Copy.
        List<Integer> input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        List<Integer> res = F.loseList(input, true, Arrays.asList(2, 4));

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(3);
        assert res.contains(5);

        assertEquals(5, input.size());

        res = F.loseList(input, true, null);

        assertEquals(5, input.size());

        assert input.equals(res);

        res = F.loseList(input, true, Collections.emptyList());

        assertEquals(5, input.size());

        assert input.equals(res);

        // Without copy.
        res = F.loseList(input, false, Arrays.asList(2, 4));

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(3);
        assert res.contains(5);

        assert input == res;

        res = F.loseList(input, true, null);

        assertEquals(3, input.size());

        assert input.equals(res);

        res = F.loseList(input, true, Collections.emptyList());

        assertEquals(3, input.size());

        assert input.equals(res);

    }

    /**
     * JUnit.
     */
    public void testLoseSet() {
        // Copy.
        Set<Integer> input = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));

        Set<Integer> res = F.loseSet(input, true, Arrays.asList(2, 4));

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(3);
        assert res.contains(5);

        assertEquals(5, input.size());

        res = F.loseSet(input, true, null);

        assertEquals(5, input.size());

        assert input.equals(res);

        res = F.loseSet(input, true, Collections.emptyList());

        assertEquals(5, input.size());

        assert input.equals(res);

        // Without copy.
        res = F.loseSet(input, false, Arrays.asList(2, 4));

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(3);
        assert res.contains(5);

        assert input == res;

        res = F.loseSet(input, true, null);

        assertEquals(3, input.size());

        assert input.equals(res);

        res = F.loseSet(input, true, Collections.emptyList());

        assertEquals(3, input.size());

        assert input.equals(res);
    }

    /**
     * JUnit.
     */
    public void testRetain() {
        // Copy.
        Collection<Integer> input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        Collection<Integer> res = F.retain(input, true, new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        });

        assertEquals(2, res.size());
        assert res.contains(2);
        assert res.contains(4);

        assertEquals(5, input.size());

        // Without copy.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.retain(input, false, new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        });

        assertEquals(2, res.size());
        assert res.contains(2);
        assert res.contains(4);

        assert input == res;

        // Null predicate.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        P1<Integer> nullIntPred = null;

        assertEquals(input, F.retain(input, true, nullIntPred));
        assertEquals(input, F.retain(input, true, (P1<Integer>[])null));
        assertTrue(F.retain(input, true, F.alwaysFalse(), nullIntPred).isEmpty());

        // Copy.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.retain(input, true, Arrays.asList(2, 4));

        assertEquals(2, res.size());
        assert res.contains(2);
        assert res.contains(4);

        // Without copy.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.retain(input, false, Arrays.asList(2, 4));

        assertEquals(2, res.size());
        assert res.contains(2);
        assert res.contains(4);

        assert input == res;

        // Copy.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.retain(input, true, 3);

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(2);
        assert res.contains(3);

        // Without copy.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.retain(input, false, 3);

        assertEquals(3, res.size());
        assert res.contains(1);
        assert res.contains(2);
        assert res.contains(3);

        assert input == res;

        // Copy.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.retain(input, true, 10);

        assertEquals(5, res.size());

        // Without copy.
        input = new LinkedList<>(Arrays.asList(1, 2, 3, 4, 5));

        res = F.retain(input, false, 10);

        assertEquals(5, res.size());

        assert input == res;

        // Copy.
        Map<Integer, String> map = F.asMap(1, "1", 2, "2", 3, "3");

        Map<Integer, String> resMap = F.retain(map, true, new P1<Map.Entry<Integer, String>>() {
            @Override public boolean apply(Map.Entry<Integer, String> e) {
                return e.getKey() != 2;
            }
        });

        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        // Without copy.
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.retain(map, false, new P1<Map.Entry<Integer, String>>() {
            @Override public boolean apply(Map.Entry<Integer, String> e) {
                return e.getKey() != 2;
            }
        });

        assertEquals(2, resMap.size());
        assert resMap.containsKey(1);
        assert resMap.containsKey(3);

        assert map == resMap;

        // Null predicate.
        P1<Map.Entry<Integer, String>> nullMapPred = null;

        assertEquals(map, F.retain(map, true, nullMapPred));
        assertEquals(map, F.retain(map, true, (P1<Map.Entry<Integer, String>>[])null));
        assertTrue(F.retain(map, true, F.alwaysFalse(), nullMapPred).isEmpty());

        // Copy.
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.retainKeys(map, true, new P1<Integer>() {
            @Override public boolean apply(Integer i) {
                return i == 2;
            }
        });

        assertEquals(1, resMap.size());
        assert resMap.containsKey(2);

        // Without copy.
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.retainKeys(map, false, new P1<Integer>() {
            @Override public boolean apply(Integer i) {
                return i == 2;
            }
        });

        assertEquals(1, resMap.size());
        assert resMap.containsKey(2);

        assert map == resMap;

        // Null predicate.
        assertEquals(map, F.retainKeys(map, true, nullIntPred));
        assertEquals(map, F.retainKeys(map, true, (P1<Integer>[])null));
        assertTrue(F.retainKeys(map, true, F.alwaysFalse(), nullIntPred).isEmpty());

        // Copy.
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.retainValues(map, true, new P1<String>() {
            @Override public boolean apply(String s) {
                return "2".equals(s);
            }
        });

        assertEquals(1, resMap.size());
        assert resMap.containsKey(2);

        // Without copy.
        map = F.asMap(1, "1", 2, "2", 3, "3");

        resMap = F.retainValues(map, false, new P1<String>() {
            @Override public boolean apply(String s) {
                return "2".equals(s);
            }
        });

        assertEquals(1, resMap.size());
        assert resMap.containsKey(2);

        assert map == resMap;

        // Null predicate.
        P1<String> nullStrPred = null;

        assertEquals(map, F.retainValues(map, true, nullStrPred));
        assertEquals(map, F.retainValues(map, true, (P1<String>[])null));
        assertTrue(F.retainValues(map, true, F.alwaysFalse(), nullStrPred).isEmpty());
    }

    /**
     * JUnit.
     */
    public void testReflectiveClosures() {
        IgniteClosure<String, Integer> c = F.cInvoke("length");

        try {
            assert c.apply("grid") == 4;
            assert c.apply("") == 0;
            assert c.apply("1") == 1;
            assert c.apply("2") == 1;
        }
        catch (GridClosureException e) {
            assert false : e.getMessage();
        }

        c = F.cInvoke("indexOf", "xx");

        try {
            assert c.apply("grid") == -1;
            assert c.apply("") == -1;
            assert c.apply("xx1") == 0;
            assert c.apply("2xx") == 1;
        }
        catch (GridClosureException e) {
            assert false : e.getMessage();
        }

        String s = "gridgain functional programming";

        IgniteOutClosure<Boolean> co = F.coInvoke(s, "contains", "prog");

        try {
            assert co.apply();
        }
        catch (GridClosureException e) {
            e.printStackTrace();

            assert false;
        }

        Collection<String> strs = new ArrayList<>();

        strs.add("GridGain");
        strs.add("Cloud");
        strs.add("Compute");
        strs.add("Data");
        strs.add("Functional");
        strs.add("Programming");

        System.out.println("Strings:");
        F.forEach(strs, F.println());

        Collection<Integer> lens = F.transform(strs, F.<String, Integer>cInvoke("length"));

        System.out.println("\nLengths:");
        F.forEach(lens, F.println("", " characters."));
    }

    /**
     * JUnit.
     */
    public void testYield() {
        Collection<String> iter = Arrays.asList("One", "Two", "Three");

        int len1 = 0;

        for (String s : iter) {
            len1 += s.length();
        }

        Collection<IgniteOutClosure<Integer>> c = F.yield(iter, new C1<String, Integer>() {
            @Override public Integer apply(String e) { return e.length(); }
        });

        int len2 = 0;

        for (IgniteOutClosure<Integer> f : c) {
            len2 += f.apply();
        }

        assert len1 == len2;
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testAlwaysTrue() {
        assert F.alwaysTrue() != null;
        assert F.alwaysTrue().apply(new Object()) == Boolean.TRUE;
        assert F.alwaysTrue().apply(null) == Boolean.TRUE;
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testAlwaysFalse() {
        assert F.alwaysFalse() != null;
        assert F.alwaysFalse().apply(new Object()) == Boolean.FALSE;
        assert F.alwaysFalse().apply(null) == Boolean.FALSE;
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testIsNull() {
        assert F.isNull() != null;
        assert !F.isNull().apply(new Object());
        assert F.isNull().apply(null);
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testNotNull() {
        assert F.notNull() != null;
        assert F.notNull().apply(new Object());
        assert !F.notNull().apply(null);
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testNot() {
        IgnitePredicate<Object> p1 = F.not(F.alwaysFalse());

        assert F.alwaysTrue() == p1;

        assert p1 != null;
        assert p1.apply(new Object());
        assert p1.apply(null);

        p1 = F.not(F.alwaysTrue());

        assert F.alwaysFalse() == p1;

        assert p1 != null;
        assert !p1.apply(new Object());
        assert !p1.apply(null);
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testEqualTo() {
        IgnitePredicate<Object> p1 = F.equalTo(null);

        assert p1 != null;
        assert !p1.apply(new Object());
        assert p1.apply(null);

        IgnitePredicate<String> p2 = F.equalTo("test");

        assert p2 != null;
        assert !p2.apply("test1");
        assert !p2.apply(null);
        assert p2.apply("test");
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testNotEqualTo() {
        IgnitePredicate<Object> p1 = F.notEqualTo(null);

        assert p1 != null;
        assert p1.apply(new Object());
        assert !p1.apply(null);

        IgnitePredicate<String> p2 = F.notEqualTo("test");

        assert p2 != null;
        assert p2.apply("test1");
        assert p2.apply(null);
        assert !p2.apply("test");
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testInstanceOf() {
        IgnitePredicate<Object> p1 = F.instanceOf(Object.class);

        assert p1 != null;
        assert p1.apply(this);
        assert !p1.apply(null);

        IgnitePredicate<Object> p2 = F.instanceOf(TestCase.class);

        assert p2 != null;
        assert p2.apply(this);
        assert !p2.apply(new Object());
        assert !p2.apply(null);
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testNotInstanceOf() {
        IgnitePredicate<Object> p1 = F.notInstanceOf(Object.class);

        assert p1 != null;
        assert !p1.apply(this);
        assert p1.apply(null);

        IgnitePredicate<Object> p2 = F.notInstanceOf(TestCase.class);

        assert p2 != null;
        assert !p2.apply(this);
        assert p2.apply(new Object());
        assert p2.apply(null);
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"unchecked", "NullableProblems", "NullArgumentToVariableArgMethod"})
    public void testAnd() {
        Collection<IgnitePredicate<Object>> ps = Arrays.asList(F.alwaysTrue(), F.alwaysTrue());

        IgnitePredicate<Object> p = F.and(ps);

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        p = F.and(ps.toArray(new IgnitePredicate[2]), ps.toArray(new IgnitePredicate[2]));

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        p = F.and(ps.toArray(new IgnitePredicate[2]), F.alwaysTrue(), F.alwaysTrue());

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        p = F.and(F.alwaysTrue(), F.alwaysTrue(), F.alwaysTrue());

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        ps = Arrays.asList(F.alwaysFalse(), F.alwaysFalse());

        p = F.and(ps);

        assert p != null;
        assert !p.apply(new Object());
        assert !p.apply(null);

        p = F.and(F.alwaysTrue(), F.alwaysTrue(), F.alwaysFalse());

        assert p != null;
        assert !p.apply(new Object());
        assert !p.apply(null);

        Collection<? extends IgnitePredicate<String>> ps2 =
            Arrays.asList(F.<String>instanceOf(String.class), F.<String>equalTo("test"));

        IgnitePredicate<String> pred2 = F.and(ps2);

        assert pred2 != null;
        assert pred2.apply("test");
        assert !pred2.apply("test1");

        assert F.and(F.alwaysFalse()) == F.alwaysFalse();
        assert F.and(F.alwaysTrue()) == F.alwaysTrue();

        assertTrue(F.and(null, null).apply(""));
        assertTrue(F.and((IgnitePredicate<? super Object>[])null, F.alwaysTrue()).apply(""));
        assertFalse(F.and((IgnitePredicate<? super Object>[])null, F.alwaysFalse()).apply(""));

        IgnitePredicate<? super Object> nullPred = null;
        assertTrue(F.and(nullPred).apply(""));
        assertTrue(F.and(nullPred, F.alwaysTrue()).apply(""));
        assertFalse(F.and(nullPred, F.alwaysFalse()).apply(""));
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"unchecked", "NullableProblems"})
    public void testOr() {
        Collection<? extends IgnitePredicate<Object>> ps = Arrays.asList(F.alwaysTrue(), F.alwaysTrue());

        IgnitePredicate<Object> p = F.or(ps);

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        p = F.or(F.alwaysTrue(), F.alwaysTrue(), F.alwaysTrue());

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        p = F.or(ps.toArray(new IgnitePredicate[2]), F.alwaysTrue(), F.alwaysTrue());

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        p = F.or(ps.toArray(new IgnitePredicate[2]), ps.toArray(new IgnitePredicate[2]));

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        ps = Arrays.asList(F.alwaysFalse(), F.alwaysFalse());

        p = F.and(ps);

        assert p != null;
        assert !p.apply(new Object());
        assert !p.apply(null);

        p = F.or(F.alwaysFalse(), F.alwaysTrue(), F.alwaysTrue());

        assert p != null;
        assert p.apply(new Object());
        assert p.apply(null);

        Collection<? extends IgnitePredicate<String>> ps2 =
            Arrays.asList(F.<String>instanceOf(String.class), F.<String>equalTo("test"));

        IgnitePredicate<String> pred2 = F.or(ps2);

        assert pred2 != null;
        assert pred2.apply("test");
        assert pred2.apply("test1");

        assert F.or(F.alwaysFalse()) == F.alwaysFalse();
        assert F.or(F.alwaysTrue()) == F.alwaysTrue();

        assertFalse(F.or(null, (IgnitePredicate<Object>[])null).apply(""));
        assertTrue(F.or((IgnitePredicate<? super Object>[])null, F.alwaysTrue()).apply(""));
        assertFalse(F.or((IgnitePredicate<? super Object>[])null, F.alwaysFalse()).apply(""));

        IgnitePredicate<? super Object> nullPred = null;
        assertFalse(F.or(nullPred).apply(""));
        assertFalse(F.or(F.alwaysFalse(), nullPred, nullPred).apply(""));
        assertTrue(F.or(F.alwaysTrue(), nullPred, nullPred).apply(""));
    }

    /**
     * JUnit.
     */
    public void testCompose() {
        IgniteClosure<String, Class<?>> c = new C1<String, Class<?>>() {
            @Nullable
            @Override public Class<?> apply(String s) {
                return s == null ? null : s.getClass();
            }
        };

        IgnitePredicate<String> p = F.compose(F.<Class>equalTo(String.class), c);

        assert p.apply("test");

        p = F.compose(F.<Class>equalTo(Long.class), c);

        assert !p.apply("test");

        p = F.compose(F.<Class>alwaysFalse(), c);

        assert p == F.<String>alwaysFalse();

        p = F.compose(F.<Class>alwaysTrue(), c);

        assert p == F.<String>alwaysTrue();
    }

    /**
     * JUnit.
     */
    public void testConstant() {
        IgniteOutClosure<Long> c = F.constant(1L);

        assert c.apply() == 1L;
    }

    /**
     * JUnit.
     */
    public void testFactory() {
        F.factory(String.class);

        assert F.factory(String.class).apply() != null;

        try {
            F.factory(ExceptionThrow.class).apply();

            assert false;
        }
        catch (GridRuntimeException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     */
    public void testIdentity() {
        IgniteClosure<Object, Object> c = F.identity();

        Object obj = new Object();
        String str = "test";

        assert c.apply(str) == str;
        assert c.apply(obj) == obj;
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testAsClosure() {
        IgniteClosure<String, Boolean> c = F.as(F.equalTo("test"));

        assert c.apply("test");
        assert !c.apply("test1");
        assert !c.apply(null);
    }

    /**
     * JUnit.
     */
    public void testForMap() {
        Map<String, String> map = new HashMap<>();

        map.put("test-key", "test-value");

        IgniteClosure<String, String> c = F.forMap(map);

        assert c.apply("test") == null;
        assert c.apply("test-key") != null;
        assert "test-value".equals(c.apply("test-key"));

        map.put("test-null-key", null);

        c = F.forMap(map, new IgniteCallable<String>() {
            @Override public String call() { return "default-value"; }
        });

        assert "default-value".equals(c.apply("test"));
        assert !map.containsKey("test");
        assert c.apply("test-key") != null;
        assert "test-value".equals(c.apply("test-key"));
        assert c.apply("test-null-key") == null;
    }

    /**
     * JUnit.
     */
    public void testStringify() {
        IgniteClosure<Object, String> c = F.string();

        assert "1".equals(c.apply(1L));
        assert "test".equals(c.apply("test"));
    }

    /**
     * JUnit.
     */
    @SuppressWarnings("NullableProblems")
    public void testIn() {
        IgnitePredicate<String> p = F.in(Arrays.asList("1", "2", "3"));

        assert p.apply("1");
        assert !p.apply("4");
        assert !p.apply(null);

        p = F.in(Arrays.asList("1", "2", "3", null));

        assert p.apply("1");
        assert !p.apply("4");
        assert p.apply(null);
    }

    /**
     * JUnit.
     */
    public void testReduce() {
        Integer res = F.reduce(Arrays.asList(1, 2, 3),
            new IgniteReducer<Integer, Integer>() {
                private int sum;

                @Override public boolean collect(Integer e) {
                    sum += e;

                    return true;
                }

                @Override public Integer reduce() { return sum; }
            });

        assert res == 6;

        // Always false.
        res = F.reduce(Arrays.asList(1, 2, 3),
            new IgniteReducer<Integer, Integer>() {
                private int sum;

                @Override public boolean collect(Integer e) {
                    sum += e;

                    return false;
                }

                @Override public Integer reduce() { return sum; }
            });

        assert res == 1;

        Map<Integer, String> map = new HashMap<>();

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        res = F.reduce(map,
            new IgniteReducer2<Integer, String, Integer>() {
                private int sum;

                @Override public boolean collect(Integer e1, String e2) {
                    sum += e1;
                    sum += Integer.parseInt(e2);

                    return true;
                }

                @Override public Integer apply() { return sum; }
            });

        assert res != null;
        assert res == 12;

        // Always false.
        res = F.reduce(map,
            new IgniteReducer2<Integer, String, Integer>() {
                private int sum;

                @Override public boolean collect(Integer e1, String e2) {
                    sum += e1;
                    sum += Integer.parseInt(e2);

                    return false;
                }

                @Override public Integer apply() { return sum; }
            });

        assert res != null;
        assert res == 2;
    }

    /**
     * JUnit.
     */
    public void testForEach() {
        final GridTuple<Integer> sum = F.t(0);

        F.forEach(Arrays.asList(1, 2, 3), new CI1<Integer>() {
            @Override public void apply(Integer e) { sum.set(sum.get() + e); }
        });

        assert sum.get() == 6;

        sum.set(0);
        Map<Integer, String> map = new HashMap<>();

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        F.forEach(map,
            new CI1<IgniteBiTuple<Integer, String>>() {
                @Override public void apply(IgniteBiTuple<Integer, String> t) {
                    sum.set(sum.get() + t.get1());
                    sum.set(sum.get() + Integer.parseInt(t.get2()));
                }
            });

        assert sum.get() == 12;

        sum.set(0);

        F.forEach(map,
            new CI1<IgniteBiTuple<Integer, String>>() {
                @Override public void apply(IgniteBiTuple<Integer, String> t) {
                    sum.set(sum.get() + t.get1());
                    sum.set(sum.get() + Integer.parseInt(t.get2()));
                }
            }, F.alwaysFalse());

        assert sum.get() == 0;
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testCopy() {
        Collection<Integer> list = new ArrayList<>();

        Collection<Integer> res = F.copy(list, Arrays.asList(1, 2, 3),
            F.<Integer>alwaysTrue(), F.<Integer>alwaysFalse());

        assert res.isEmpty();

        F.copy(list, Arrays.asList(1, 2, 3), F.<Integer>alwaysTrue(), F.<Integer>alwaysTrue());

        assertEquals(3, res.size());

        res.clear();

        F.copy(list, Arrays.asList(1, 2, 3), null);

        assertEquals(3, res.size());

        res.clear();

        F.copy(list, Arrays.asList(1, 2, 3), F.<Integer>alwaysFalse());

        assert res.isEmpty();

        F.copy(list, Arrays.asList(1, 2, 3), F.<Integer>alwaysTrue());

        assertEquals(3, res.size());
    }

     /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod", "ZeroLengthArrayAllocation"})
    public void testConcatArray() {
        String[] arr = {"1", "2"};

        String[] newArr = F.concat(arr, "3");

        assertEquals("1", newArr[0]);

        assertEquals("2", newArr[1]);

        assertEquals("3", newArr[2]);

        assertEquals(3, newArr.length);

        arr = null;

        newArr = F.concat(arr, "1");

        assertEquals("1", newArr[0]);

        assertEquals(1, newArr.length);

        arr = new String[0];

        newArr = F.concat(arr, "1");

        assertEquals("1", newArr[0]);

        assertEquals(1, newArr.length);
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullableProblems", "NullArgumentToVariableArgMethod", "RedundantCast"})
    public void testConcatNull() {
        assertNull(F.concat((Object[])null, null));
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testTransform() {
        Collection<Integer> list = new ArrayList<>();

        Collection<Integer> res = F.transform(list, Arrays.asList(1, 2, 3),
            new C1<Integer, Integer>() {
                @Override public Integer apply(Integer e) { return e + 1; }
            },
            F.<Integer>alwaysTrue(), F.<Integer>alwaysFalse());

        assert res.isEmpty();

        res = F.transform(list, Arrays.asList(1, 2, 3),
            new C1<Integer, Integer>() {
                @Override public Integer apply(Integer e) { return e + 1; }
            },
            F.<Integer>alwaysTrue(), F.<Integer>alwaysTrue());

        assert !res.isEmpty();
        assertEquals(3, res.size());
        assert res.contains(2);
        assert res.contains(3);
        assert res.contains(4);

        res.clear();

        res = F.transform(list, Arrays.asList(1, 2, 3),
            new C1<Integer, Integer>() {
                @Override public Integer apply(Integer e) { return e + 1; }
            },
            null);

        assertEquals(3, res.size());

        res.clear();

        res = F.transform(list, Arrays.asList(1, 2, 3),
            new C1<Integer, Integer>() {
                @Override public Integer apply(Integer e) { return e + 1; }
            },
            F.<Integer>alwaysTrue());

        assertEquals(3, res.size());

        res.clear();

        res = F.transform(list, Arrays.asList(1, 2, 3),
            new C1<Integer, Integer>() {
                @Override public Integer apply(Integer e) { return e + 1; }
            },
            F.<Integer>alwaysFalse());

        assert res.isEmpty();
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked", "ZeroLengthArrayAllocation"})
    public void testIsAll() {
        // Predicates with 1 free variable.
        assert F.isAll(new Object(), F.<Object>alwaysTrue());
        assert !F.isAll(new Object(), F.<Object>alwaysFalse());

        assert !F.isAll(new Object(), F.<Object>alwaysTrue(), F.<Object>alwaysFalse());
        assert F.isAll(new Object(), F.<Object>alwaysTrue(), F.<Object>alwaysTrue());

        assert F.isAll(new Object(), F.and(F.<Object>alwaysTrue(), F.<Object>alwaysTrue()));
        assert !F.isAll(new Object(), F.and(F.<Object>alwaysTrue(), F.<Object>alwaysFalse()));

        // Predicates with 2 free variables.
        P2<Integer, Integer> evenSum2 = new P2<Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2) { return (i1 + i2) % 2 == 0; }
        };

        P2<Integer, Integer> nullP2 = null;

        P2[] p2Arr = { evenSum2, nullP2 };

        assert F.isAll2(1, 1, evenSum2);
        assert !F.isAll2(1, 4, evenSum2);
        assert F.isAll2(4, 24, evenSum2);

        // Odd cases.
        assert F.isAll2(1, 1, p2Arr);
        assert F.isAll2(1, 1, new P2[0]);
        assert F.isAll2(1, 1, nullP2);

        // Predicates with 3 free variables.
        P3<Integer, Integer, Integer> evenSum3 = new P3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2, Integer i3) {
                return (i1 + i2 + i3) % 2 == 0;
            }
        };

        P3<Integer, Integer, Integer> alwaysTrue3 = new P3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2, Integer i3) { return true; }
        };

        P3<Integer, Integer, Integer> alwaysFalse3 = new P3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2, Integer i3) { return false; }
        };

        P3<Integer, Integer, Integer> nullP3 = null;

        P3[]  p3Arr = { evenSum3, nullP3 };

        assert F.isAll3(1, 1, 2, evenSum3);
        assert !F.isAll3(1, 4, 4, evenSum3);
        assert F.isAll3(4, 24, 6, evenSum3);

        assert !F.isAll3(1, 4, 54, evenSum3, alwaysTrue3);
        assert !F.isAll3(4, 24, 2, evenSum3, alwaysFalse3);
        assert !F.isAll3(4, 24, 20, evenSum3, alwaysTrue3, alwaysFalse3);

        // Odd cases.
        assert F.isAll3(1, 1, p3Arr);
        assert F.isAll3(1, 1, new P3[0]);
        assert F.isAll3(1, 1, nullP3);
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"unchecked", "ZeroLengthArrayAllocation"})
    public void testIsAny() {
        // Predicates with 1 free variable.
        assert F.isAny(new Object(), F.alwaysTrue());
        assert !F.isAny(new Object(), F.alwaysFalse());

        assert F.isAny(new Object(), F.<Object>alwaysTrue(), F.<Object>alwaysFalse());
        assert F.isAny(new Object(), F.<Object>alwaysTrue(), F.<Object>alwaysTrue());
        assert !F.isAny(new Object(), F.<Object>alwaysFalse(), F.<Object>alwaysFalse());

        // Predicates with 2 free variables.
        P2<Integer, Integer> evenSum2 = new P2<Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2) { return (i1 + i2) % 2 == 0; }
        };

        P2<Integer, Integer> alwaysTrue2 = new P2<Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2) { return true; }
        };

        P2<Integer, Integer> alwaysFalse2 = new P2<Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2) { return false; }
        };

        P2<Integer, Integer> nullP2 = null;

        P2[] p2Arr = { evenSum2, nullP2 };

        assert F.isAny2(1, 1, evenSum2);
        assert !F.isAny2(1, 4, evenSum2);
        assert F.isAny2(4, 24, evenSum2);

        assert F.isAny2(1, 4, evenSum2, alwaysTrue2);
        assert F.isAny2(4, 24, evenSum2, alwaysFalse2);
        assert !F.isAny2(1, 4, evenSum2, alwaysFalse2, alwaysFalse2);

        // Odd cases.
        assert F.isAny2(1, 1, p2Arr);
        assert !F.isAny2(1, 1, new P2[0]);
        assert !F.isAny2(1, 1, nullP2);

        // Predicates with 3 free variables.
        P3<Integer, Integer, Integer> evenSum3 = new P3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2, Integer i3) {
                return (i1 + i2 + i3) % 2 == 0;
            }
        };

        P3<Integer, Integer, Integer> alwaysTrue3 = new P3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2, Integer i3) { return true; }
        };

        P3<Integer, Integer, Integer> alwaysFalse3 = new P3<Integer, Integer, Integer>() {
            @Override public boolean apply(Integer i1, Integer i2, Integer i3) { return false; }
        };

        P3<Integer, Integer, Integer> nullP3 = null;

        P3[] p3Arr = { evenSum3, nullP3 };

        assert F.isAny3(1, 1, 2, evenSum3);
        assert F.isAny3(1, 1, 2, evenSum3);
        assert !F.isAny3(1, 4, 4, evenSum3);
        assert F.isAny3(4, 24, 6, evenSum3);

        assert F.isAny3(1, 4, 54, evenSum3, alwaysTrue3);
        assert F.isAny3(4, 24, 2, evenSum3, alwaysFalse3);
        assert F.isAny3(4, 24, 20, evenSum3, alwaysTrue3, alwaysFalse3);
        assert !F.isAny3(1, 24, 20, evenSum3, alwaysFalse3, alwaysFalse3);

        // Odd cases.
        assert F.isAny3(1, 1, 2, p3Arr);
        assert !F.isAny3(1, 1, 2, new P3[0]);
        assert !F.isAny3(1, 1, 2, nullP3);
    }

    /**
     * JUnit.
     */
    public void testFind() {
        String res = F.find(Arrays.asList("1", "2", "3"), "default-value",
            F.<String>alwaysFalse(), new P1<String>() {
                @Override public boolean apply(String e) { return "2".equals(e); }
            });

        assert "2".equals(res);

        res = F.find(
            Arrays.asList("1", "2", "3"),
            "default-value",
            F.<String>alwaysFalse(),
            new P1<String>() {
                @Override public boolean apply(String e) { return "2".equals(e); }
            },
            null);

        assert "2".equals(res);

        res = F.find(Arrays.asList("1", "2", "3"), "default-value",
            F.<String>alwaysFalse(), new P1<String>() {
                @Override public boolean apply(String e) { return "test".equals(e); }
            });

        assert "default-value".equals(res);

        Integer resInt = F.find(Arrays.asList("1", "2", "3"), -1,
            new C1<String, Integer>() {
                @Override public Integer apply(String e) { return Integer.parseInt(e); }
            },
            F.<String>alwaysFalse(), new P1<String>() {
                @Override public boolean apply(String e) { return "2".equals(e); }
            });

        assert resInt == 2;

        resInt = F.find(Arrays.asList("1", "2", "3"), -1,
            new C1<String, Integer>() {
                @Override public Integer apply(String e) { return Integer.parseInt(e); }
            }, F.<String>alwaysFalse());

        assert resInt == -1;

        resInt = F.find(Arrays.asList("1", "2", "3"), -1,
            new C1<String, Integer>() {
                @Override public Integer apply(String e) { return Integer.parseInt(e); }
            }, F.<String>alwaysTrue());

        assert resInt == 1;

        P1<String> nullPred = null;

        assertNull(F.find(Arrays.asList("1", "2", "3"), nullPred));
    }

    /**
     * JUnit.
     */
    public void testPartition() {
        IgniteBiTuple<Collection<String>, Collection<String>> tuple =
            F.partition(Arrays.asList("1", "2", "3"),
                new P1<String>() {
                    @Override public boolean apply(String e) { return "2".equals(e); }
                });

        assert tuple != null;

        assert tuple.get1() != null;
        assert tuple.get1().size() == 1;
        assert tuple.get1().contains("2");

        assert tuple.get2() != null;
        assert tuple.get2().size() == 2;
        assert tuple.get2().contains("1");
        assert tuple.get2().contains("3");

        Map<Integer, String> map = new HashMap<>();

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        IgniteBiTuple<Map<Integer, String>, Map<Integer, String>> tupleMap = F.partition(map,
            new P2<Integer, String>() {
                @Override public boolean apply(Integer e1, String e2) {
                    return e1 == 2 && "2".equals(e2);
                }
            });

        assert tupleMap != null;

        assert tupleMap.get1() != null;
        assert tupleMap.get1().size() == 1;
        assert tupleMap.get1().containsKey(2);
        assert tupleMap.get1().containsValue("2");

        assert tupleMap.get2() != null;
        assert tupleMap.get2().size() == 2;
        assert tupleMap.get2().containsKey(1);
        assert tupleMap.get2().containsValue("1");
        assert tupleMap.get2().containsKey(3);
        assert tupleMap.get2().containsValue("3");
    }

    /**
     * JUnit.
     */
    public void testExist() {
        IgnitePredicate<String> pred = new P1<String>() {
            @Override public boolean apply(String e) { return "2".equals(e); }
        };

        assert F.exist(Arrays.asList("1", "2", "3"), pred);
        assert !F.exist(Arrays.asList("1", "2", "3"), F.<String>alwaysFalse(), pred);
        assert F.exist(Arrays.asList("1", "2", "3"), pred);
        assert !F.exist(Arrays.asList("1", "2", "3"), F.<String>alwaysFalse());
        assert F.exist(Arrays.asList("1", "2", "3"), F.<String>alwaysTrue());
        assert F.exist(Arrays.asList("1", "2", "3"), F.<String>alwaysTrue(), null);
        assert !F.exist(Arrays.asList("1", "2", "3"), F.<String>alwaysFalse(), null);
        assert F.exist(Arrays.asList("1", "2", "3"), (P1<String>[])null);

        P1<String> nullStrPred = null;

        assert F.exist(Arrays.asList("1", "2", "3"), nullStrPred);
        assert F.exist(Arrays.asList("1", "2", "3"), (P1<String>[])null);

        Map<Integer, String> map = new HashMap<>();

        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");

        IgnitePredicate<Map.Entry<Integer, String>> predMap = new P1<Map.Entry<Integer, String>>() {
            @Override public boolean apply(Map.Entry<Integer, String> entry) {
                return entry.getKey() == 2 && "2".equals(entry.getValue());
            }
        };

        assert F.exist(map, predMap);
        assert !F.exist(map, predMap, F.<Map.Entry<Integer, String>>alwaysFalse());
        assert F.exist(map, F.<Map.Entry<Integer, String>>alwaysTrue(), F.<Map.Entry<Integer, String>>alwaysTrue());
        assert F.exist(map, F.<Map.Entry<Integer, String>>alwaysTrue(), null);
        assert !F.exist(map, F.<Map.Entry<Integer, String>>alwaysFalse(), null);
        assert F.exist(map, (P1<Map.Entry<Integer, String>>[])null);

        IgnitePredicate<Map.Entry<Integer, String>> nullMapPred = null;

        assert F.exist(map, nullMapPred);
    }

    /**
     * JUnit.
     */
    public void testForAll() {
        IgnitePredicate<String> p = new P1<String>() {
            @Override public boolean apply(String e) { return e.contains("2"); }
        };

        // Iterable argument.
        assert !F.forAll(Arrays.asList("1", "2", "3"), p);
        assert F.forAll(Arrays.asList("21", "22", "23"), p);
        assert F.forAll(Arrays.asList("21", "22", "23"), p, F.<String>alwaysTrue());
        assert !F.forAll(Arrays.asList("1", "2", "3"), p, F.<String>alwaysTrue());
        assert F.forAll(Arrays.asList("1", "2", "3"), F.<String>alwaysTrue(), F.<String>alwaysTrue());
        assert F.forAll(Arrays.asList("1", "2", "3"), F.<String>alwaysTrue(), null);
        assert !F.forAll(Arrays.asList("1", "2", "3"), F.<String>alwaysFalse(), null);
        assert F.forAll(Arrays.asList("1", "2", "3"), (P1<String>[])null);

        IgnitePredicate<String> nullStrPred = null;

        assert F.forAll(Arrays.asList("1", "2", "3"), nullStrPred);

        // Array argument.
        assert !F.forAll(new String[] {"1", "2", "3"}, p);
        assert F.forAll(new String[] {"21", "22", "23"}, p);
        assert F.forAll(new String[] {"21", "22", "23"}, p, F.<String>alwaysTrue());
        assert !F.forAll(new String[] {"1", "2", "3"}, p, F.<String>alwaysTrue());
        assert F.forAll(new String[] {"1", "2", "3"}, F.<String>alwaysTrue(), F.<String>alwaysTrue());
        assert F.forAll(new String[] {"1", "2", "3"}, F.<String>alwaysTrue(), null);
        assert !F.forAll(new String[] {"1", "2", "3"}, F.<String>alwaysFalse(), null);
        assert F.forAll(new String[] {"1", "2", "3"}, nullStrPred);
        assert F.forAll(new String[] {"1", "2", "3"}, (P1<String>[])null);

        Map<Integer, String> map = F.asMap(21, "21", 22, "22", 23, "23");

        IgnitePredicate<Map.Entry<Integer, String>> p2 = new P1<Map.Entry<Integer, String>>() {
            @Override public boolean apply(Map.Entry<Integer, String> entry) {
                return entry.getValue().contains("2");
            }
        };

        assert F.forAll(map, p2);
        assert !F.forAll(map, p2, F.<Map.Entry<Integer, String>>alwaysFalse());
        assert F.forAll(map, F.<Map.Entry<Integer, String>>alwaysTrue(), F.<Map.Entry<Integer, String>>alwaysTrue());
        assert F.forAll(map, F.<Map.Entry<Integer, String>>alwaysTrue(), null);
        assert !F.forAll(map, F.<Map.Entry<Integer, String>>alwaysFalse(), null);
        assert F.forAll(map, (P1<Map.Entry<Integer, String>>[])null);

        IgnitePredicate<Map.Entry<Integer, String>> nullMapPred = null;

        assert F.forAll(map, nullMapPred);

        map.remove(21);
        map.put(1, "1");

        assert !F.forAll(map, p2);
        assert !F.forAll(map, p2, F.<Map.Entry<Integer, String>>alwaysFalse());
        assert F.forAll(map, F.<Map.Entry<Integer, String>>alwaysTrue(), F.<Map.Entry<Integer, String>>alwaysTrue());
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testForAny() {
        IgnitePredicate<String> p = new P1<String>() {
            @Override public boolean apply(String e) { return e.contains("2"); }
        };

        // Iterable argument.
        assert F.forAny(Arrays.asList("1", "2", "3"), p);
        assert !F.forAny(Arrays.asList("1", "3", "5"), p);
        assert F.forAny(Arrays.asList("1", "2", "3"), F.<String>alwaysTrue());
        assert !F.forAny(Arrays.asList("1", "2", "3"), F.<String>alwaysFalse());
        assert F.forAny(Arrays.asList("1", "2", "3"), p, F.<String>alwaysTrue());
        assert !F.forAny(Arrays.asList("1", "5", "3"), p, F.<String>alwaysTrue());
        assert !F.forAny(Arrays.asList("1", "2", "3"), p, F.<String>alwaysFalse());
        assert F.forAny(Arrays.asList("1", "2", "3"), F.<Object>alwaysTrue(), null);

        // Empty set.
        assert !F.forAny(Collections.<String>emptySet());
        assert !F.forAny(Collections.<String>emptySet(), (P1<String>[])null);
        assert !F.forAny(Collections.<String>emptySet(), p);
        assert !F.forAny(Collections.<String>emptySet(), F.<Object>alwaysTrue());
        assert !F.forAny(Collections.<String>emptySet(), F.<Object>alwaysFalse());
        assert !F.forAny(Collections.<String>emptySet(), F.<Object>alwaysTrue(), null);
        assert !F.forAny(Collections.<String>emptySet(), F.<Object>alwaysFalse(), null);

        IgnitePredicate<String> truePred = new P1<String>() {
            @Override public boolean apply(String e) {
                return true;
            }
        };

        assert !F.forAny(Collections.<String>emptySet(), truePred);

        IgnitePredicate<String> falsePred = new P1<String>() {
            @Override public boolean apply(String e) {
                return false;
            }
        };

        assert !F.forAny(Collections.<String>emptySet(), falsePred);

        P1<String> nullStrPred = null;

        assert !F.forAny(Collections.<String>emptySet(), nullStrPred);

        // Array argument.
        assert F.forAny(new String[] {"1", "2", "3"}, p);
        assert !F.forAny(new String[] {"1", "3", "5"}, p);
        assert F.forAny(new String[] {"1", "2", "3"}, F.<String>alwaysTrue());
        assert !F.forAny(new String[] {"1", "2", "3"}, F.<String>alwaysFalse());
        assert F.forAny(new String[] {"1", "2", "3"}, p, F.<String>alwaysTrue());
        assert !F.forAny(new String[] {"1", "5", "3"}, p, F.<String>alwaysTrue());
        assert !F.forAny(new String[] {"1", "2", "3"}, p, F.<String>alwaysFalse());

        // Empty array.
        assert !F.forAny(EMPTY_STR);
        assert !F.forAny(EMPTY_STR, (P1<String>[])null);
        assert !F.forAny(EMPTY_STR, nullStrPred);
        assert !F.forAny(EMPTY_STR, p);
        assert !F.forAny(EMPTY_STR, F.<Object>alwaysTrue());
        assert !F.forAny(EMPTY_STR, F.<Object>alwaysFalse());
        assert !F.forAny(EMPTY_STR, truePred);
        assert !F.forAny(EMPTY_STR, falsePred);
        assert !F.forAny(EMPTY_STR, F.<Object>alwaysTrue(), null);
        assert !F.forAny(EMPTY_STR, F.<Object>alwaysFalse(), null);

        Map<Integer, String> map = F.asMap(21, "21", 22, "22", 23, "23");

        assert F.forAny(map);
        assert F.forAny(map, null);

        IgnitePredicate<Map.Entry<Integer, String>> p2 = new P1<Map.Entry<Integer, String>>() {
            @Override public boolean apply(Map.Entry<Integer, String> e) {
                return e.getValue().contains("1");
            }
        };

        assert F.forAny(map, p2);
        assert !F.forAny(map, p2, F.<Map.Entry<Integer, String>>alwaysFalse());
        assert F.forAny(map, F.<Map.Entry<Integer, String>>alwaysTrue(), F.<Map.Entry<Integer, String>>alwaysTrue());
        assert F.forAny(map, F.<Map.Entry<Integer, String>>alwaysTrue(), null);
        assert !F.forAny(map, F.<Map.Entry<Integer, String>>alwaysFalse(), null);
        assert F.forAny(map, (P1<Map.Entry<Integer, String>>[])null);

        IgnitePredicate<Map.Entry<Integer, String>> nullMapPred = null;

        assert F.forAny(map, nullMapPred);

        map.remove(21);

        assert !F.forAny(map, p2);

        map.put(1, "1");

        assert F.forAny(map, p2);
        assert !F.forAny(map, p2, F.<Map.Entry<Integer, String>>alwaysFalse());
        assert F.forAny(map, F.<Map.Entry<Integer, String>>alwaysTrue(), F.<Map.Entry<Integer, String>>alwaysTrue());

        map.clear();

        // For empty map.
        assert F.forAny(map);
        assert !F.forAny(map, p2);
        assert !F.forAny(map, F.<Map.Entry<Integer, String>>alwaysFalse());
        assert F.forAny(map, F.<Map.Entry<Integer, String>>alwaysTrue());
    }

    /**
     * JUnit.
     */
    public void testFold() {
        Integer res = F.fold(Arrays.asList(1, 2, 3), 1, new C2<Integer, Integer, Integer>() {
            @Override public Integer apply(Integer val1, Integer val2) {
                return val1 + val2;
            }
        });

        assert res != null;
        assert res == 7;
    }

    /**
     * JUnit.
     */
    public void testEqArray() {
        Integer[] a1 = new Integer[] {1, 2, 3, 4};
        Integer[] a2 = new Integer[] {2, 1, 4, 3};

        assert F.eqArray(a1, a2, false, false);

        a1 = new Integer[] {1, 1, 3, 4};
        a2 = new Integer[] {2, 1, 4, 3};

        assert !F.eqArray(a1, a2, false, true);

        a1 = new Integer[] {1, 1, 3, 4};
        a2 = new Integer[] {2, 1, 4};

        assert !F.eqArray(a1, a2, false, true);

        a1 = new Integer[] {1, 2, 3, 4, 5, 6, 7, 8};
        a2 = new Integer[] {3, 4, 5, 6, 7, 8, 9, 10};

        assert !F.eqArray(a1, a2, true, false);
    }

    /**
     *  JUnit.
     */
    public void testIterable() {
        Collection<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 7, 7, 7, 7);

        try {
            P1<Integer> p = new P1<Integer>() {
                @Override public boolean apply(Integer e) { return e % 2 == 0; }
            };

            for (int i : F.iterable(data, p)) {
                System.out.print(i + " ");

                assert i % 2 == 0;
            }

            for (int i : F.iterable(data, p, null)) {
                System.out.print(i + " ");

                assert i % 2 == 0;
            }

            P1<Integer> nullIntPred = null;

            Collection<Integer> data2 = new ArrayList<>(data.size());
            for (int i : F.iterable(data, nullIntPred)) {
                System.out.print(i + " ");

                data2.add(i);
            }

            assertEquals(data, data2);

            data2.clear();

            for (int i : F.iterable(data, (P1<Integer>[])null)) {
                System.out.print(i + " ");

                data2.add(i);
            }

            assertEquals(data, data2);
        }
        catch (Exception e) {
            assert false : e.getMessage();
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        Collection<Integer> data =
            new LinkedList<>(Arrays.asList(1, 2, 12, 14, 3, 4, 5, 45, 11, 6, 7, 8, 9, 10, 7));

        int oldSize = data.size();

        P1<Integer> p = new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        };

        int cnt = 0;

        Iterator<Integer> iter = F.iterator(data, F.<Integer>identity(), false, p);

        while (iter.hasNext()) {
            assert iter.next() % 2 == 0;

            if (cnt % 2 == 0)
                iter.remove();

            cnt++;
        }

        assertEquals(7, cnt);

        assert !data.contains(2);

        assert data.contains(12);

        assert !data.contains(14);

        assert data.contains(4);

        assert !data.contains(6);

        assert data.contains(8);

        assert !data.contains(10);

        assertEquals(oldSize - 4, data.size());

        iter = F.iterator(data, F.<Integer>identity(), true, p);

        while (iter.hasNext()) {
            iter.next();

            try {
                iter.remove();

                fail();
            }
            catch (UnsupportedOperationException e) {
                info("Caught expected exception: " + e);
            }
        }

        data = new LinkedList<>(Arrays.asList(1, 2, 12, 14, 3, 4, 5, 45, 11, 6, 7, 8, 9, 10, 7));

        iter = F.iterator(data, F.<Integer>identity(), false, p, null);

        cnt = 0;

        while (iter.hasNext()) {
            assert iter.next() % 2 == 0;

            if (cnt % 2 == 0)
                iter.remove();

            cnt++;
        }

        assertEquals(7, cnt);
    }

    /**
     * JUnit.
     */
    public void testFailIterator() {
        Collection<Integer> data = Arrays.asList(1, 2);

        Iterator<Integer> iter = F.iterator0(data, true, F.<Integer>alwaysTrue());

        try {
            iter.next();
            iter.next();
            iter.next();

            assert false;
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     */
    private static class ExceptionThrow {
        /** @throws Exception Thrown always. */
        private ExceptionThrow() throws Exception {
            throw new Exception("Test exception");
        }
    }

    /**
     * JUnit.
     */
    public void testCollectionView1() {
        Collection<Integer> c = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        IgnitePredicate<Integer> p = new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        };

        Collection<Integer> view = F.view(c, p);

        assert view.size() == 5;

        for (int i : view) { assert i % 2 == 0; }

        view.add(20);

        for (Iterator<Integer> iter = view.iterator(); iter.hasNext(); iter.next())
            iter.remove();

        assert view.isEmpty();

        assert !c.isEmpty();
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testCollectionView2() {
        Collection<String> fruits = new LinkedList<>(F.asList("apple", "pear", "orange"));

        // Not filtered view.
        Collection<String> view = F.view(fruits);

        assertEquals(fruits.size(), view.size());

        view.add("mandarin");

        assert view.contains("mandarin");

        assert fruits.contains("mandarin");

        assert view.remove("mandarin");

        assert !view.contains("mandarin");

        assert !fruits.contains("mandarin");

        // Filtered view.
        view = F.view(fruits, new P1<String>() {
            @Override public boolean apply(String e) {
                // Filter out orange.
                return e.contains("p");
            }
        });

        assertEquals(fruits.size() - 1, view.size());

        assert !view.contains("orange");

        assert fruits.contains("orange");

        for (String f : view) {
            assert "apple".equals(f) || "pear".equals(f);
        }

        // Must be filtered out.
        assert !view.add("melon");

        assert !view.contains("melon");

        assert !fruits.contains("melon");

        // Must be added.
        assert view.add("apricot");

        assert view.contains("apricot");

        assert fruits.contains("apricot");

        // With null predicates.
        view = F.view(fruits, null);

        assertEquals(fruits.size(), view.size());

        view.clear();

        assert view.isEmpty();

        assert fruits.isEmpty();
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation", "unchecked"})
    public void testCollectionViewNullPredicates() {
        Collection<Integer> c = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        IgnitePredicate<Integer> nullIntPred = null;

        int cSize = c.size();

        assertEquals(cSize, F.view(c, nullIntPred).size());
        assertEquals(cSize, F.view(c, new P1[0]).size());
        assertEquals(cSize, F.view(c, (P1<Integer>[])null).size());
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testTransformList() {
        Collection<String> fruits = new LinkedList<>(F.asList("apple", "pear", "orange"));

        // Filtered transformation.
        List<Integer> list = F.transformList(fruits, new C1<String, Integer>() {
                @Override public Integer apply(String s) {
                    return s.length();
                }
            }, new P1<String>() {
                @Override public boolean apply(String e) {
                    return !"orange".equals(e);
                }
            }
        );

        assertEquals(fruits.size() - 1, list.size());

        list.contains(4);
        list.contains(5);

        list.add(8);

        assert list.contains(8);

        assert list.remove(Integer.valueOf(8));

        Iterator<Integer> iter = list.iterator();

        assertEquals(5, iter.next().intValue());

        iter.remove();

        assert !list.contains(5);

        list.clear();

        assert list.isEmpty();

        assert !fruits.isEmpty();

        // With null predicates.
        list = F.transformList(fruits, new C1<String, Integer>() {
            @Override public Integer apply(String s) {
                return s.length();
            }
        }, null);

        assertEquals(fruits.size(), list.size());

        // With always true and always false predicates.
        list = F.transformList(fruits, new C1<String, Integer>() {
            @Override public Integer apply(String s) {
                return s.length();
            }
        }, F.alwaysFalse());

        assertEquals(0, list.size());

        list = F.transformList(fruits, new C1<String, Integer>() {
            @Override public Integer apply(String s) {
                return s.length();
            }
        }, F.alwaysTrue());

        assertEquals(fruits.size(), list.size());
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod", "ZeroLengthArrayAllocation", "unchecked"})
    public void testTransformListNullPredicate() {
        Collection<Integer> c = new LinkedList<>(F.asList(1, 2, 3));

        P1<Integer> pred = new P1<Integer>() {
            @Override public boolean apply(Integer e) {
                return e != 1;
            }
        };

        P1<Integer> nullPred = null;

        int cSize = c.size();
        IgniteClosure<Integer, Integer> identity = F.identity();

        assertEquals(cSize, F.transformList(c, identity, nullPred).size());
        assertEquals(cSize - 1, F.transformList(c, identity, pred, null).size());
        assertEquals(cSize, F.transformList(c, identity, (P1<Integer>[])null).size());
        assertEquals(cSize, F.transformList(c, identity, new P1[0]).size());
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testTransformSet() {
        Collection<String> fruits = new LinkedList<>(F.asList("apple", "pear", "orange"));

        // Filtered transformation.
        Set<Integer> set = F.transformSet(fruits, new C1<String, Integer>() {
                @Override public Integer apply(String s) {
                    return s.length();
                }
            }, new P1<String>() {
                @Override public boolean apply(String e) {
                    return !"orange".equals(e);
                }
            }
        );

        assertEquals(fruits.size() - 1, set.size());

        set.contains(5);
        set.contains(4);

        set.add(8);

        assert set.contains(8);

        assert set.remove(Integer.valueOf(8));

        assert !set.contains(8);

        Iterator<Integer> iter = set.iterator();

        iter.next();

        iter.remove();

        assertEquals(1, set.size());

        assertEquals(3, fruits.size());

        set.clear();

        assert set.isEmpty();

        assertEquals(3, fruits.size());

        // With null predicates.
        set = F.transformSet(fruits, new C1<String, Integer>() {
            @Override public Integer apply(String s) {
                return s.length();
            }
        }, null);

        assertEquals(fruits.size(), set.size());

        // With always true and always false predicates.
        set = F.transformSet(fruits, new C1<String, Integer>() {
            @Override public Integer apply(String s) {
                return s.length();
            }
        }, F.alwaysFalse());

        assertEquals(0, set.size());

        set = F.transformSet(fruits, new C1<String, Integer>() {
            @Override public Integer apply(String s) {
                return s.length();
            }
        }, F.alwaysTrue());

        assertEquals(fruits.size(), set.size());
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod", "ZeroLengthArrayAllocation", "unchecked"})
    public void testTransformSetNullPredicate() {
        Collection<Integer> c = new HashSet<>(F.asList(1, 2, 3));

        P1<Integer> pred = new P1<Integer>() {
            @Override public boolean apply(Integer e) {
                return e != 1;
            }
        };

        P1<Integer> nullPred = null;

        int cSize = c.size();
        IgniteClosure<Integer, Integer> identity = F.identity();

        assertEquals(cSize, F.transformSet(c, identity, nullPred).size());
        assertEquals(cSize - 1, F.transformSet(c, identity, pred, null).size());
        assertEquals(cSize, F.transformSet(c, identity, (P1<Integer>[])null).size());
        assertEquals(cSize, F.transformSet(c, identity, new P1[0]).size());
    }

    /**
     * JUnit.
     */
    public void testMapView1() {
        Map<Integer, Integer> c = new HashMap<>();

        for (int i = 1; i <= 10; i++)
            c.put(i, i);

        IgnitePredicate<Integer> p = new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        };

        Map<Integer, Integer> view = F.view(c, p);

        assert view.size() == 5;

        for (int i : view.values()) { assert i % 2 == 0; }

        view.put(20, 20);

        view.containsKey(20);

        Iterator<Map.Entry<Integer, Integer>> iter = view.entrySet().iterator();

        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }

        assert view.isEmpty();
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testMapView2() {
        Map<String, Integer> prices = new HashMap<>(F.asMap("apple", 5, "pear", 6, "orange", 3));

        // Not filtered view.
        Map<String, Integer> view = F.view(prices);

        assertEquals(prices.size(), view.size());

        view.remove("orange");

        assert !view.containsKey("orange");

        view.put("orange", 4);

        assertEquals(Integer.valueOf(4), view.get("orange"));

        // Filtered view.
        view = F.view(prices, new P1<String>() {
            @Override public boolean apply(String e) {
                return "apple".equals(e) || "orange".equals(e);
            }
        });

        assertEquals(2, view.size());

        assertEquals(2, view.entrySet().size());

        assertEquals(2, view.keySet().size());

        assert !view.containsKey("pear");

        assert !view.containsValue(6);

        assert view.get("pear") == null;

        assertEquals(2, view.keySet().size());

        assert view.keySet().contains("apple");

        assert view.keySet().contains("orange");

        assertEquals(2, view.values().size());

        assert view.values().contains(5);

        assert view.values().contains(4);

        view.remove("orange");

        assert !view.containsKey("orange");

        view.clear();

        assert view.isEmpty();

        prices.clear();

        // Must be filtered out.
        view.put("mandarin", 10);

        assert view.isEmpty();

        assert prices.isEmpty();

        // With null predicates.
        view = F.view(prices, null);

        assertEquals(prices.size(), view.size());

        // With always true and always false predicates.
        view = F.view(prices, F.alwaysFalse());

        assertEquals(0, view.size());

        view = F.view(prices, F.alwaysTrue());

        assertEquals(prices.size(), view.size());
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation", "unchecked"})
    public void testMapViewNullPredicates() {
        Map<Integer, Integer> c = new HashMap<>();

        for (int i = 1; i <= 10; i++)
            c.put(i, i);

        IgnitePredicate<Integer> p = new P1<Integer>() {
            @Override public boolean apply(Integer e) { return e % 2 == 0; }
        };

        IgnitePredicate<Integer> nullPred = null;

        int cSize = c.size();

        assertEquals(cSize, F.view(c, nullPred).size());
        assertEquals(5, F.view(c, p, null).size());
        assertEquals(cSize, F.view(c, (P1<Integer>[])null).size());
        assertEquals(cSize, F.view(c, new P1[0]).size());
    }

    /**
     * JUnit.
     */
    public void testViewAsMap() {
        Set<Integer> c = F.asSet(1, 2, 3);

        IgniteClosure<Integer, String> clo = new C1<Integer, String>() {
            @Override public String apply(Integer e) {
                return String.valueOf(e);
            }
        };

        Map<Integer, String> m = F.viewAsMap(c, clo);

        assertEquals("1", m.get(1));
        assertEquals("2", m.get(2));
        assertEquals("3", m.get(3));
        assertNull(m.get(4));

        m = F.viewAsMap(c, clo, new P1<Integer>() {
            @Override public boolean apply(Integer e) {
                return (e & 1) == 0;
            }
        });

        assertNull(m.get(1));
        assertEquals("2", m.get(2));
        assertFalse(m.containsKey(3));
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"ZeroLengthArrayAllocation", "unchecked"})
    public void testSize() {
        Collection<String> c = F.asList("v1", "v2", "v3");

        assertEquals(3, F.size(c));
        assertEquals(0, F.size(c, F.<String>alwaysFalse()));
        assertEquals(3, F.size(c, F.<String>alwaysTrue()));

        P1<String> pred = new P1<String>() {
            @Override public boolean apply(String e) {
                return e.contains("2");
            }
        };

        P1<String> nullPred = null;

        assertEquals(3, F.size(c, nullPred));
        assertEquals(1, F.size(c, pred));
        assertEquals(1, F.size(c, pred, null));
        assertEquals(3, F.size(c, (P1<String>[])null));
        assertEquals(3, F.size(c, new P1[0]));

        assertEquals(3, F.size(c.iterator()));
        assertEquals(0, F.size(c.iterator(), F.<String>alwaysFalse()));
        assertEquals(3, F.size(c.iterator(), F.<String>alwaysTrue()));

        assertEquals(1, F.size(c.iterator(), pred));
        assertEquals(1, F.size(c.iterator(), pred, null));
        assertEquals(3, F.size(c.iterator(), nullPred));
        assertEquals(3, F.size(c.iterator(), (P1<String>[])null));
        assertEquals(3, F.size(c.iterator(), new P1[0]));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCacheExpire() throws Exception {
        Ignite g = startGrid(1);

        try {
            GridCache<String, String> cache = g.cache(null);

            cache.put("k1", "v1");
            cache.put("k2", "v2");

            assert cache.forAll(F.<String, String>cacheExpireBefore(Long.MAX_VALUE));
            assert !cache.forAll(F.<String, String>cacheExpireAfter(Long.MAX_VALUE));
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCacheContainsGet() throws Exception {
        Ignite g = startGrid(1);

        try {
            GridCache<String, String> cache = g.cache(null);

            cache.put("k1", "v1");


            assertEquals("v1", cache.get("k1"));

            cache.put("k2", "v2");

            assertEquals("v2", cache.get("k2"));

            assert cache.forAll(F.<String, String>cacheContainsGet(Arrays.asList("v1", "v2")));
            assert !cache.forAll(F.<String, String>cacheContainsGet(Arrays.asList("v2")));
            assert !cache.forAll(F.<String, String>cacheContainsGet(Arrays.asList("v6")));

            assert cache.forAll(F.<String, String>cacheContainsGet("v1", "v2"));
            assert !cache.forAll(F.<String, String>cacheContainsGet("v2"));
            assert !cache.forAll(F.<String, String>cacheContainsGet("v6"));

            assert cache.forAll(F.<String, String>cacheContainsGet(F.asMap("k1", "v1", "k2", "v2")));
            assert !cache.forAll(F.<String, String>cacheContainsGet(F.asMap("k2", "v2")));
            assert !cache.forAll(F.<String, String>cacheContainsGet(F.asMap("k2", "v5")));
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCacheContainsEntriesGet1() throws Exception {
        Ignite g = startGrid(1);

        try {
            GridCache<String, String> cache = g.cache(null);

            cache.put("k1", "v1");
            cache.put("k2", "v2");

            Map.Entry<String, String> e1 = new AbstractMap.SimpleImmutableEntry<>("k1", "v1");
            Map.Entry<String, String> e2 = new AbstractMap.SimpleImmutableEntry<>("k2", "v2");
            Map.Entry<String, String> e3 = new AbstractMap.SimpleImmutableEntry<>("k2", "v1");

            assert cache.forAll(F.cacheContainsEntriesGet(Arrays.asList(e1, e2)));
            assert !cache.forAll(F.cacheContainsEntriesGet(Arrays.asList(e1)));
            assert !cache.forAll(F.cacheContainsEntriesGet(Arrays.asList(e1, e3)));

            assert cache.forAll(F.cacheContainsEntriesGet(e1, e2));
            assert !cache.forAll(F.cacheContainsEntriesGet(e1));
            assert !cache.forAll(F.cacheContainsEntriesGet(e1, e3));
        }
        finally {
            stopGrid(1);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCacheValuesGet() throws Exception {
        Ignite g = startGrid(1);

        try {
            GridCache<String, String> cache = g.cache(null);

            cache.put("k1", "v1");
            cache.put("k2", "v2");

            assert cache.forAll(F.<String, String>cacheValuesGet());
            assert cache.forAll(F.<String, String>cacheValuesGet(F.<String>alwaysTrue()));
            assert !cache.forAll(F.<String, String>cacheValuesGet(F.<String>alwaysFalse()));

            P1<String> truePred = new P1<String>() {
                @Override public boolean apply(String e) {
                    return "v1".equals(e) || "v2".equals(e);
                }
            };

            assert cache.forAll(F.<String, String>cacheValuesGet(truePred));

            P1<String> nullPred = null;

            assert cache.forAll(F.<String, String>cacheValuesGet(nullPred));
            assert cache.forAll(F.<String, String>cacheValuesGet(truePred, null));
            assert cache.forAll(F.<String, String>cacheValuesGet((P1<String>[])null));

            assert !cache.forAll(F.<String, String>cacheValuesGet(new P1<String>() {
                @Override public boolean apply(String e) {
                    return "v3".equals(e);
                }
            }));
        }
        finally {
            stopGrid(1);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Test event.
     */
    private static class TestEvent extends IgniteEventAdapter {
        /**
         * @param type Event type.
         */
        private TestEvent(int type) {
            super(new GridTestNode(UUID.randomUUID()), "test message", type);
        }

        /**
         * @param node Node.
         */
        private TestEvent(ClusterNode node) {
            super(node, "test message", 1);
        }
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testEventType() {
        // Always false.
        IgnitePredicate<IgniteEvent> p = F.eventType();

        assert p != null;

        for (int i = 1; i < 100; i++)
            assert !p.apply(new TestEvent(i));

        // Always false.
        p = F.eventType(null);

        assert p != null;

        for (int i = 1; i < 100; i++)
            assert !p.apply(new TestEvent(i));

        p = F.eventType(1, 5, 10);

        assert p.apply(new TestEvent(1));
        assert !p.apply(new TestEvent(2));
        assert !p.apply(new TestEvent(3));
        assert p.apply(new TestEvent(5));
        assert p.apply(new TestEvent(10));
        assert !p.apply(new TestEvent(153));
    }

    /**
     * JUnit.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testEventId() {
        // Always false.
        IgnitePredicate<IgniteEvent> p = F.eventId();

        for (int i = 1; i < 100; i++)
            assert !p.apply(new TestEvent(i));

        // Always false.
        p = F.eventId(null);

        for (int i = 1; i < 100; i++)
            assert !p.apply(new TestEvent(i));

        IgniteEvent evt1 = new TestEvent(1);
        IgniteEvent evt2 = new TestEvent(2);
        IgniteEvent evt3 = new TestEvent(3);

        p = F.eventId(evt1.id(), evt3.id());

        assert p.apply(evt1);
        assert !p.apply(evt2);
        assert p.apply(evt3);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEventAfter() throws Exception {
        IgniteEvent evt1 = new TestEvent(1);

        IgnitePredicate<IgniteEvent> p = F.eventAfter(U.currentTimeMillis() + 100);

        assert p != null;

        assert !p.apply(evt1);

        p = F.eventAfter(U.currentTimeMillis() - 100);

        IgniteEvent evt3 = new TestEvent(3);

        assert p.apply(evt3);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEventNode1() throws Exception {
        // Always false.
        IgnitePredicate<IgniteEvent> p = F.eventNode(null);

        for (int i = 1; i < 100; i++)
            assert !p.apply(new TestEvent(i));

        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);
        Ignite g3 = startGrid(3);

        try {
            IgniteEvent evt1 = new TestEvent(g1.cluster().localNode());
            IgniteEvent evt2 = new TestEvent(g3.cluster().localNode());
            IgniteEvent evt3 = new TestEvent(g1.cluster().localNode());
            IgniteEvent evt4 = new TestEvent(g2.cluster().localNode());

            Collection<ClusterNode> nodes = Arrays.asList(g1.cluster().localNode(), g3.cluster().localNode());

            p = F.eventNode(nodes);

            assert p.apply(evt1);
            assert p.apply(evt2);
            assert p.apply(evt3);
            assert !p.apply(evt4);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
            stopGrid(3);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullArgumentToVariableArgMethod"})
    public void testEventNode2() throws Exception {
        final Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        IgniteEvent evt1 = new TestEvent(g1.cluster().localNode());
        IgniteEvent evt2 = new TestEvent(g1.cluster().localNode());
        IgniteEvent evt3 = new TestEvent(g1.cluster().localNode());
        IgniteEvent evt4 = new TestEvent(g2.cluster().localNode());

        try {
            IgnitePredicate<IgniteEvent> p = F.eventNode(getTestGridName(1), null);

            assert p != null;

            assert p.apply(evt1);
            assert p.apply(evt2);
            assert p.apply(evt3);
            assert p.apply(evt4);

            p = F.eventNode(getTestGridName(1), new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    return n != null && n.id().equals(g1.cluster().localNode().id());
                }
            });

            assert p.apply(evt1);
            assert p.apply(evt2);
            assert p.apply(evt3);
            assert !p.apply(evt4);

            p = F.eventNode(getTestGridName(1), F.<ClusterNode>alwaysFalse());

            assert !p.apply(evt1);
            assert !p.apply(evt2);
            assert !p.apply(evt3);
            assert !p.apply(evt4);

            try {
                F.eventNode(null, null);
            }
            catch (GridRuntimeException e) {
                info("Caught expected exception: " + e);
            }

        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * JUnit.
     */
    public void testEventNodeId() {
        // Always false.
        IgnitePredicate<IgniteEvent> p = F.eventNodeId();

        for (int i = 1; i < 100; i++)
            assert !p.apply(new TestEvent(i));

        UUID[] ids = null;

        // Always false.
        p = F.eventNodeId(ids);

        for (int i = 1; i < 100; i++)
            assert !p.apply(new TestEvent(i));

        IgniteEvent evt1 = new TestEvent(1);
        IgniteEvent evt2 = new TestEvent(2);
        IgniteEvent evt3 = new TestEvent(3);

        p = F.eventNodeId(evt1.node().id());

        assert p.apply(evt1);
        assert !p.apply(evt2);
        assert !p.apply(evt3);
    }

    /**
     *
     */
    public void testConcatSingleAndCollection() {
        String one = "1";
        String two = "2";
        String three = "3";
        String four = "4";

        List<String> list = F.asList(two, three, four);

        // Read-only view.
        Collection<String> joinedReadOnly = F.concat(false, one, list);

        assertEquals(4, joinedReadOnly.size());

        String match = one;

        for (String s : joinedReadOnly) {
            assertEquals(match, s);

            match = Integer.toString(Integer.valueOf(match) + 1);
        }

        Collection<String> joinedCp = F.concat(true, one, list);

        assertEquals(joinedReadOnly, joinedCp);

        // Make sure copy is writable.
        joinedCp.remove(one);
        joinedCp.add(one);
    }

    /**
     *
     */
    public void testConcatCollections() {
        String one = "1";
        String two = "2";
        String three = "3";
        String four = "4";

        List<String> l1 = F.asList(one, two);
        List<String> l2 = F.asList(three, four);

        // Read-only view.
        Collection<String> joinedReadOnly = F.concat(false, l1, l2);

        assertEquals(4, joinedReadOnly.size());

        String match = one;

        for (String s : joinedReadOnly) {
            assertEquals(match, s);

            match = Integer.toString(Integer.valueOf(match) + 1);
        }

        Collection<String> joinedCp = F.concat(true, l1, l2);

        assertEquals(joinedReadOnly, joinedCp);

        // Make sure copy is writable.
        joinedCp.remove(one);
        joinedCp.add(one);
    }

    /**
     *
     */
    public void testStringConcat() {
        List<String> strs = Arrays.asList("a", "b", "c");

        assert "a|b|c".equals(F.concat(strs, "|"));

        assert "abc".equals(F.concat(strs, null));

        IgniteReducer<String, String> r = F.concatReducer("|");

        for (String s : strs)
            r.collect(s);

        assert "a|b|c".equals(r.reduce());
    }

    /**
     *
     */
    public void testViewWithNulls() {
        List<String> strs = Arrays.asList(null, "b", null, null);

        for (String str : F.view(strs, F.notNull()))
            assert str != null;
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testAwaitAll() throws Exception {
        final GridFutureAdapter[] futs = {
            new GridFutureAdapter<>(), new GridFutureAdapter<>(), new GridFutureAdapter<>()
        };

        for (IgniteFuture fut : futs) {
            assert !fut.isDone();
        }

        new Thread() {
            @Override public void run() {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                for (GridFutureAdapter fut : futs) {
                    fut.onDone();
                }
            }
        }.start();

        F.<Object>awaitAll(futs);

        for (IgniteFuture fut : futs) {
            assert fut.isDone();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testAwaitOne() throws Exception {
        final GridFutureAdapter<?>[] futs = {new GridFutureAdapter(), new GridFutureAdapter(), new GridFutureAdapter()};

        for (IgniteFuture fut : futs) {
            assert !fut.isDone();
        }

        new Thread() {
            @Override public void run() {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                F.rand(futs).onDone();
            }
        }.start();

        IgniteFuture doneFut = F.awaitOne((IgniteFuture[])futs);

        assert doneFut.isDone();

        for (IgniteFuture fut : futs) {
            assert doneFut == fut ? fut.isDone() : !fut.isDone();
        }

        // Check only NULLs.
        IgniteFuture<Object> fut = F.awaitOne(Arrays.asList((IgniteFuture<Object>)null, null, null));

        assert fut.isDone();
    }
}
