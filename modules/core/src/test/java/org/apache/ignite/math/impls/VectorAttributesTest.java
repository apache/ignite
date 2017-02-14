package org.apache.ignite.math.impls;

import org.apache.ignite.math.StorageOpsKinds;
import org.apache.ignite.math.Vector;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.*;

import static org.junit.Assert.*;

/** */
public class VectorAttributesTest {
    // todo add attributes tests for offheap vector

    /** */ @Test
    public void defaultConstructorTest() {
        DenseLocalOnHeapVector v = new DenseLocalOnHeapVector();

        boolean expECaught = false;

        try {
            assertTrue(v.isDense());
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        expECaught = false;

        try {
            assertTrue(v.isSequentialAccess());
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        expECaught = false;

        try {
            assertTrue(v.getLookupCost() == 0);
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        expECaught = false;

        try {
            assertTrue(v.isAddConstantTime());
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        assertNull(v.clusterGroup());

        assertNotNull(v.guid());
    }

    /** */ @Test
    public void nullOffHeapTest() {
        DenseLocalOffHeapVector v = new DenseLocalOffHeapVector((double[])null);

        boolean expECaught = false;

        try {
            assertTrue(v.isDense());
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        expECaught = false;

        try {
            assertTrue(v.isSequentialAccess());
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        expECaught = false;

        try {
            assertTrue(v.getLookupCost() == 0);
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        expECaught = false;

        try {
            assertTrue(v.isAddConstantTime());
        } catch (org.apache.ignite.math.UnsupportedOperationException uoe) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        assertNull(v.clusterGroup());

        assertNotNull(v.guid());
    }

    /** */ @Test
    public void isDenseTest() {
        alwaysTrueAttributeTest(StorageOpsKinds::isDense);
    }

    /** */ @Test
    public void isSequentialAccessTest() {
        alwaysTrueAttributeTest(StorageOpsKinds::isSequentialAccess);
    }

    /** */ @Test
    public void getLookupCostTest() {
        alwaysTrueAttributeTest(v -> v.getLookupCost() == 0);
    }

    /** */ @Test
    public void isAddConstantTimeTest() {
        alwaysTrueAttributeTest(StorageOpsKinds::isAddConstantTime);
    }

    /** */ @Test
    public void clusterGroupTest() {
        alwaysTrueAttributeTest(v -> v.clusterGroup() == null);
    }

    /** */ @Test
    public void guidTest() {
        alwaysTrueAttributeTest(v -> v.guid() != null);
    }

    /** */
    private void alwaysTrueAttributeTest(Predicate<Vector> pred) {
        boolean expECaught = false;

        try {
            assertTrue("Null map args.",
                pred.test(new DenseLocalOnHeapVector((Map<String, Object>)null)));
        } catch (AssertionError e) {
            expECaught = true;
        }

        assertTrue("Default constructor expect exception at this predicate.", expECaught);

        assertTrue("Size from args.",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{ put("size", 99); }})));

        final double[] test = new double[99];

        assertTrue("Size from array in args.",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("copy", false);
            }})));

        assertTrue("Size from array in args, shallow copy.",
            pred.test(new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("copy", true);
            }})));

        assertTrue("Null array shallow copy.",
            pred.test(new DenseLocalOnHeapVector(null, true)));

        assertTrue("0 size shallow copy.",
            pred.test(new DenseLocalOnHeapVector(new double[0], true)));

        assertTrue("0 size.",
            pred.test(new DenseLocalOnHeapVector(new double[0], false)));

        assertTrue("1 size shallow copy.",
            pred.test(new DenseLocalOnHeapVector(new double[1], true)));

        assertTrue("1 size.",
            pred.test(new DenseLocalOnHeapVector(new double[1], false)));

        assertTrue("0 size default copy.",
            pred.test(new DenseLocalOnHeapVector(new double[0])));

        assertTrue("1 size default copy.",
            pred.test(new DenseLocalOnHeapVector(new double[1])));

        // todo test below

        assertTrue("Size from args, off heap vector.",
            pred.test(new DenseLocalOffHeapVector(new HashMap<String, Object>(){{ put("size", 99); }})));

        assertTrue("Size from array in args, off heap vector.",
            pred.test(new DenseLocalOffHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("copy", false);
            }})));

        assertTrue("Size from array in args, shallow copy, off heap vector.",
            pred.test(new DenseLocalOffHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("copy", true);
            }})));

        assertTrue("0 size, off heap vector.",
            pred.test(new DenseLocalOffHeapVector(new double[0])));

        assertTrue("1 size, off heap vector.",
            pred.test(new DenseLocalOffHeapVector(new double[1])));

    }
}
