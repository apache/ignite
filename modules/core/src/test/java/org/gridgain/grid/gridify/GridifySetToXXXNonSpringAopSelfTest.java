/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify;

import org.gridgain.testframework.junits.common.*;
import java.util.*;

/**
 * To run this test with JBoss AOP make sure of the following:
 *
 * 1. The JVM is started with following parameters to enable jboss online weaving
 *      (replace ${GRIDGAIN_HOME} to you $GRIDGAIN_HOME):
 *      -javaagent:${GRIDGAIN_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      -Djboss.aop.class.path=[path to grid compiled classes (Idea out folder) or path to gridgain.jar]
 *      -Djboss.aop.exclude=org,com -Djboss.aop.include=org.gridgain
 *
 * 2. The following jars should be in a classpath:
 *      ${GRIDGAIN_HOME}libs/javassist-3.x.x.jar
 *      ${GRIDGAIN_HOME}libs/jboss-aop-jdk50-4.0.4.jar
 *      ${GRIDGAIN_HOME}libs/jboss-aspect-library-jdk50-4.0.4.jar
 *      ${GRIDGAIN_HOME}libs/jboss-common-4.2.2.jar
 *      ${GRIDGAIN_HOME}libs/trove-1.0.2.jar
 *
 * To run this test with AspectJ APO make sure of the following:
 *
 * 1. The JVM is started with following parameters for enable AspectJ online weaving
 *      (replace ${GRIDGAIN_HOME} to you $GRIDGAIN_HOME):
 *      -javaagent:${GRIDGAIN_HOME}/libs/aspectjweaver-1.7.2.jar
 *
 * 2. Classpath should contains the ${GRIDGAIN_HOME}/modules/tests/config/aop/aspectj folder.
 */
@GridCommonTest(group="AOP")
public class GridifySetToXXXNonSpringAopSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testGridifySetToSet() throws Exception {
        try {
            startGrid("GridifySetToSetTarget");
            startGrid("GridifySetToSetTarget2");
            startGrid("GridifySetToSetTarget3");

            GridifySetToSetTarget target = new GridifySetToSetTarget();

            Collection<Long> primesInSet = target.findPrimes(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Prime numbers in set '" + primesInSet + "'.");

            primesInSet = target.findPrimesWithoutSplitSize(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Prime numbers w/o splitsize in set '" + primesInSet + "'.");

            primesInSet = target.findPrimesWithoutSplitSizeAndThreshold(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Prime numbers w/o splitsize and threshold in set '" + primesInSet + "'.");

            primesInSet = target.findPrimesInListWithoutSplitSizeAndThreshold(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Prime numbers in list w/o splitsize and threshold in set '" + primesInSet + "'.");

            primesInSet = target.findPrimesInArrayListWithoutSplitSizeAndThreshold(
                new ArrayList<>(Arrays.asList(2L, 3L, 4L, 6L)));

            info(">>> Prime numbers in arraylist w/o splitsize and threshold in set '" + primesInSet + "'.");

            Long[] primesInArr = target.findPrimesInArray(new Long[]{2L, 3L, 4L, 6L});

            info(">>> Prime numbers in array '" + Arrays.asList(primesInArr) + "'.");

            long[] primesInPrimArr = target.findPrimesInPrimitiveArray(new long[]{2L, 3L, 4L, 6L});

            info(">>> Prime numbers in primitive array '" + primesInPrimArr + "'.");

            Iterator<Long> primesInIter = target.findPrimesWithIterator(Arrays.asList(2L, 3L, 4L, 6L).iterator());

            info(">>> Prime numbers in iterator '" + convert(primesInIter) + "'.");

            Enumeration<Long> primesInEnum = target.findPrimesWithEnumeration(
                new MathEnumerationAdapter<>(Arrays.asList(2L, 3L, 4L, 6L)));

            info(">>> Prime numbers in enumeration '" + convert(primesInEnum) + "'.");
        }
        finally {
            stopGrid("GridifySetToSetTarget");
            stopGrid("GridifySetToSetTarget2");
            stopGrid("GridifySetToSetTarget3");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridifySetToValue() throws Exception {
        try {
            startGrid("GridifySetToValueTarget");
            startGrid("GridifySetToValueTarget2");
            startGrid("GridifySetToValueTarget3");

            GridifySetToValueTarget target = new GridifySetToValueTarget();

            Long max = target.findMaximum(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Maximum in collection '" + max + "'.");

            max = target.findMaximumInList(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Maximum in list '" + max + "'.");

            max = target.findMaximumWithoutSplitSize(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Maximum w/o splitsize in collection '" + max + "'.");

            max = target.findMaximumWithoutSplitSizeAndThreshold(Arrays.asList(2L, 3L, 4L, 6L));

            info(">>> Maximum w/o splitsize and threshold in collection '" + max + "'.");

            max = target.findMaximumInIterator(Arrays.asList(2L, 3L, 4L, 6L).iterator());

            info(">>> Maximum in iterator '" + max + "'.");

            max = target.findMaximumInEnumeration(new MathEnumerationAdapter<>(Arrays.asList(2L, 3L, 4L, 6L)));

            info(">>> Maximum in enumeration '" + max + "'.");
        }
        finally {
            stopGrid("GridifySetToValueTarget");
            stopGrid("GridifySetToValueTarget2");
            stopGrid("GridifySetToValueTarget3");
        }
    }

    /**
     * Convert data to collection.
     *
     * @param iter Iterator.
     * @return Collection.
     */
    private <T> Collection<T> convert(Iterator<T> iter) {
        List<T> list = new ArrayList<>();

        while (iter.hasNext()) {
            list.add(iter.next());
        }

        return list;
    }

    /**
     * Convert data to collection.
     *
     * @param iter Iterator.
     * @return Collection.
     */
    private <T> Collection<T> convert(Enumeration<T> iter) {
        List<T> list = new ArrayList<>();

        while (iter.hasMoreElements()) {
            list.add(iter.nextElement());
        }

        return list;
    }

    /**
     * Enumeration adapter.
     */
    private static class MathEnumerationAdapter<T> implements Enumeration<T> {
        /** */
        private Iterator<T> iter;

        /**
         * Creates Enumeration.
         *
         * @param col Input collection.
         */
        MathEnumerationAdapter(Collection<T> col) {
            iter = col.iterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasMoreElements() {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T nextElement() {
            return iter.next();
        }
    }
}
