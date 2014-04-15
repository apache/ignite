/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify;

import org.gridgain.grid.compute.gridify.*;

import java.util.*;

/**
 * Test set-to-set target interface.
 */
public interface GridifySetToSetTargetInterface {
    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    public Collection<Long> findPrimes(Collection<Long> input);

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2)
    public Collection<Long> findPrimesWithoutSplitSize(Collection<Long> input);

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget")
    public Collection<Long> findPrimesWithoutSplitSizeAndThreshold(Collection<Long> input);

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget")
    public Collection<Long> findPrimesInListWithoutSplitSizeAndThreshold(List<Long> input);

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @SuppressWarnings({"CollectionDeclaredAsConcreteClass"})
    @GridifySetToSet(gridName = "GridifySetToSetTarget")
    public Collection<Long> findPrimesInArrayListWithoutSplitSizeAndThreshold(ArrayList<Long> input);

    /**
     * Find prime numbers in array.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    public Long[] findPrimesInArray(Long[] input);

    /**
     * Find prime numbers in primitive array.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    public long[] findPrimesInPrimitiveArray(long[] input);

    /**
     * Find prime numbers in iterator.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    public Iterator<Long> findPrimesWithIterator(Iterator<Long> input);

    /**
     * Find prime numbers in enumeration.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    public Enumeration<Long> findPrimesWithEnumeration(Enumeration<Long> input);
}
