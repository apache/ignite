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
 * Test set-to-value target interface.
 */
public interface GridifySetToValueTargetInterface {
    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    public Long findMaximum(Collection<Long> input);

    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget")
    public Long findMaximumInList(List<Long> input);

    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget", threshold = 2)
    public Long findMaximumWithoutSplitSize(Collection<Long> input);

    /**
     * Find maximum value in collection.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget")
    public Long findMaximumWithoutSplitSizeAndThreshold(Collection<Long> input);

    /**
     * Find maximum in array.
     *
     * @param input Input array.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    public Long findPrimesInArray(Long[] input);

    /**
     * Find maximum in primitive array.
     *
     * @param input Input array.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    public long findMaximumInPrimitiveArray(long[] input);

    /**
     * Find maximum value in Iterator.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    public long findMaximumInIterator(Iterator<Long> input);


    /**
     * Find maximum value in Enumeration.
     *
     * @param input Input collection.
     * @return Maximum value.
     */
    @GridifySetToValue(gridName = "GridifySetToValueTarget", threshold = 2, splitSize = 2)
    public long findMaximumInEnumeration(Enumeration<Long> input);
}
