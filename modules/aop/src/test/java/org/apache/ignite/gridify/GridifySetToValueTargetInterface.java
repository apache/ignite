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

package org.apache.ignite.gridify;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.compute.gridify.GridifySetToValue;

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