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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.compute.gridify.GridifySetToSet;

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