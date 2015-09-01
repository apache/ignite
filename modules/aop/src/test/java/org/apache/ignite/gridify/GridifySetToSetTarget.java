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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.compute.gridify.GridifySetToSet;

/**
 * Test set-to-set target.
 */
public class GridifySetToSetTarget implements GridifySetToSetTargetInterface, Serializable {
    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    @Override public Collection<Long> findPrimes(Collection<Long> input) {
        return findPrimes0(input);
    }

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2)
    @Override public Collection<Long> findPrimesWithoutSplitSize(Collection<Long> input) {
        return findPrimes0(input);
    }

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget")
    @Override public Collection<Long> findPrimesWithoutSplitSizeAndThreshold(Collection<Long> input) {
        return findPrimes0(input);
    }

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget")
    @Override public Collection<Long> findPrimesInListWithoutSplitSizeAndThreshold(List<Long> input) {
        return findPrimes0(input);
    }

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @SuppressWarnings({"CollectionDeclaredAsConcreteClass"})
    @GridifySetToSet(gridName = "GridifySetToSetTarget")
    @Override public Collection<Long> findPrimesInArrayListWithoutSplitSizeAndThreshold(ArrayList<Long> input) {
        return findPrimes0(input);
    }

    /**
     * Find prime numbers in array.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    @Override public Long[] findPrimesInArray(Long[] input) {
        return findPrimesInArray0(input);
    }

    /**
     * Find prime numbers in primitive array.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    @Override public long[] findPrimesInPrimitiveArray(long[] input) {
        return findPrimesInPrimitiveArray0(input);
    }

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    private Collection<Long> findPrimes0(Iterable<Long> input) {
        System.out.println(">>>");
        System.out.println("Find primes in: " + input);
        System.out.println(">>>");

        Collection<Long> res = new ArrayList<>();

        for (Long val : input) {
            Long divisor = checkPrime(val, 2, val);

            if (divisor == null)
                res.add(val);
        }

        return res;
    }

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    private Long[] findPrimesInArray0(Long[] input) {
        System.out.println(">>>");
        System.out.println("Find primes in array: " + Arrays.asList(input));
        System.out.println(">>>");

        Collection<Long> res = new ArrayList<>();

        for (Long val : input) {
            Long divisor = checkPrime(val, 2, val);

            if (divisor == null)
                res.add(val);
        }

        return res.toArray(new Long[res.size()]);
    }

    /**
     * Find prime numbers in collection.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    private long[] findPrimesInPrimitiveArray0(long[] input) {
        System.out.println(">>>");
        System.out.println("Find primes in primitive array: " + Arrays.toString(input));
        System.out.println(">>>");

        Collection<Long> res = new ArrayList<>();

        for (Long val : input) {
            Long divisor = checkPrime(val, 2, val);

            if (divisor == null)
                res.add(val);
        }

        long[] arr = new long[res.size()];

        int i = 0;

        for (Long element : res) {
            arr[i] = element;
            i++;
        }

        return arr;
    }

    /**
     * Find prime numbers in iterator.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    @Override public Iterator<Long> findPrimesWithIterator(Iterator<Long> input) {
        System.out.println(">>>");
        System.out.println("Find primes in iterator: " + input);
        System.out.println(">>>");

        Collection<Long> res = new ArrayList<>();


        while (input.hasNext()) {
            Long val = input.next();

            Long divisor = checkPrime(val, 2, val);

            if (divisor == null)
                res.add(val);
        }

        return new MathIteratorAdapter<>(res);
    }

    /**
     * Find prime numbers in enumeration.
     *
     * @param input Input collection.
     * @return Prime numbers.
     */
    @GridifySetToSet(gridName = "GridifySetToSetTarget", threshold = 2, splitSize = 2)
    @Override public Enumeration<Long> findPrimesWithEnumeration(Enumeration<Long> input) {
        System.out.println(">>>");
        System.out.println("Find primes in enumeration: " + input);
        System.out.println(">>>");

        Collection<Long> res = new ArrayList<>();

        while (input.hasMoreElements()) {
            Long val = input.nextElement();

            Long divisor = checkPrime(val, 2, val);

            if (divisor == null)
                res.add(val);
        }

        return new MathEnumerationAdapter<>(res);
    }

    /**
     * Method to check value for prime.
     * Returns first divisor found or {@code null} if no divisor was found.
     *
     * @param val Value to check for prime.
     * @param minRage Lower boundary of divisors range.
     * @param maxRange Upper boundary of divisors range.
     * @return First divisor found or {@code null} if no divisor was found.
     */
    private Long checkPrime(long val, long minRage, long maxRange) {
        // Loop through all divisors in the range and check if the value passed
        // in is divisible by any of these divisors.
        // Note that we also check for thread interruption which may happen
        // if the job was cancelled from the grid task.
        for (long divisor = minRage; divisor <= maxRange && !Thread.currentThread().isInterrupted(); divisor++) {
            if (divisor != 1 && divisor != val && val % divisor == 0)
                return divisor;
        }

        return null;
    }

    /**
     * Serializable {@link Enumeration} implementation based on {@link Collection}.
     */
    private static class MathEnumerationAdapter<T> implements Enumeration<T>, Serializable {
        /** */
        private Collection<T> col;

        /** */
        private transient Iterator<T> iter;

        /**
         * Creates enumeration.
         *
         * @param col Collection.
         */
        private MathEnumerationAdapter(Collection<T> col) {
            this.col = col;

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

        /**
         * Recreate inner state for object after deserialization.
         *
         * @param in Input stream.
         * @throws ClassNotFoundException Thrown in case of error.
         * @throws IOException Thrown in case of error.
         */
        private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
            // Always perform the default de-serialization first.
            in.defaultReadObject();

            iter = col.iterator();
        }

        /**
         * @param out Output stream
         * @throws IOException Thrown in case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            // Perform the default serialization for all non-transient, non-static fields.
            out.defaultWriteObject();
        }
    }

    /**
     * Serializable {@link Iterator} implementation based on {@link Collection}.
     */
    private static class MathIteratorAdapter<T> implements Iterator<T>, Serializable {
        /** */
        private Collection<T> col;

        /** */
        private transient Iterator<T> iter;

        /**
         * @param col Collection.
         */
        MathIteratorAdapter(Collection<T> col) {
            this.col = col;

            iter = col.iterator();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T next() {
            return iter.next();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            iter.remove();
        }

        /**
         * Recreate inner state for object after deserialization.
         *
         * @param in Input stream.
         * @throws ClassNotFoundException Thrown in case of error.
         * @throws IOException Thrown in case of error.
         */
        private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
            // Always perform the default de-serialization first.
            in.defaultReadObject();

            iter = col.iterator();
        }

        /**
         * @param out Output stream
         * @throws IOException Thrown in case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            // Perform the default serialization for all non-transient, non-static fields.
            out.defaultWriteObject();
        }
    }
}