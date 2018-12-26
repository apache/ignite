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

package org.apache.ignite.ml.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteException;

/**
 * Class with various utility methods.
 */
public class Utils {
    /**
     * Perform deep copy of an object.
     *
     * @param orig Original object.
     * @param <T> Class of original object;
     * @return Deep copy of original object.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> T copy(T orig) {
        Object obj;

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);

            out.writeObject(orig);
            out.flush();
            out.close();

            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));

            obj = in.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteException("Couldn't copy the object.", e);
        }

        return (T)obj;
    }

    /**
     * Select k distinct integers from range [0, n) with reservoir sampling:
     * https://en.wikipedia.org/wiki/Reservoir_sampling.
     *
     * @param n Number specifying left end of range of integers to pick values from.
     * @param k Count specifying how many integers should be picked.
     * @param rand RNG.
     * @return Array containing k distinct integers from range [0, n);
     */
    public static int[] selectKDistinct(int n, int k, Random rand) {
        int i;
        Random r = rand != null ? rand : new Random();

        int res[] = new int[k];
        for (i = 0; i < k; i++)
            res[i] = i;

        for (; i < n; i++) {
            int j = r.nextInt(i + 1);

            if (j < k)
                res[j] = i;
        }

        return res;
    }

    /**
     * Select k distinct integers from range [0, n) with reservoir sampling:
     * https://en.wikipedia.org/wiki/Reservoir_sampling.
     * Equivalent to {@code selectKDistinct(n, k, new Random())}.
     *
     * @param n Number specifying left end of range of integers to pick values from.
     * @param k Count specifying how many integers should be picked.
     * @return Array containing k distinct integers from range [0, n);
     */
    public static int[] selectKDistinct(int n, int k) {
        return selectKDistinct(n, k, new Random());
    }

    /**
     * Convert given iterator to a stream with known count of entries.
     *
     * @param iter Iterator.
     * @param cnt Count.
     * @param <T> Type of entries.
     * @return Stream constructed from iterator.
     */
    public static <T> Stream<T> asStream(Iterator<T> iter, long cnt) {
        return StreamSupport.stream(
                Spliterators.spliterator(iter, cnt, Spliterator.ORDERED),
                false);
    }

    /**
     * Convert given iterator to a stream.
     *
     * @param iter Iterator.
     * @param <T> Iterator content type.
     * @return Stream constructed from iterator.
     */
    public static <T> Stream<T> asStream(Iterator<T> iter) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED),
                false);
    }

    /**
     * Zips two streams (in functional sense of zipping) i.e. returns stream consisting
     * of results of applying zipper to corresponding entries of two stream.
     *
     * @param a First stream.
     * @param b Second stream.
     * @param zipper Bi-function combining two streams.
     * @param <A> Type of first stream entries.
     * @param <B> Type of secong stream entries.
     * @param <C> Type of zipper output.
     * @return Two streams zipped together.
     */
    public static<A, B, C> Stream<C> zip(Stream<? extends A> a,
        Stream<? extends B> b,
        BiFunction<? super A, ? super B, ? extends C> zipper) {
        Objects.requireNonNull(zipper);
        Spliterator<? extends A> aSpliterator = Objects.requireNonNull(a).spliterator();
        Spliterator<? extends B> bSpliterator = Objects.requireNonNull(b).spliterator();

        // Zipping looses DISTINCT and SORTED characteristics
        int characteristics = aSpliterator.characteristics() & bSpliterator.characteristics() &
            ~(Spliterator.DISTINCT | Spliterator.SORTED);

        long zipSize = ((characteristics & Spliterator.SIZED) != 0)
            ? Math.min(aSpliterator.getExactSizeIfKnown(), bSpliterator.getExactSizeIfKnown())
            : -1;

        Iterator<A> aIterator = Spliterators.iterator(aSpliterator);
        Iterator<B> bIterator = Spliterators.iterator(bSpliterator);
        Iterator<C> cIterator = new Iterator<C>() {
            @Override public boolean hasNext() {
                return aIterator.hasNext() && bIterator.hasNext();
            }

            @Override public C next() {
                return zipper.apply(aIterator.next(), bIterator.next());
            }
        };

        Spliterator<C> split = Spliterators.spliterator(cIterator, zipSize, characteristics);
        return (a.isParallel() || b.isParallel())
            ? StreamSupport.stream(split, true)
            : StreamSupport.stream(split, false);
    }

}
