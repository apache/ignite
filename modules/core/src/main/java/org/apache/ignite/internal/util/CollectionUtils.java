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

package org.apache.ignite.internal.util;

import static java.util.Collections.addAll;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class provides various method to work with collections.
 */
public final class CollectionUtils {
    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param col Collection to check.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Collection<?> col) {
        return col == null || col.isEmpty();
    }
    
    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param c Collection to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Iterable<?> c) {
        return c == null || (c instanceof Collection<?> ? ((Collection<?>) c).isEmpty() : !c.iterator().hasNext());
    }
    
    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param col Collection to check.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Map<?, ?> col) {
        return col == null || col.isEmpty();
    }
    
    /**
     * Gets first element from given list or returns {@code null} if list is empty.
     *
     * @param list List to retrieve the first element.
     * @param <T>  Type of the elements of the list.
     * @return The first element of the given list or {@code null} in case the list is empty.
     */
    public static <T> T first(List<? extends T> list) {
        if (nullOrEmpty(list)) {
            return null;
        }
        
        return list.get(0);
    }
    
    /**
     * Union set and items.
     *
     * @param set Set.
     * @param ts  Items.
     * @param <T> Type of the elements of set and items..
     * @return Immutable union of set and items.
     */
    @SafeVarargs
    public static <T> Set<T> union(@Nullable Set<T> set, @Nullable T... ts) {
        if (nullOrEmpty(set)) {
            return ts == null || ts.length == 0 ? Set.of() : Set.of(ts);
        }
    
        if (ts == null || ts.length == 0) {
            return unmodifiableSet(set);
        }
        
        Set<T> res = new HashSet<>(set);
        
        addAll(res, ts);
        
        return unmodifiableSet(res);
    }
    
    /**
     * Union collections.
     *
     * @param collections Collections.
     * @param <T>         Type of the elements of collections.
     * @return Immutable union of collections.
     */
    @SafeVarargs
    public static <T> Collection<T> union(Collection<T>... collections) {
        if (collections == null || collections.length == 0) {
            return List.of();
        }
        
        return new AbstractCollection<>() {
            /** Total size of the collections. */
            int size = -1;
            
            /** {@inheritDoc} */
            @Override
            public Iterator<T> iterator() {
                return concat(collections).iterator();
            }
            
            /** {@inheritDoc} */
            @Override
            public int size() {
                if (size == -1) {
                    int s = 0;
                    
                    for (Collection<T> collection : collections) {
                        s += collection.size();
                    }
                    
                    size = s;
                }
                
                return size;
            }
        };
    }
    
    /**
     * Create a lazy concatenation of iterables.
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param iterables Iterables.
     * @param <T>       Type of the elements.
     * @return Concatenation of iterables.
     */
    @SafeVarargs
    public static <T> Iterable<T> concat(@Nullable Iterable<? extends T>... iterables) {
        if (iterables == null || iterables.length == 0) {
            return Collections::emptyIterator;
        }
        
        return () -> new Iterator<>() {
            /** Current index at {@code iterables}. */
            int idx = 0;
            
            /** Current iterator. */
            Iterator<? extends T> curr = emptyIterator();
            
            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                while (!curr.hasNext() && idx < iterables.length) {
                    curr = iterables[idx++].iterator();
                }
                
                return curr.hasNext();
            }
            
            /** {@inheritDoc} */
            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    return curr.next();
                }
            }
        };
    }
    
    /**
     * Create a collection view that can only be read.
     *
     * @param collection Basic collection.
     * @param mapper     Conversion function.
     * @param <T1>       Base type of the collection.
     * @param <T2>       Type for view.
     * @return Read-only collection view.
     */
    public static <T1, T2> Collection<T2> viewReadOnly(
            @Nullable Collection<? extends T1> collection,
            @Nullable Function<? super T1, ? extends T2> mapper
    ) {
        if (nullOrEmpty(collection)) {
            return emptyList();
        }
    
        if (mapper == null) {
            return unmodifiableCollection((Collection<T2>) collection);
        }
        
        return new AbstractCollection<>() {
            /** {@inheritDoc} */
            @Override
            public Iterator<T2> iterator() {
                Iterator<? extends T1> iterator = collection.iterator();
                
                return new Iterator<>() {
                    /** {@inheritDoc} */
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                    
                    /** {@inheritDoc} */
                    @Override
                    public T2 next() {
                        return mapper.apply(iterator.next());
                    }
                };
            }
            
            /** {@inheritDoc} */
            @Override
            public int size() {
                return collection.size();
            }
            
            /** {@inheritDoc} */
            @Override
            public boolean isEmpty() {
                return collection.isEmpty();
            }
        };
    }
    
    /**
     * Difference of two sets.
     *
     * @param a   First set.
     * @param b   Second set.
     * @param <T> Type of the elements.
     * @return Immutable set of elements of the first without the second.
     */
    public static <T> Set<T> difference(@Nullable Set<T> a, @Nullable Set<T> b) {
        if (nullOrEmpty(a)) {
            return Set.of();
        } else if (nullOrEmpty(b)) {
            return unmodifiableSet(a);
        }
        
        // Lazy initialization.
        Set<T> res = null;
        
        for (T t : a) {
            if (!b.contains(t)) {
                if (res == null) {
                    res = new HashSet<>();
                }
                
                res.add(t);
            }
        }
        
        return res == null ? Set.of() : unmodifiableSet(res);
    }
    
    /** Stub. */
    private CollectionUtils() {
        // No op.
    }
}
