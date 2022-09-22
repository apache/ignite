package de.kp.works.ignite.gremlin;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public final class CloseableIteratorUtils {

    private CloseableIteratorUtils() {
    }

    public static <S> Iterator<S> limit(final Iterator<S> iterator, final int limit) {
        return new CloseableIterator<S>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return iterator.hasNext() && this.count < limit;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                if (this.count++ >= limit)
                    throw FastNoSuchElementException.instance();
                return iterator.next();
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
    }

    public static <S, E> Iterator<E> map(final Iterator<S> iterator, final Function<S, E> function) {
        return new CloseableIterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                return function.apply(iterator.next());
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
    }

    public static <S> Iterator<S> filter(final Iterator<S> iterator, final Predicate<S> predicate) {
        return new CloseableIterator<S>() {
            S nextResult = null;

            @Override
            public boolean hasNext() {
                if (null != this.nextResult) {
                    return true;
                } else {
                    advance();
                    return null != this.nextResult;
                }
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                try {
                    if (null != this.nextResult) {
                        return this.nextResult;
                    } else {
                        advance();
                        if (null != this.nextResult)
                            return this.nextResult;
                        else
                            throw FastNoSuchElementException.instance();
                    }
                } finally {
                    this.nextResult = null;
                }
            }

            private void advance() {
                this.nextResult = null;
                while (iterator.hasNext()) {
                    final S s = iterator.next();
                    if (predicate.test(s)) {
                        this.nextResult = s;
                        return;
                    }
                }
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
    }

    public static <S, E> Iterator<E> flatMap(final Iterator<S> iterator, final Function<S, Iterator<E>> function) {
        return new CloseableIterator<E>() {

            private Iterator<E> currentIterator = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (this.currentIterator.hasNext())
                    return true;
                else {
                    while (iterator.hasNext()) {
                        this.currentIterator = function.apply(iterator.next());
                        if (this.currentIterator.hasNext())
                            return true;
                    }
                }
                return false;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                if (this.hasNext())
                    return this.currentIterator.next();
                else
                    throw FastNoSuchElementException.instance();
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
    }

    @SafeVarargs
    public static <S> Iterator<S> concat(final Iterator<S>... iterators) {
        final MultiIterator<S> iterator = new MultiIterator<>();
        for (final Iterator<S> itty : iterators) {
            iterator.addIterator(itty);
        }
        return iterator;
    }

    static class MultiIterator<T> implements CloseableIterator<T>, Serializable {

        private final List<Iterator<T>> iterators = new ArrayList<>();
        private int current = 0;

        public void addIterator(final Iterator<T> iterator) {
            this.iterators.add(iterator);
        }

        @Override
        public boolean hasNext() {
            if (this.current >= this.iterators.size())
                return false;

            Iterator<T> currentIterator = this.iterators.get(this.current);

            while (true) {
                if (currentIterator.hasNext()) {
                    return true;
                } else {
                    this.current++;
                    if (this.current >= iterators.size())
                        break;
                    currentIterator = iterators.get(this.current);
                }
            }
            return false;
        }

        @Override
        public void remove() {
            this.iterators.get(this.current).remove();
        }

        @Override
        public T next() {
            if (this.iterators.isEmpty()) throw FastNoSuchElementException.instance();

            Iterator<T> currentIterator = iterators.get(this.current);
            while (true) {
                if (currentIterator.hasNext()) {
                    return currentIterator.next();
                } else {
                    this.current++;
                    if (this.current >= iterators.size())
                        break;
                    currentIterator = iterators.get(current);
                }
            }
            throw FastNoSuchElementException.instance();
        }

        public void clear() {
            this.iterators.clear();
            this.current = 0;
        }

        @Override
        public void close() {
            this.iterators.forEach(CloseableIterator::closeIterator);
        }
    }
}
