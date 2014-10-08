/*
 Copyright (C) Roman Levenstein. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.romix.scala.collection.concurrent;

import com.romix.scala.*;

import java.util.*;
import java.util.Map.*;

/**
 * Mimic immutable ListMap in Scala
 *
 * @param <V>
 * @author Roman Levenstein <romixlev@gmail.com>
 */
abstract class ListMap<K, V> {

    ListMap<K, V> next;

    static <K, V> ListMap<K, V> map(K k, V v, ListMap<K, V> tail) {
        return new Node<K, V>(k, v, tail);
    }

    static <K, V> ListMap<K, V> map(K k, V v) {
        return new Node<K, V>(k, v, null);
    }

    static <K, V> ListMap<K, V> map(K k1, V v1, K k2, V v2) {
        return new Node<K, V>(k1, v1, new Node<K, V>(k2, v2, null));
    }

    public abstract int size();

    public abstract boolean isEmpty();

    abstract public boolean contains(K k, V v);

    abstract public boolean contains(K key);

    abstract public Option<V> get(K key);

    abstract public ListMap<K, V> add(K key, V value);

    abstract public ListMap<K, V> remove(K key);

    abstract public Iterator<Entry<K, V>> iterator();


    static class EmptyListMap<K, V> extends ListMap<K, V> {
        public ListMap<K, V> add(K key, V value) {
            return ListMap.map(key, value, null);
        }

        public boolean contains(K k, V v) {
            return false;
        }

        public boolean contains(K k) {
            return false;
        }

        public ListMap<K, V> remove(K key) {
            return this;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Option<V> get(K key) {
            return Option.makeOption(null);
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EmptyListMapIterator<K, V>();
        }

        static class EmptyListMapIterator<K, V> implements Iterator<Entry<K, V>> {

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Entry<K, V> next() {
                return null;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Operation not supported");
            }

        }
    }

    static class Node<K, V> extends ListMap<K, V> {
        final K k;
        final V v;

        Node(K k, V v, ListMap<K, V> next) {
            this.k = k;
            this.v = v;
            this.next = next;
        }

        public ListMap<K, V> add(K key, V value) {
            return ListMap.map(key, value, remove(key));
        }

        public boolean contains(K k, V v) {
            if (k.equals(this.k) && v.equals(this.v))
                return true;
            else if (next != null)
                return next.contains(k, v);
            return false;
        }

        public boolean contains(K k) {
            if (k.equals(this.k))
                return true;
            else if (next != null)
                return next.contains(k);
            return false;
        }

        public ListMap<K, V> remove(K key) {
            if (!contains(key))
                return this;
            else
                return remove0(key);
        }

        private ListMap<K, V> remove0(K key) {
            ListMap<K, V> n = this;
            ListMap<K, V> newN = null;
            ListMap<K, V> lastN = null;
            while (n != null) {
                if (n instanceof EmptyListMap) {
                    newN.next = n;
                    break;
                }
                Node<K, V> nn = (Node<K, V>) n;
                if (key.equals(nn.k)) {
                    n = n.next;
                    continue;
                }
                else {
                    if (newN != null) {
                        lastN.next = ListMap.map(nn.k, nn.v, null);
                        lastN = lastN.next;
                    }
                    else {
                        newN = ListMap.map(nn.k, nn.v, null);
                        lastN = newN;
                    }
                }
                n = n.next;
            }
            return newN;
        }

        @Override
        public int size() {
            if (next == null)
                return 1;
            else
                return 1 + next.size();
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public Option<V> get(K key) {
            if (key.equals(k))
                return Option.makeOption(v);
            if (next != null)
                return next.get(key);
            return Option.makeOption(null);
        }


        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new NodeIterator<K, V>(this);
        }

        static class NodeIterator<K, V> implements Iterator<Entry<K, V>> {
            ListMap<K, V> n;

            public NodeIterator(Node<K, V> n) {
                this.n = n;
            }

            @Override
            public boolean hasNext() {
//                return n!= null && n.next != null && !(n.next instanceof EmptyListMap);
                return n != null && !(n instanceof EmptyListMap);
            }

            @Override
            public Entry<K, V> next() {
                if (n instanceof Node) {
                    Node<K, V> nn = (Node<K, V>) n;
                    Pair<K, V> res = new Pair<K, V>(nn.k, nn.v);
                    n = n.next;
                    return res;
                }
                else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new RuntimeException("Operation not supported");
            }

        }
    }
}
