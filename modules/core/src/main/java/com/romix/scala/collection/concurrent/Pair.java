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

import java.util.*;

/**
 * Helper class simulating a tuple of 2 elements in Scala
 *
 * @param <K>
 * @param <V>
 * @author Roman Levenstein <romixlev@gmail.com>
 */
public class Pair<K, V> implements Map.Entry<K, V> {

    final K k;
    final V v;

    Pair(K k, V v) {
        this.k = k;
        this.v = v;
    }

    @Override
    public K getKey() {
        // TODO Auto-generated method stub
        return k;
    }

    @Override
    public V getValue() {
        // TODO Auto-generated method stub
        return v;
    }

    @Override
    public V setValue(V value) {
        throw new RuntimeException("Operation not supported");
    }

}
