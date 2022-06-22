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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Client Ignite Set.
 */
class ClientIgniteSetImpl<T> implements ClientIgniteSet<T> {
    /** */
    private final String name;

    /** */
    private final IgniteUuid id;

    /** */
    private final ReliableChannel ch;

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param name Name.
     * @param id Id.
     */
    public ClientIgniteSetImpl(ReliableChannel ch, String name, IgniteUuid id) {
        assert ch != null;
        assert name != null;
        assert id != null;

        this.ch = ch;
        this.name = name;
        this.id = id;
    }

    @Override
    public boolean add(T t) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Iterator<T> iterator() {
        return null;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean collocated() {
        return false;
    }

    @Override
    public boolean removed() {
        return false;
    }

    @Override
    public void affinityRun(IgniteRunnable job) {

    }

    @Override
    public <R> R affinityCall(IgniteCallable<R> job) {
        return null;
    }
}
