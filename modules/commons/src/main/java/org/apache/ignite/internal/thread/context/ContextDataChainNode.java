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

package org.apache.ignite.internal.thread.context;

/** */
public abstract class ContextDataChainNode<T> {
    /** */
    private final int storedAttrIdBits;

    /** */
    private final T prev;

    /** */
    protected ContextDataChainNode() {
        storedAttrIdBits = 0;
        prev = null;
    }

    /** */
    protected ContextDataChainNode(int storedAttrIdBits, T prev) {
        this.storedAttrIdBits = storedAttrIdBits;
        this.prev = prev;
    }

    /** */
    abstract boolean isEmpty();

    /** */
    int storedAttributeIdBits() {
        return storedAttrIdBits;
    }

    /** */
    boolean containsValueFor(ContextAttribute<?> attr) {
        return (storedAttrIdBits & attr.bitmask()) != 0;
    }

    /** */
    T previous() {
        assert !isEmpty();

        return prev;
    }
}
