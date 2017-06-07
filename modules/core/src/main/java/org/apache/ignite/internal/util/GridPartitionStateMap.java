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

import java.io.Externalizable;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;

/**
 * Grid partition state map. States are encoded using bits.
 * <p>
 * Null values are prohibited.
 */
public class GridPartitionStateMap extends AbstractMap<Integer, GridDhtPartitionState> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Required bits to hold all state. Additional zero state is required as well. */
    private static final int BITS = Integer.SIZE -
        Integer.numberOfLeadingZeros(GridDhtPartitionState.values().length + 1);

    /** */
    private final BitSet states;

    /** */
    private int size;

    /** {@inheritDoc} */
    @Override public Set<Entry<Integer, GridDhtPartitionState>> entrySet() {
        return new AbstractSet<Entry<Integer, GridDhtPartitionState>>() {
            @Override public Iterator<Entry<Integer, GridDhtPartitionState>> iterator() {
                final int size = states.length() == 0 ? 0 : (states.length() - 1)/ BITS + 1;

                return new Iterator<Entry<Integer, GridDhtPartitionState>>() {
                    private int next;
                    private int cur;

                    @Override public boolean hasNext() {
                        while(state(next) == null && next < size)
                            next++;

                        return next < size;
                    }

                    @Override public Entry<Integer, GridDhtPartitionState> next() {
                        cur = next;
                        next++;

                        return new Entry<Integer, GridDhtPartitionState>() {
                            int p = cur;

                            @Override public Integer getKey() {
                                return p;
                            }

                            @Override public GridDhtPartitionState getValue() {
                                return state(p);
                            }

                            @Override public GridDhtPartitionState setValue(GridDhtPartitionState val) {
                                return setState(p, val);
                            }
                        };
                    }

                    @Override public void remove() {
                        setState(cur, null);
                    }
                };
            }

            @Override public int size() {
                return GridPartitionStateMap.this.size();
            }
        };
    }

    /**
     * Default constructor.
     */
    public GridPartitionStateMap() {
        states = new BitSet();
    }

    /**
     * @param parts Partitions to hold.
     */
    public GridPartitionStateMap(int parts) {
        states = new BitSet(parts);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionState put(Integer key, GridDhtPartitionState val) {
        assert val != null;

        return setState(key, val);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionState get(Object key) {
        return state((Integer)key);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionState remove(Object key) {
        return setState((Integer)key, null);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return state((Integer)key) != null;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** */
    private GridDhtPartitionState setState(int part, GridDhtPartitionState st) {
        GridDhtPartitionState old = state(part);

        if (old == st)
            return old;

        int off = part * BITS;

        int ist = st == null ? 0 : st.ordinal() + 1; // Reserve all zero bits for empty value

        for (int i = 0; i < BITS; i++) {
            states.set(off + i, (ist & 1) == 1);

            ist >>>= 1;
        }

        size += (st == null ? -1 : old == null ? 1 : 0);

        return old;
    }

    /** */
    private GridDhtPartitionState state(int part) {
        int off = part * BITS;

        int st = 0;

        for (int i = 0; i < BITS; i++)
            st |= ((states.get(off + i) ? 1 : 0) << i);

        return st == 0 ? null : GridDhtPartitionState.values()[st - 1];
    }
}
