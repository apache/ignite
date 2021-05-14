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

package org.apache.ignite.network.internal;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Message with all types supported by Direct Marshalling.
 */
public class AllTypesMessage implements NetworkMessage {
    /** */
    @TestFieldType(MessageCollectionItemType.BYTE)
    byte a;

    /** */
    @TestFieldType(MessageCollectionItemType.SHORT)
    short b;

    /** */
    @TestFieldType(MessageCollectionItemType.INT)
    int c;

    /** */
    @TestFieldType(MessageCollectionItemType.LONG)
    long d;

    /** */
    @TestFieldType(MessageCollectionItemType.FLOAT)
    float e;

    /** */
    @TestFieldType(MessageCollectionItemType.DOUBLE)
    double f;

    /** */
    @TestFieldType(MessageCollectionItemType.CHAR)
    char g;

    /** */
    @TestFieldType(MessageCollectionItemType.BOOLEAN)
    boolean h;

    /** */
    @TestFieldType(MessageCollectionItemType.BYTE_ARR)
    byte[] i;

    /** */
    @TestFieldType(MessageCollectionItemType.SHORT_ARR)
    short[] j;

    /** */
    @TestFieldType(MessageCollectionItemType.INT_ARR)
    int[] k;

    /** */
    @TestFieldType(MessageCollectionItemType.LONG_ARR)
    long[] l;

    /** */
    @TestFieldType(MessageCollectionItemType.FLOAT_ARR)
    float[] m;

    /** */
    @TestFieldType(MessageCollectionItemType.DOUBLE_ARR)
    double[] n;

    /** */
    @TestFieldType(MessageCollectionItemType.CHAR_ARR)
    char[] o;

    /** */
    @TestFieldType(MessageCollectionItemType.BOOLEAN_ARR)
    boolean[] p;

    /** */
    @TestFieldType(MessageCollectionItemType.STRING)
    String q;

    /** */
    @TestFieldType(MessageCollectionItemType.BIT_SET)
    BitSet r;

    /** */
    @TestFieldType(MessageCollectionItemType.UUID)
    UUID s;

    /** */
    @TestFieldType(MessageCollectionItemType.IGNITE_UUID)
    IgniteUuid t;

    /** */
    @TestFieldType(MessageCollectionItemType.MSG)
    NetworkMessage u;

    /** */
    Object[] v;

    /** */
    Collection<?> w;

    /** */
    Map<?, ?> x;

    /** {@inheritDoc} */
    @Override public short directType() {
        return 5555;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o1) {
        if (this == o1) return true;
        if (o1 == null || getClass() != o1.getClass()) return false;
        AllTypesMessage message = (AllTypesMessage) o1;
        return a == message.a && b == message.b && c == message.c && d == message.d && Float.compare(message.e, e) == 0 && Double.compare(message.f, f) == 0 && g == message.g && h == message.h && Arrays.equals(i, message.i) && Arrays.equals(j, message.j) && Arrays.equals(k, message.k) && Arrays.equals(l, message.l) && Arrays.equals(m, message.m) && Arrays.equals(n, message.n) && Arrays.equals(o, message.o) && Arrays.equals(p, message.p) && Objects.equals(q, message.q) && Objects.equals(r, message.r) && Objects.equals(s, message.s) && Objects.equals(t, message.t) && Objects.equals(u, message.u) && Arrays.equals(v, message.v) && Objects.equals(w, message.w) && Objects.equals(x, message.x);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(a, b, c, d, e, f, g, h, q, r, s, t, u, w, x);
        result = 31 * result + Arrays.hashCode(i);
        result = 31 * result + Arrays.hashCode(j);
        result = 31 * result + Arrays.hashCode(k);
        result = 31 * result + Arrays.hashCode(l);
        result = 31 * result + Arrays.hashCode(m);
        result = 31 * result + Arrays.hashCode(n);
        result = 31 * result + Arrays.hashCode(o);
        result = 31 * result + Arrays.hashCode(p);
        result = 31 * result + Arrays.hashCode(v);
        return result;
    }
}
