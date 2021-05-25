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

package org.apache.ignite.network.processor.internal;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;

/**
 * Message with all types supported by Direct Marshalling.
 */
class AllTypesMessageImpl implements AllTypesMessage, AllTypesMessage.Builder {

    private byte a;

    private short b;

    private int c;

    private long d;

    private float e;

    private double f;

    private char g;

    private boolean h;

    private byte[] i;

    private short[] j;

    private int[] k;

    private long[] l;

    private float[] m;

    private double[] n;

    private char[] o;

    private boolean[] p;

    private String q;

    private BitSet r;

    private UUID s;

    private IgniteUuid t;

    private NetworkMessage u;

    private NetworkMessage[] v;

    private Collection<NetworkMessage> w;

    private Map<String, NetworkMessage> x;

    /**
     * @return A.
     */
    @Override public byte a() {
        return a;
    }

    /**
     * @param a New a.
     */
    @Override public Builder a(byte a) {
        this.a = a;
        return this;
    }

    /**
     * @return B.
     */
    @Override public short b() {
        return b;
    }

    /**
     * @param b New b.
     */
    @Override public Builder b(short b) {
        this.b = b;
        return this;
    }

    /**
     * @return C.
     */
    @Override public int c() {
        return c;
    }

    /**
     * @param c New c.
     */
    @Override public Builder c(int c) {
        this.c = c;
        return this;
    }

    /**
     * @return D.
     */
    @Override public long d() {
        return d;
    }

    /**
     * @param d New d.
     */
    @Override public Builder d(long d) {
        this.d = d;
        return this;
    }

    /**
     * @return Exception.
     */
    @Override public float e() {
        return e;
    }

    /**
     * @param e New exception.
     */
    @Override public Builder e(float e) {
        this.e = e;
        return this;
    }

    /**
     * @return F.
     */
    @Override public double f() {
        return f;
    }

    /**
     * @param f New f.
     */
    @Override public Builder f(double f) {
        this.f = f;
        return this;
    }

    /**
     * @return G.
     */
    @Override public char g() {
        return g;
    }

    /**
     * @param g New g.
     */
    @Override public Builder g(char g) {
        this.g = g;
        return this;
    }

    /**
     * @return H.
     */
    @Override public boolean h() {
        return h;
    }

    /**
     * @param h New h.
     */
    @Override public Builder h(boolean h) {
        this.h = h;
        return this;
    }

    /**
     * @return I.
     */
    @Override public byte[] i() {
        return i;
    }

    /**
     * @param i New i.
     */
    @Override public Builder i(byte[] i) {
        this.i = i;
        return this;
    }

    /**
     * @return J.
     */
    @Override public short[] j() {
        return j;
    }

    /**
     * @param j New j.
     */
    @Override public Builder j(short[] j) {
        this.j = j;
        return this;
    }

    /**
     * @return K.
     */
    @Override public int[] k() {
        return k;
    }

    /**
     * @param k New k.
     */
    @Override public Builder k(int[] k) {
        this.k = k;
        return this;
    }

    /**
     * @return L.
     */
    @Override public long[] l() {
        return l;
    }

    /**
     * @param l New l.
     */
    @Override public Builder l(long[] l) {
        this.l = l;
        return this;
    }

    /**
     * @return M.
     */
    @Override public float[] m() {
        return m;
    }

    /**
     * @param m New m.
     */
    @Override public Builder m(float[] m) {
        this.m = m;
        return this;
    }

    /**
     * @return N.
     */
    @Override public double[] n() {
        return n;
    }

    /**
     * @param n New n.
     */
    @Override public Builder n(double[] n) {
        this.n = n;
        return this;
    }

    /**
     * @return O.
     */
    @Override public char[] o() {
        return o;
    }

    /**
     * @param o New o.
     */
    @Override public Builder o(char[] o) {
        this.o = o;
        return this;
    }

    /**
     * @return P.
     */
    @Override public boolean[] p() {
        return p;
    }

    /**
     * @param p New p.
     */
    @Override public Builder p(boolean[] p) {
        this.p = p;
        return this;
    }

    /**
     * @return Q.
     */
    @Override public String q() {
        return q;
    }

    /**
     * @param q New q.
     */
    @Override public Builder q(String q) {
        this.q = q;
        return this;
    }

    /**
     * @return R.
     */
    @Override public BitSet r() {
        return r;
    }

    /**
     * @param r New r.
     */
    @Override public Builder r(BitSet r) {
        this.r = r;
        return this;
    }

    /**
     * @return S.
     */
    @Override public UUID s() {
        return s;
    }

    /**
     * @param s New s.
     */
    @Override public Builder s(UUID s) {
        this.s = s;
        return this;
    }

    /**
     * @return T.
     */
    @Override public IgniteUuid t() {
        return t;
    }

    /**
     * @param t New t.
     */
    @Override public Builder t(IgniteUuid t) {
        this.t = t;
        return this;
    }

    /**
     * @return U.
     */
    @Override public NetworkMessage u() {
        return u;
    }

    /**
     * @param u New u.
     */
    @Override public Builder u(NetworkMessage u) {
        this.u = u;
        return this;
    }

    /**
     * @return V.
     */
    @Override public NetworkMessage[] v() {
        return v;
    }

    /**
     * @param v New v.
     */
    @Override public Builder v(NetworkMessage[] v) {
        this.v = v;
        return this;
    }

    /**
     * @return W.
     */
    @Override public Collection<NetworkMessage> w() {
        return w;
    }

    /**
     * @param w New w.
     */
    @Override public Builder w(Collection<NetworkMessage> w) {
        this.w = w;
        return this;
    }

    /**
     * @return X.
     */
    @Override public Map<String, NetworkMessage> x() {
        return x;
    }

    /**
     * @param x New x.
     */
    @Override public Builder x(Map<String, NetworkMessage> x) {
        this.x = x;
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o1) {
        if (this == o1)
            return true;
        if (o1 == null || getClass() != o1.getClass())
            return false;
        AllTypesMessageImpl message = (AllTypesMessageImpl)o1;
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

    /** {@inheritDoc} */
    @Override public AllTypesMessage build() {
        return this;
    }
}
