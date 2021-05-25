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

import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.processor.annotations.AutoSerializable;

@AutoSerializable(messageFactory = AllTypesMessageFactory.class)
public interface AllTypesMessage extends NetworkMessage {
    short TYPE = 123;

    byte a();

    short b();

    int c();

    long d();

    float e();

    double f();

    char g();

    boolean h();

    byte[] i();

    short[] j();

    int[] k();

    long[] l();

    float[] m();

    double[] n();

    char[] o();

    boolean[] p();

    String q();

    BitSet r();

    UUID s();

    IgniteUuid t();

    NetworkMessage u();

    NetworkMessage[] v();

    Collection<NetworkMessage> w();

    Map<String, NetworkMessage> x();

    /** {@inheritDoc} */
    @Override public default short directType() {
        return 5555;
    }

    interface Builder {
        AllTypesMessage build();

        Builder a(byte a);

        Builder b(short b);

        Builder c(int c);

        Builder d(long d);

        Builder e(float e);

        Builder f(double f);

        Builder g(char g);

        Builder h(boolean h);

        Builder i(byte[] i);

        Builder j(short[] j);

        Builder k(int[] k);

        Builder l(long[] l);

        Builder m(float[] m);

        Builder n(double[] n);

        Builder o(char[] o);

        Builder p(boolean[] p);

        Builder q(String q);

        Builder r(BitSet r);

        Builder s(UUID s);

        Builder t(IgniteUuid t);

        Builder u(NetworkMessage u);

        Builder v(NetworkMessage[] v);

        Builder w(Collection<NetworkMessage> w);

        Builder x(Map<String, NetworkMessage> x);
    }
}
