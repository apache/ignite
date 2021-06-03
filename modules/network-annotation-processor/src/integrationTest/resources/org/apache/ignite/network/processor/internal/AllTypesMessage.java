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
import org.apache.ignite.network.annotations.Transferable;

@Transferable(1)
public interface AllTypesMessage extends NetworkMessage {
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
}
