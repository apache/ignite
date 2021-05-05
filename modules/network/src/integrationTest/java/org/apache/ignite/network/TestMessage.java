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


package org.apache.ignite.network;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.message.NetworkMessage;

/** */
public class TestMessage implements NetworkMessage, Serializable {
    /** Visible type for tests. */
    public static final short TYPE = 3;

    /** */
    private final String msg;

    /** */
    @IgniteToStringInclude
    private final Map<Integer, String> map;

    /** */
    public TestMessage(String msg, Map<Integer, String> map) {
        this.msg = msg;
        this.map = map;
    }

    public TestMessage(String msg) {
        this(msg, Collections.emptyMap());
    }

    public String msg() {
        return msg;
    }

    public Map<Integer, String> getMap() {
        return map;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestMessage message = (TestMessage) o;
        return Objects.equals(msg, message.msg) && Objects.equals(map, message.map);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(msg, map);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TestMessage.class, this);
    }
}
