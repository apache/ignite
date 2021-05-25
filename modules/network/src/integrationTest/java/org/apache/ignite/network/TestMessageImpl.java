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

/** */
class TestMessageImpl implements TestMessage, TestMessage.Builder, Serializable {
    /** */
    private String msg;

    /** */
    @IgniteToStringInclude
    private Map<Integer, String> map = Collections.emptyMap();

    /** {@inheritDoc} */
    @Override public String msg() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, String> map() {
        return map;
    }

    /** {@inheritDoc} */
    @Override public Builder msg(String msg) {
        this.msg = msg;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Builder map(Map<Integer, String> map) {
        this.map = map;
        return this;
    }

    /** {@inheritDoc} */
    @Override public TestMessage build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestMessageImpl message = (TestMessageImpl) o;
        return Objects.equals(msg, message.msg) && Objects.equals(map, message.map);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(msg, map);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TestMessageImpl.class, this);
    }
}
