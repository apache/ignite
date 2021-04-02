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
import java.util.Objects;

/**
 * Representation of the network member.
 */
public class NetworkMember implements Serializable {
    /** Unique name of member in cluster. */
    private final String name;

    /**
     * @param name Unique name of member in cluster.
     */
    public NetworkMember(String name) {
        this.name = name;
    }

    /**
     * @return Unique name of member in cluster.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        NetworkMember member = (NetworkMember)o;
        return Objects.equals(name, member.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "NetworkMember{" +
            "name='" + name + '\'' +
            '}';
    }
}
