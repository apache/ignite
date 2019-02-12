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

package org.apache.ignite.internal.processors.platform.client;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Affinity function descriptor. Used to compare affinity functions of two tables.
 */
public class ClientPartitionAffinityDescriptor implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Number of partitions. */
    private final int parts;

    /**
     * Constructor.
     *
     * @param parts Number of partitions.
     */
    public ClientPartitionAffinityDescriptor(int parts) {
        this.parts = parts;
    }

    /**
     * Check is provided descriptor is compatible with this instance (i.e. can be used in the same co-location group).
     *
     * @param other Other descriptor.
     * @return {@code True} if compatible.
     */
    public boolean isCompatible(ClientPartitionAffinityDescriptor other) {
        if (other == null)
            return false;

        return other.parts == parts;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientPartitionAffinityDescriptor.class, this);
    }
}
