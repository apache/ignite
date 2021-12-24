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

package org.apache.ignite.internal.network.serialization;

import java.util.List;

/**
 * Class that holds a byte array of the serialized object and a list of type descriptor ids with all the descriptors that were
 * used during the serialization.
 * TODO: IGNITE-16081 It's a temporary class that will be removed.
 */
public class SerializationResult {
    /** Serialized object. */
    private final byte[] array;

    /** Type descriptors. */
    private final List<Integer> ids;

    /**
     * Constructor.
     *
     * @param array Serialized object.
     * @param ids   Type descriptors.
     */
    public SerializationResult(byte[] array, List<Integer> ids) {
        this.array = array;
        this.ids = ids;
    }

    /** Gets serialized object. */
    public byte[] array() {
        return array;
    }

    /** Gets descriptor ids. */
    public List<Integer> ids() {
        return ids;
    }
}
