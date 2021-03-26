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

package org.apache.ignite.binary;

/**
 * Binary objects utility class.
 */
public final class BinaryObjects {
    /**
     * Wraps byte array to BinaryObject.
     * @param data Object data.
     * @return Binary object.
     */
    public static BinaryObject wrap(byte[] data) {
        return null;
    }

    /**
     * Deserializes binary object.
     *
     * @param obj Object to deserialize.
     * @param targetCls Target class.
     * @return Deserialized object.
     */
    public static <T> T deserialize(BinaryObject obj, Class<T> targetCls) {
        return null;
    }

    /**
     * Constructor.
     */
    private BinaryObjects() {
        // No-op.
    }
}
