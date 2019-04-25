/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.marshaller;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Marshaller for binary protocol messages.
 */
public interface GridClientMarshaller {
    /**
     * Marshals object to byte array.
     *
     * @param obj Object to marshal.
     * @param off Start offset.
     * @return Byte buffer.
     * @throws IOException If marshalling failed.
     */
    public ByteBuffer marshal(Object obj, int off) throws IOException;

    /**
     * Unmarshals object from byte array.
     *
     * @param bytes Byte array.
     * @return Unmarshalled object.
     * @throws IOException If unmarshalling failed.
     */
    public <T> T unmarshal(byte[] bytes) throws IOException;
}