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

package org.apache.ignite.marshaller;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * {@code Marshaller} allows to marshal or unmarshal objects in grid. It provides
 * serialization/deserialization mechanism for all instances that are sent across networks
 * or are otherwise serialized.
 * <p>
 * Ignite provides the following {@code Marshaller} implementations:
 * <ul>
 * <li>Default binary marshaller. For more information, see {@link IgniteBinary}.</li>
 * </ul>
 * <p>
 * <p>Usage of other marshallers</p>
 *
 *
 */
public interface Marshaller {
    /**
     * Sets marshaller context.
     *
     * @param ctx Marshaller context.
     */
    public void setContext(MarshallerContext ctx);

    /**
     * Marshals object to the output stream. This method should not close
     * given output stream.
     *
     * @param obj Object to marshal. {@code null} object will be marshaled to binary {@code null} representation.
     * @param out Output stream to marshal into.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void marshal(@Nullable Object obj, OutputStream out) throws IgniteCheckedException;

    /**
     * Marshals object to byte array.
     *
     * @param obj Object to marshal. {@code null} object will be marshaled to binary {@code null} representation.
     * @return Byte array.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException;

    /**
     * Unmarshals object from the input stream using given class loader.
     * This method should not close given input stream.
     *
     * @param <T> Type of unmarshalled object.
     * @param in Input stream.
     * @param clsLdr If not {@code null} then given class loader will be used for unmarshal object.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException;

    /**
     * Unmarshals object from byte array using given class loader.
     *
     * @param <T> Type of unmarshalled object.
     * @param arr Byte array.
     * @param clsLdr If not {@code null} then given class loader will be used for unmarshal object.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public <T> T unmarshal(byte[] arr, @Nullable ClassLoader clsLdr) throws IgniteCheckedException;
}
