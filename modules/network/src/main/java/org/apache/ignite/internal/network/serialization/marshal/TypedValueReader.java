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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataInputStream;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

/**
 * Knows how to read a value from a {@link DataInputStream} and, optionally, knows the expected type (might be taken from
 * a field or be an array component type).
 */
interface TypedValueReader {
    /**
     * Reads the next value from a {@link DataInputStream}.
     *
     * @param input     from where to read
     * @param declaredClass the original class of the object (i.e. {@code byte.class} for {@code byte}); might be {@code null}
     *                      if the type information is not available
     * @param context   unmarshalling context
     * @return the value that was read
     * @throws IOException          if an I/O problem occurs
     * @throws UnmarshalException   if another problem (like {@link ClassNotFoundException}) occurs
     */
    Object read(DataInputStream input, @Nullable Class<?> declaredClass, UnmarshallingContext context)
            throws IOException, UnmarshalException;
}
