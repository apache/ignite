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

package org.apache.ignite.internal.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Implementation of {@link Base64Encoder} interface for Java 8 and later.
 *
 * @deprecated Use {@code java.util.Base64} directly instead.
 */
@Deprecated
public class Base64EncoderImpl implements Base64Encoder {
    /** {@inheritDoc} */
    @Override public String encode(byte[] msg) {
        try {
            Object encoder = Class.forName("java.util.Base64").getDeclaredMethod("getEncoder").invoke(null);

            Class<?> encCls = Class.forName("java.util.Base64$Encoder");

            Method encMtd = encCls.getDeclaredMethod("encodeToString", byte[].class);

            return (String)encMtd.invoke(encoder, (Object)msg);
        }
        catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("Failed to encode Base64 message", e);
        }
    }
}
