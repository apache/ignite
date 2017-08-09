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

package org.apache.ignite.internal.binary;

import java.nio.charset.Charset;
import org.jetbrains.annotations.Nullable;

/**
 * Encoding codes to be used in binary marshalling for string data.
 */
public class BinaryStringEncoding {
    /** */
    public static final byte ENC_WINDOWS_1251 = 1;

    /** */
    private static final Charset CS_WINDOWS_1251 = Charset.forName("windows-1251");

    /**
     * Retrieves charset by numeric encoding code.
     *
     * @param code encoding code.
     * @return charset or {@code null} if no charset corresponds to code given.
     */
    @Nullable public static Charset charsetByCode(byte code) {
        switch (code) {
            case ENC_WINDOWS_1251:
                return CS_WINDOWS_1251;

            default:
                return null;
        }
    }
}
