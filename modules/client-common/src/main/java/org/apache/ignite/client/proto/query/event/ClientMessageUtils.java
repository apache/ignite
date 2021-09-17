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

package org.apache.ignite.client.proto.query.event;

import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;

/**
 * Client message utils class.
 */
public class ClientMessageUtils {
    /**
     * Packs a string or null if string is null.
     *
     * @param packer Message packer.
     * @param str String to serialize.
     */
    public static void writeStringNullable(ClientMessagePacker packer, String str) {
        if (str == null)
            packer.packNil();
        else
            packer.packString(str);
    }

    /**
     * Unpack a string or null if no string is presented.
     *
     * @param unpacker Message unpacker.
     * @return String or null.
     */
    public static String readStringNullable(ClientMessageUnpacker unpacker) {
        if (!unpacker.tryUnpackNil())
            return unpacker.unpackString();

        return null;
    }
}
