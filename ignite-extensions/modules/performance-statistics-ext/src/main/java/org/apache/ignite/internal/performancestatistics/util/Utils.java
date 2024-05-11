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

package org.apache.ignite.internal.performancestatistics.util;

import java.io.PrintStream;
import com.fasterxml.jackson.core.io.CharTypes;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** */
public class Utils {
    /** Json mapper. */
    public static final ObjectMapper MAPPER = new ObjectMapper();

    /** */
    private static final char[] HC = "0123456789ABCDEF".toCharArray();

    /** Creates empty object for given value if absent. */
    public static ObjectNode createObjectIfAbsent(String val, ObjectNode json) {
        ObjectNode node = (ObjectNode)json.get(val);

        if (node == null) {
            node = MAPPER.createObjectNode();

            json.set(val, node);
        }

        return node;
    }

    /** Creates empty array for given value if absent. */
    public static ArrayNode createArrayIfAbsent(String val, ObjectNode json) {
        ArrayNode node = (ArrayNode)json.get(val);

        if (node == null) {
            node = MAPPER.createArrayNode();

            json.set(val, node);
        }

        return node;
    }

    /**
     * Prints JSON-escaped string to the stream.
     *
     * @param ps Print stream to write to.
     * @param str String to print.
     * @see CharTypes#appendQuoted(StringBuilder, String)
     */
    public static void printEscaped(PrintStream ps, String str) {
        int[] escCodes = CharTypes.get7BitOutputEscapes();

        int escLen = escCodes.length;

        for (int i = 0, len = str.length(); i < len; ++i) {
            char c = str.charAt(i);

            if (c >= escLen || escCodes[c] == 0) {
                ps.print(c);

                continue;
            }

            ps.print('\\');

            int escCode = escCodes[c];

            if (escCode < 0) {
                ps.print('u');
                ps.print('0');
                ps.print('0');

                int val = c;

                ps.print(HC[val >> 4]);
                ps.print(HC[val & 0xF]);
            }
            else
                ps.print((char)escCode);
        }
    }
}
