/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import static org.apache.ignite.internal.management.api.CommandUtils.join;

/**
 * Utility class for creating {@code CommangHandler} log messages.
 */
public class CommandLogger {
    /**
     * Join input parameters with space and wrap optional braces {@code []}.
     *
     * @param params Other input parameter.
     * @return Joined parameters wrapped optional braces.
     */
    public static String optional(Object... params) {
        return join(new SB(), "[", " ", params).a("]").toString();
    }

    /**
     * Generates readable error message from exception
     * @param e Exctption
     * @return error message
     */
    public static String errorMessage(Throwable e) {
        String msg = e.getMessage();

        if (F.isEmpty(msg))
            msg = e.getClass().getName();
        else if (msg.startsWith("Failed to handle request")) {
            int p = msg.indexOf("err=");

            msg = msg.substring(p + 4, msg.length() - 1);
        }

        return msg;
    }
}
