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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;

public class CommandLogger {
    /** Indent for help output. */
    public static final String INDENT = "  ";

    /**
     * Join input parameters with specified {@code delimeter} between them.
     *
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return Joined paramaters with specified {@code delimeter}.
     */
    public static String j(String delimeter, Object... params) {
        return j(new SB(), "", delimeter, params).toString();
    }

    /**
     * Join input parameters with specified {@code delimeter} between them and append to the end {@code delimeter}.
     *
     * @param sb Specified string builder.
     * @param sbDelimeter Delimeter between {@code sb} and appended {@code param}.
     * @param delimeter Specified delimeter.
     * @param params Other input parameter.
     * @return SB with appended to the end joined paramaters with specified {@code delimeter}.
     */
    public static SB j(SB sb, String sbDelimeter, String delimeter, Object... params) {
        if (!F.isEmpty(params)) {
            sb.a(sbDelimeter);

            for (Object par : params)
                sb.a(par).a(delimeter);

            sb.setLength(sb.length() - delimeter.length());
        }

        return sb;
    }


    /**
     * Join input parameters with space and wrap optional braces {@code []}.
     *
     * @param params Other input parameter.
     * @return Joined parameters wrapped optional braces.
     */
    public static String op(Object... params) {
        return j(new SB(), "[", " ", params).a("]").toString();
    }

    /**
     * Concatenates input parameters to single string with OR delimiter {@code |}.
     *
     * @param params Remaining parameters.
     * @return Concatenated string.
     */
    public static String or(Object... params) {
        return j("|", params);
    }

    /**
     * Join input parameters with space and wrap grouping braces {@code ()}.
     *
     * @param params Input parameter.
     * @return Joined parameters wrapped grouped braces.
     */
    public static String g(Object... params) {
        return j(new SB(), "(", " ", params).a(")").toString();
    }

    /**
     * Output specified string to console.
     *
     * @param s String to output.
     */
    public void log(String s) {
        System.out.println(s);
    }

    /**
     *
     * Output specified string to console.
     *
     * @param s String to output.
     */
    public void logWithIndent(Object s) {
        log(i(s));
    }

    /**
     *
     * Output specified string to console.
     *
     * @param s String to output.
     */
    public void logWithIndent(Object s, int indentCnt) {
        log(i(s), indentCnt);
    }

    /**
     * Adds indent to begin of object's string representation.
     *
     * @param o Input object.
     * @return Indented string.
     */
    public static String i(Object o) {
        return i(o, 1);
    }


    /**
     * Adds specified indents to begin of object's string representation.
     *
     * @param o Input object.
     * @param indentCnt Number of indents.
     * @return Indented string.
     */
    public static String i(Object o, int indentCnt) {
        assert indentCnt >= 0;

        String s = o == null ? null : o.toString();

        switch (indentCnt) {
            case 0:
                return s;

            case 1:
                return INDENT + s;

            default:
                int sLen = s == null ? 4 : s.length();

                SB sb = new SB(sLen + indentCnt * INDENT.length());

                for (int i = 0; i < indentCnt; i++)
                    sb.a(INDENT);

                return sb.a(s).toString();
        }
    }

    /**
     * Format and output specified string to console.
     *
     * @param format A format string as described in Format string syntax.
     * @param args Arguments referenced by the format specifiers in the format string.
     */
    public void log(String format, Object... args) {
        System.out.printf(format, args);
    }

    /**
     * Output empty line.
     */
    public void nl() {
        System.out.println();
    }

    /**
     * Print error to console.
     *

     * @param s Optional message.
     * @param e Error to print.
     */
    public void error(String s, Throwable e) {
        if (!F.isEmpty(s))
            log(s);

        String msg = e.getMessage();

        if (F.isEmpty(msg))
            msg = e.getClass().getName();

        if (msg.startsWith("Failed to handle request")) {
            int p = msg.indexOf("err=");

            msg = msg.substring(p + 4, msg.length() - 1);
        }

        log("Error: " + msg);
    }



    public boolean printErrors(Map<UUID, Exception> exceptions, String s) {
        if (!F.isEmpty(exceptions)) {
            log(s);

            for (Map.Entry<UUID, Exception> e : exceptions.entrySet()) {
                logWithIndent("Node ID: " + e.getKey());

                logWithIndent("Exception message:");
                logWithIndent(e.getValue().getMessage(), 2);
                nl();
            }

            return true;
        }

        return false;
    }
}
