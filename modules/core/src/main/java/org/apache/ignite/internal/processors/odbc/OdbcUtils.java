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

package org.apache.ignite.internal.processors.odbc;

/**
 * Various ODBC utility methods.
 */
public class OdbcUtils {
    /**
     * Add quotation marks at the beginning and end of the string.
     *
     * @param str Input string.
     * @return String surrounded with quotation marks.
     */
    public static String addQuotationMarksIfNeeded(String str) {
        if (!str.startsWith("\"") && !str.isEmpty())
            return "\"" + str + "\"";

        return str;
    }

    /**
     * Remove quotation marks at the beginning and end of the string if present.
     *
     * @param str Input string.
     * @return String without leading and trailing quotation marks.
     */
    public static String removeQuotationMarksIfNeeded(String str) {
        if (str.startsWith("\"") && str.endsWith("\""))
            return str.substring(1, str.length() - 1);

        return str;
    }

    /**
     * Private constructor.
     */
    private OdbcUtils() {
        // No-op.
    }
}
