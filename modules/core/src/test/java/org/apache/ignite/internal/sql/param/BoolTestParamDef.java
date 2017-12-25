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

package org.apache.ignite.internal.sql.param;

import java.util.List;

/** A boolean test parameter definition. Boolean parameters have additional syntax compared to enum parameters. */
public class BoolTestParamDef extends TestParamDef<Boolean> {

    /** A keyword for false value of parameter (e.g., LOGGING/NOLOGGING). */
    private final String falseKeyword;

    /**
     * Creates a boolean test parameter definition.
     *
     * @param trueKeyword Keyword for true value of parameter.
     * @param falseKeyword Keyword for false value of parameter.
     * @param fldName Field name in the target class.
     * @param testValues Values to test.
     */
    public BoolTestParamDef(String trueKeyword, String falseKeyword, String fldName, List<Value<Boolean>> testValues) {
        super(trueKeyword, fldName, Boolean.class, testValues);

        this.falseKeyword = falseKeyword;
    }

    /**
     * Returns a keyword for false value of parameter (e.g., LOGGING/NOLOGGING).
     * @return A keyword for false value of parameter.
     */
    public String falseKeyword() {
        return falseKeyword;
    }
}
