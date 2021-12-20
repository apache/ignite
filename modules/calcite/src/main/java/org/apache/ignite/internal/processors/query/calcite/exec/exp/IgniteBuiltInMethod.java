/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.lang.reflect.Method;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserUtil;

/**
 * Built-in methods.
 */
public enum IgniteBuiltInMethod {
    /** */
    SYSTEM_RANGE2(IgniteSqlFunctions.class, "systemRange", Object.class, Object.class),

    /** */
    SYSTEM_RANGE3(IgniteSqlFunctions.class, "systemRange", Object.class, Object.class, Object.class),

    /** */
    PARSE_INTERVAL_YEAR_MONTH(SqlParserUtil.class, "intervalToMonths", String.class, SqlIntervalQualifier.class),

    /** */
    PARSE_INTERVAL_DAY_TIME(SqlParserUtil.class, "intervalToMillis", String.class, SqlIntervalQualifier.class),

    /** */
    BYTESTRING_TO_STRING(IgniteSqlFunctions.class, "toString", ByteString.class),

    /** */
    STRING_TO_BYTESTRING(IgniteSqlFunctions.class, "toByteString", String.class);

    /** */
    public final Method method;

    /** */
    IgniteBuiltInMethod(Method method) {
        this.method = method;
    }

    /** Defines a method. */
    IgniteBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes));
    }
}
