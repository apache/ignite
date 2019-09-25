/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 */
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
package org.h2.test.trace;

import java.math.BigDecimal;

import org.h2.util.StringUtils;

/**
 * An argument of a statement.
 */
class Arg {
    private Class<?> clazz;
    private Object obj;
    private Statement stat;

    Arg(Class<?> clazz, Object obj) {
        this.clazz = clazz;
        this.obj = obj;
    }

    Arg(Statement stat) {
        this.stat = stat;
    }

    @Override
    public String toString() {
        if (stat != null) {
            return stat.toString();
        }
        return quote(clazz, getValue());
    }

    /**
     * Calculate the value if this is a statement.
     */
    void execute() throws Exception {
        if (stat != null) {
            obj = stat.execute();
            clazz = stat.getReturnClass();
            stat = null;
        }
    }

    Class<?> getValueClass() {
        return clazz;
    }

    Object getValue() {
        return obj;
    }

    private static String quote(Class<?> valueClass, Object value) {
        if (value == null) {
            return null;
        } else if (valueClass == String.class) {
            return StringUtils.quoteJavaString(value.toString());
        } else if (valueClass == BigDecimal.class) {
            return "new BigDecimal(\"" + value.toString() + "\")";
        } else if (valueClass.isArray()) {
            if (valueClass == String[].class) {
                return StringUtils.quoteJavaStringArray((String[]) value);
            } else if (valueClass == int[].class) {
                return StringUtils.quoteJavaIntArray((int[]) value);
            }
        }
        return value.toString();
    }

}
