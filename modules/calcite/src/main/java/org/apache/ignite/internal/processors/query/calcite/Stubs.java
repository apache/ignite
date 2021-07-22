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
package org.apache.ignite.internal.processors.query.calcite;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/** Stubs */
public class Stubs {
    /** */
    public static int intFoo(Object... args) {
        return args == null ? 0 : args.length;
    }

    public static boolean boolFoo(Object... args) {
        return args == null;
    }

    /** */
    public static String stringFoo(Object... args) {
        return args == null ? "null" : "not null";
    }

    public static Collection<Object[]> resultSetGenerator(int cnt, IgniteTypeFactory factory, RelDataType type) {
        List<Object[]> res = new ArrayList<>(cnt);

        for (int row = 0; row < cnt; row++) {
            Object[] tmp = new Object[type.getFieldCount()];

            res.add(tmp);

            for (RelDataTypeField field : type.getFieldList())
                tmp[field.getIndex()] = rndValueOfType(factory.getJavaClass(field.getType()));
        }

        return res;
    }

    private static Object rndValueOfType(Type type) {
        if (type == byte.class || type == Byte.class)
            return (byte)ThreadLocalRandom.current().nextInt(100);

        if (type == short.class || type == Short.class)
            return (short)ThreadLocalRandom.current().nextInt(100);

        if (type == int.class || type == Integer.class)
            return ThreadLocalRandom.current().nextInt(100);

        if (type == long.class || type == Long.class)
            return (long)ThreadLocalRandom.current().nextInt(100);

        if (type == float.class || type == Float.class)
            return ThreadLocalRandom.current().nextFloat();

        if (type == double.class || type == Double.class)
            return ThreadLocalRandom.current().nextDouble();

        if (type == UUID.class)
            return UUID.randomUUID();

        if (type == String.class)
            return UUID.randomUUID().toString();

        throw new IllegalStateException("Can't generate value of type " + type.getTypeName());
    }
}
