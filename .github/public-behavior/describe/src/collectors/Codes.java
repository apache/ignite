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

package collectors;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import framework.BuiltTree;
import framework.Collector;
import framework.Declarations;

/**
 * Describes protocol, storage and error codes declared in internal classes. Users see only the value of a code, so neither its
 * class nor its name is part of what they rely on. A code is identified by its group, such as thin client operations, and its
 * name. The name is a rename group: a code renamed within its group is matched with itself, not with a code of another group
 * that has the same value. Moving codes to another class changes only their row in the tables below.
 * <p>
 * A code is never marked deprecated: {@code @Deprecated} on an internal constant is not a mark that users see. So removing such
 * a code is a change like any other.
 */
public class Codes implements Collector {
    /** Group of the query error codes. Ignite maps each of them to an SQLSTATE, whatever class declares it. */
    private static final String QUERY_ERRORS = "query error code";

    /** Internal classes that declare codes as constants: area, group of the codes, class, names of the constants. */
    private static final String[][] CONSTANTS = {
        {"Thin client protocol", "operation", "org.apache.ignite.internal.processors.platform.client.ClientMessageParser", "OP_.*"},
        {"Thin client protocol", "status", "org.apache.ignite.internal.processors.platform.client.ClientStatus", ".*"},
        {"Thin client protocol", "version", "org.apache.ignite.internal.processors.platform.client.ClientConnectionContext",
            "VER_.*|DEFAULT_VER"},
        {"JDBC protocol", "request", "org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest", "[A-Z_]+"},
        {"JDBC protocol", "version", "org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext", "VER_.*|CURRENT_VER"},
        {"ODBC protocol", "request", "org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest", "[A-Z_]+"},
        {"ODBC protocol", "version", "org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext", "VER_.*|CURRENT_VER"},
        {"Binary format", "constant", "org.apache.ignite.internal.binary.GridBinaryMarshaller", "[A-Z_0-9]+"},
        {"Stored data", "page type", "org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO", "T_.*"},
        {"Stored data", "snapshot metadata", "org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata",
            "serialVersionUID"},
        {"Errors", "SQLSTATE", "org.apache.ignite.internal.processors.odbc.SqlStateCode", "[A-Z_0-9]+"},
        {"Errors", QUERY_ERRORS, "org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode", "[A-Z_0-9]+"},
        {"CLI", "exit code", "org.apache.ignite.internal.commandline.CommandHandler", "EXIT_CODE_.*|ERR_.*"},
        {"SQL keywords", "native parser", "org.apache.ignite.internal.sql.SqlKeyword", "[A-Z_0-9]+"},
    };

    /** Internal enums whose constants users send or store by a code: area, group of the codes, enum, method that returns the code. */
    private static final String[][] CODED_ENUMS = {
        {"Thin client protocol", "server feature", "org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature",
            "featureId"},
        {"Thin client protocol", "Java client feature", "org.apache.ignite.internal.client.thin.ProtocolBitmaskFeature", "featureId"},
        {"REST", "command", "org.apache.ignite.internal.processors.rest.GridRestCommand", "key"},
        {"Stored data", "WAL record type", "org.apache.ignite.internal.pagemem.wal.record.WALRecord$RecordType", "index"},
    };

    /** {@inheritDoc} */
    @Override public void describe(BuiltTree tree, Declarations out) throws Exception {
        for (String[] c : CONSTANTS) {
            Class<?> cls = Class.forName(c[2]);
            List<Field> codes = codes(cls, c[3]);

            for (Field f : codes) {
                out.add(c[0], c[1] + ' ' + f.getName(), value(Declarations.staticValue(f)), "", tree.where(cls, f.getName(), -1, false),
                    f.getName(), null);
            }

            if (codes.isEmpty())
                out.error("No constants matching " + c[3] + " found in " + c[2]);

            if (c[1].equals(QUERY_ERRORS))
                sqlStates(tree, out, c[0], codes);
        }

        for (String[] c : CODED_ENUMS) {
            Class<?> cls = Class.forName(c[2]);
            Method code = cls.getMethod(c[3]);

            for (Object e : cls.getEnumConstants()) {
                String name = ((Enum<?>)e).name();

                out.add(c[0], c[1] + ' ' + name, String.valueOf(code.invoke(e)), "", tree.where(cls, name, -1, false), name, null);
            }
        }
    }

    /** @return Constants of the class whose names match the pattern and whose values are codes. */
    private static List<Field> codes(Class<?> cls, String names) {
        List<Field> res = new ArrayList<>();

        for (Field f : cls.getDeclaredFields()) {
            int mod = f.getModifiers();
            Class<?> type = f.getType();

            if (!f.isSynthetic() && Modifier.isStatic(mod) && Modifier.isFinal(mod) && f.getName().matches(names)
                && (type.isPrimitive() || type == String.class || type == ClientListenerProtocolVersion.class))
                res.add(f);
        }

        return res;
    }

    /** Describes the SQLSTATE that each query error code of a class maps to. */
    private static void sqlStates(BuiltTree tree, Declarations out, String area, List<Field> codes) {
        for (Field f : codes) {
            if (f.getType() != int.class)
                continue;

            int code = (Integer)Declarations.staticValue(f);

            out.add(area, "SQLSTATE of query error code " + code, IgniteQueryErrorCode.codeToSqlState(code), "",
                tree.where(IgniteQueryErrorCode.class, "codeToSqlState", 1, false));
        }
    }

    /** @return Value of a code. */
    private static String value(Object val) {
        if (val instanceof ClientListenerProtocolVersion) {
            ClientListenerProtocolVersion ver = (ClientListenerProtocolVersion)val;

            return ver.major() + "." + ver.minor() + '.' + ver.maintenance();
        }

        return Declarations.value(val);
    }
}
