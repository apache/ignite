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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.sql.param.TestParamDef;
import org.apache.ignite.internal.sql.param.ParamTests;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.StringOrPattern;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;

/**
 * Common class for SQL parser tests.
 */
@SuppressWarnings("ThrowableNotThrown")
public abstract class SqlParserAbstractSelfTest extends GridCommonAbstractTest {
    /**
     * Make sure that parse error occurs.
     *
     * @param schema Schema.
     * @param sql SQL.
     * @param msg Expected error message.
     */
    protected static void assertParseError(final String schema, final String sql, String msg) {
        assertParseError(schema, sql, StringOrPattern.of(msg));
    }

    /**
     * Make sure that parse error occurs.
     *
     * @param schema Schema.
     * @param sql SQL.
     * @param msgRe Expected error message.
     */
    protected static void assertParseError(final String schema, final String sql, StringOrPattern msgRe) {
        GridTestUtils.assertThrowsSR(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                new SqlParser(schema, sql).nextCommand();

                return null;
            }
        }, SqlParseException.class, msgRe);
    }

    /** FIXME */
    protected <T> void testParameter(final String schema, final String cmdPrefix, TestParamDef<T> def)
        throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        for (TestParamDef.Value<T> val : def.testValues()) {

            for (TestParamDef.Syntax syn : TestParamDef.Syntax.values()) {

                if (!val.supportedSyntaxes().contains(syn))
                    continue;

                try {
                    String sql = ParamTests.makeSqlWithParams(cmdPrefix,
                        new TestParamDef.DefValPair<>(def, val, syn));

                    if (val instanceof TestParamDef.InvalidValue)

                        assertParseError(schema, sql, ((TestParamDef.InvalidValue)val).errorMsgFragment());

                    else {
                        X.println("Checking command: " + sql);

                        SqlCommand cmd = new SqlParser(schema, sql).nextCommand();

                        checkField(cmd, def, val);
                    }
                }
                catch (Exception | AssertionError e) {
                    throw new AssertionError(
                        "When testing " + def + " with expected value " + val + "\n" + e.getMessage(), e);
                }
            }
        }
    }

    /** FIXME */
    protected <T> void checkField(SqlCommand cmd, TestParamDef<T> def, TestParamDef.Value<T> val)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Method getter = cmd.getClass().getMethod(def.cmdFieldName());

        Object cmdVal = getter.invoke(cmd);

        if (cmdVal != null)
            assertTrue(def.fieldClass().isAssignableFrom(cmdVal.getClass()));

        assertEquals(val.fieldValue(), cmdVal);
    }
}
