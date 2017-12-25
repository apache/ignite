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
import org.apache.ignite.internal.sql.param.ParamTestUtils;
import org.apache.ignite.internal.sql.param.TestParamDef;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
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
     * @param msgRe Expected error message.
     */
    protected static void assertParseError(final String schema, final String sql, String msgRe) {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                new SqlParser(schema, sql).nextCommand();

                return null;
            }
        }, SqlParseException.class, msgRe);
    }

    /**
     * Tests that SQL command parameter is handled correctly by creating SQL command with a parameter,
     * parsing it and checking field value inside the {@link SqlCommand} subclass created by the parser.
     *
     * @param schema The schema.
     * @param cmdPrefix Start of the SQL command to which the parameter is appended.
     * @param paramDef Parameter definition.
     * @param otherParamVals Default values of the other parameters (which are not specified). If
     *      not null, values of these not specified parameters are checked as well in the fields
     *      of {@link SqlCommand} subclass.
     * @param <T> Parameter type.
     * @throws AssertionError if testing fails.
     */
    @SuppressWarnings("unchecked")
    protected <T> void testParameter(final String schema, final String cmdPrefix, TestParamDef<T> paramDef,
        @Nullable List<TestParamDef.DefValPair<?>> otherParamVals) {

        for (TestParamDef.Value<T> val : paramDef.testValues()) {

            for (TestParamDef.Syntax syn : TestParamDef.Syntax.values()) {

                if (!val.supportedSyntaxes().contains(syn))
                    continue;

                try {
                    String sql = ParamTestUtils.makeSqlWithParams(cmdPrefix,
                        new TestParamDef.DefValPair<>(paramDef, val, syn));

                    if (val instanceof TestParamDef.InvalidValue)

                        assertParseError(schema, sql, ((TestParamDef.InvalidValue)val).errorMsgFragment());

                    else {
                        X.println("Checking command: " + sql);

                        SqlCommand cmd = new SqlParser(schema, sql).nextCommand();

                        checkField(cmd, paramDef, val);

                        if (otherParamVals != null) {

                            for (TestParamDef.DefValPair<?> otherParamDefVal : otherParamVals) {

                                if (!otherParamDefVal.def().cmdFieldName().equals(paramDef.cmdFieldName()))
                                    checkField(cmd,
                                        (TestParamDef<Object>)otherParamDefVal.def(),
                                        (TestParamDef.Value<Object>)otherParamDefVal.val());
                            }
                        }
                    }
                }
                catch (Exception | AssertionError e) {
                    throw new AssertionError(
                        "When testing " + paramDef + " with expected value " + val + "\n" + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Checks that the field in {@link SqlCommand} is set correctly. The method expects the command class
     * to have a getter with the same name as the field.
     *
     * @param cmd The command class to look up the field in.
     * @param def The parameter definition.
     * @param val The value to expect in the command class field.
     * @param <T> The type of the value in the command class.
     * @throws NoSuchMethodException If there is no getter with the same name as the parameter.
     * @throws InvocationTargetException If the getter cannot be called.
     * @throws IllegalAccessException If we don't have access to the getter.
     */
    protected <T> void checkField(SqlCommand cmd, TestParamDef<T> def, TestParamDef.Value<T> val)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        assert def.testValues().contains(val);

        Method getter = cmd.getClass().getMethod(def.cmdFieldName());

        Object cmdVal = getter.invoke(cmd);

        if (cmdVal != null)
            assertTrue(def.fieldClass().isAssignableFrom(cmdVal.getClass()));

        assertEquals(val.fieldValue(), cmdVal);
    }
}
