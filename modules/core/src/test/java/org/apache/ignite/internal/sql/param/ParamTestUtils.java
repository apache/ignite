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

import com.google.common.base.Optional;
import junit.framework.TestCase;
import org.apache.ignite.internal.sql.SqlEnumParserUtils;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.jetbrains.annotations.NotNull;

import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import static org.apache.ignite.internal.sql.SqlKeyword.DEFAULT;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.KEY_EQ_VAL;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.KEY_SPACE_VAL;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.KEY_WITH_OPT_NO;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.VAL;

/** Contains utilily methods for testing SQL parameters. */
public final class ParamTestUtils {

    /** A string to test bad boolean value parameter. Side note: purple smurfs don't play ping-pong. */
    private static final String BAD_BOOLEAN_VALUE = "Gnip_gnop";

    /** Creates SQL statement with given parameters.
     *
     * @param cmdPrefix Main command to add parameters to.
     * @param params Parameters to add with values and options.
     * @return SQL string with command and parameters.
     */
    @SuppressWarnings("unchecked")
    @NotNull public static String makeSqlWithParams(String cmdPrefix, TestParamDef.DefValPair ... params) {
        StringBuilder sb = new StringBuilder();

        sb.append(cmdPrefix);

        for (TestParamDef.DefValPair<Object> p : params)
            sb.append(makeParamStr(p));

        return sb.toString();
    }

    /**
     * Creates string representation of parameter + value + options.
     *
     * @param param The parameter + value + options to convert to string.
     * @return The resulting SQL fragment with the parameter.
     */
    @SuppressWarnings("unchecked")
    @NotNull public static <T> String makeParamStr(TestParamDef.DefValPair<T> param) {

        assert param.val().supportedSyntaxes().contains(param.syntax());
        assert param.def().testValues().contains(param.val());

        if (param.val() instanceof TestParamDef.MissingValue)
            return "";

        TestParamDef.SpecifiedValue<T> sval = (TestParamDef.SpecifiedValue<T>) param.val();

        switch (param.syntax()) {
            case KEY_WITH_OPT_NO:
                TestCase.assertTrue(param.def() instanceof BoolTestParamDef);
                TestCase.assertNotNull(sval.fieldValue());

                if ((Boolean) sval.fieldValue())
                    return " " + param.def().cmdParamName();
                else
                    return " " + ((BoolTestParamDef) param.def()).falseKeyword();

            case VAL:
                return " " + sval.cmdValue();

            case KEY_EQ_VAL:
                return " " + param.def().cmdParamName() + SqlLexerTokenType.EQUALS.asChar() + sval.cmdValue();

            case KEY_SPACE_VAL:
                return " " + param.def().cmdParamName() + ' ' + sval.cmdValue();

            default:
                TestCase.fail("Internal error: enum value not handled");
                return "<INTERNAL-ERROR>"; // shouldn't arrive here
        }
    }

    /**
     * Creates a basic list of values to test string parameter with.
     *
     * @param dfltVal The default value to test (when parameter is specified with {@link SqlKeyword#DEFAULT} value
     *      or empty if the default value shall not be tested.
     * @param missingVal What value to expect if the parameter is missing.
     * Empty if the missing value shall not be tested.
     * @param testValues String values to test.
     * @return List of parameter value definitions.
     */
    @NotNull public static List<TestParamDef.Value<String>> makeBasicStrTestValues(Optional<String> dfltVal,
        Optional<String> missingVal, String... testValues) {

        List<TestParamDef.Value<String>> params = new LinkedList<>();

        if (missingVal.isPresent())
            params.add(new TestParamDef.MissingValue<>(missingVal.get()));

        EnumSet<TestParamDef.Syntax> syntaxes = EnumSet.of(KEY_SPACE_VAL, KEY_EQ_VAL);

        if (dfltVal.isPresent())
            params.add(new TestParamDef.ValidValue<>(DEFAULT, dfltVal.get(), syntaxes));

        for (String v : testValues) {
            params.add(new TestParamDef.ValidIdentityValue<>(v.toUpperCase(), syntaxes));
            params.add(new TestParamDef.ValidValue<>('"' + v + '"', v, syntaxes));
        }

        return params;
    }

    /**
     * Creates a basic list of values to test enum parameter with.
     *
     * @param cls The enum class.
     * @param dfltVal The default value to test (when parameter is specified with {@link SqlKeyword#DEFAULT} value
     *      or empty if the default value shall not be tested.
     * @param missingVal What value to expect if the parameter is missing.
     *      Empty if the missing value shall not be tested.
     * @return List of parameter value definitions.
     */
    @NotNull public static <T extends Enum<T>> List<TestParamDef.Value<T>> makeBasicEnumTestValues(Class<T> cls,
        Optional<T> dfltVal, Optional<T> missingVal) {

        List<TestParamDef.Value<T>> params = new LinkedList<>();

        if (missingVal.isPresent())
            params.add(new TestParamDef.MissingValue<>(missingVal.get()));

        EnumSet<TestParamDef.Syntax> syntaxes = EnumSet.of(VAL, KEY_EQ_VAL, KEY_SPACE_VAL);

        if (dfltVal.isPresent())
            params.add(new TestParamDef.ValidValue<>(DEFAULT, dfltVal.get(), syntaxes));

        for (T e : cls.getEnumConstants()) {
            params.add(new TestParamDef.ValidIdentityValue<>(e, syntaxes));
        }

        return params;
    }

    /**
     * Creates a boolean parameter definition with basic list of values to test boolean parameter with.
     *
     * @param cmdParamName The parameter keyword.
     * @param fldName The corresponding field name in the {@link SqlCommand} subclass.
     * @param fldCls The corresponding field class in the {@link SqlCommand} subclass.
     * @param dfltVal The default value to test (when parameter is specified with {@link SqlKeyword#DEFAULT} value
     *      or empty if the default value shall not be tested.
     * @param missingVal What value to expect if the parameter is missing.
     *      Empty if the missing value shall not be tested.
     * @return List of parameter value definitions.
     */
    @NotNull public static <T extends Enum<T>> TestParamDef<T> makeBasicEnumDef(String cmdParamName,
        String fldName, Class<T> fldCls, Optional<T> dfltVal, Optional<T> missingVal) {

        return new TestParamDef<>(cmdParamName, fldName, fldCls,
            ParamTestUtils.makeBasicEnumTestValues(fldCls, dfltVal, missingVal));
    }

    /**
     * Creates a basic list of values to test identifier parameter with.
     *
     * @param dfltVal The default value to test (when parameter is specified with {@link SqlKeyword#DEFAULT} value
     *      or empty if the default value shall not be tested.
     * @param missingVal What value to expect if the parameter is missing.
     *      Empty if the missing value shall not be tested.
     * @param errorFragment What fragment (string or regex) to expect in the exception
     *      if the parameter value is not a valid identifier.
     * @return List of parameter value definitions.
     */
    @NotNull public static List<TestParamDef.Value<String>> makeBasicIdTestValues(
        String[] validVals, String[] invalidVals, Optional<String> dfltVal, Optional<String> missingVal,
        String errorFragment) {

        List<TestParamDef.Value<String>> params = new LinkedList<>();

        if (missingVal.isPresent())
            params.add(new TestParamDef.MissingValue<>(missingVal.get()));

        EnumSet<TestParamDef.Syntax> syntaxes = EnumSet.of(KEY_SPACE_VAL, KEY_EQ_VAL);

        if (dfltVal.isPresent())
            params.add(new TestParamDef.ValidValue<>(DEFAULT, dfltVal.get(), syntaxes));

        for (String v : validVals) {
            params.add(new TestParamDef.ValidIdentityValue<>(v.toUpperCase(), syntaxes));
            params.add(new TestParamDef.ValidValue<>('"' + v + '"', v, syntaxes));
        }

        for (String s : invalidVals)
            params.add(new TestParamDef.InvalidValue<String>(s, errorFragment));

        return params;
    }

    /**
     * Creates a boolean parameter definition with basic list of values to test boolean parameter with.
     *
     * @param trueKeyword The keyword that specifies true value.
     * @param fldName The corresponding field name in the {@link SqlCommand} subclass.
     * @param dfltVal The default value to test (when parameter is specified with {@link SqlKeyword#DEFAULT} value
     *      or empty if the default value shall not be tested.
     * @param missingVal What value to expect if the parameter is missing.
     *      Empty if the missing value shall not be tested.
     * @return List of parameter value definitions.
     */
    @NotNull public static TestParamDef<Boolean> makeBasicBoolDef(String trueKeyword, String falseKeyword,
        String fldName, Optional<Boolean> dfltVal, Optional<Boolean> missingVal) {

        return new BoolTestParamDef(trueKeyword, falseKeyword, fldName,
            ParamTestUtils.makeBoolTestValues(dfltVal, missingVal));
    }

    /**
     * Creates a basic list of values to test boolean parameter with.
     *
     * @param dfltVal The default value to test (when parameter is specified with {@link SqlKeyword#DEFAULT} value
     *      or empty if the default value shall not be tested.
     * @param missingVal What value to expect if the parameter is missing.
     *      Empty if the missing value shall not be tested.
     * @return List of parameter value definitions.
     */
    @NotNull public static List<TestParamDef.Value<Boolean>> makeBoolTestValues(
        Optional<Boolean> dfltVal, Optional<Boolean> missingVal) {

        List<TestParamDef.Value<Boolean>> params = new LinkedList<>();

        if (missingVal.isPresent())
            params.add(new TestParamDef.MissingValue<>(missingVal.get()));

        if (dfltVal.isPresent())
            params.add(new TestParamDef.ValidValue<>(DEFAULT, dfltVal.get(), EnumSet.of(KEY_EQ_VAL)));

        EnumSet<TestParamDef.Syntax> syntaxes = EnumSet.of(KEY_WITH_OPT_NO, KEY_EQ_VAL);

        for (SqlEnumParserUtils.BooleanEnum v : SqlEnumParserUtils.BooleanEnum.values())
            params.add(new TestParamDef.ValidValue<>(v.toString(), v.toBoolean(), syntaxes));

        params.add(new TestParamDef.InvalidValue<Boolean>(BAD_BOOLEAN_VALUE,
            "re:.*Unexpected token: \"" + BAD_BOOLEAN_VALUE.toUpperCase()
                + "\".*expected:.*one of.*TRUE, FALSE.*",
            EnumSet.of(KEY_EQ_VAL)));

        return params;
    }

    /** Prevents instance creation. */
    private ParamTestUtils() {
        // Prevent instance creation
    }
}
