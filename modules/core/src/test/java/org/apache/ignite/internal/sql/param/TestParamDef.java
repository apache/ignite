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

import org.apache.ignite.internal.sql.command.SqlCommand;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;

import java.util.EnumSet;
import java.util.List;

import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.KEY_EQ_VAL;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.KEY_SPACE_VAL;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.VAL;

/** Defines a parameter to test in {@link org.apache.ignite.internal.sql} tests.*/
public class TestParamDef<T> {

    /** The parameter name in SQL command. */
    private final String cmdParamName;
    /** The parameter field name in {@link SqlCommand} subclass. */
    private final String fldName;
    /** The parameter field class in {@link SqlCommand} subclass. */
    private final Class<T> fldCls;
    /** Parameter values to test. */
    private final List<Value<T>> testValues;

    /**
     * Creates a tested parameter definition.
     *
     * @param cmdParamName The parameter name in SQL command.
     * @param fldName The parameter field name in {@link SqlCommand} subclass.
     * @param fldCls The parameter field class in {@link SqlCommand} subclass.
     * @param testValues Parameter values to test.
     */
    public TestParamDef(String cmdParamName, String fldName, Class<T> fldCls, List<Value<T>> testValues) {
        this.cmdParamName = cmdParamName;
        this.fldName = fldName;
        this.fldCls = fldCls;
        this.testValues = testValues;
    }

    /**
     * Returns the parameter name in SQL command.
     * @return The parameter name in SQL command.
     */
    public String cmdParamName() {
        return cmdParamName;
    }

    /**
     * Returns the parameter field name in {@link SqlCommand} subclass.
     * @return The parameter field name in {@link SqlCommand} subclass.
     */
    public String cmdFieldName() {
        return fldName;
    }

    /** Returns the parameter field class in {@link SqlCommand} subclass.
     * @return The parameter field class in {@link SqlCommand} subclass.
     */
    public Class<?> fieldClass() {
        return fldCls;
    }

    /** Returns arameter values to test.
     * @return Parameter values to test.
     */
    public List<Value<T>> testValues() {
        return testValues;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TestParamDef{" +
            "cmdParamName='" + cmdParamName + '\'' +
            ", fldName='" + fldName + '\'' +
            ", fldCls=" + fldCls.getName() +
            '}';
    }

    /** Parameter syntax variant in SQL command. */
    public enum Syntax {
        /** Syntax: {@code [no]key} (no value, key optionally prefixed with "no"). */
        KEY_WITH_OPT_NO,

        /** Syntax: {@code value} (no key). */
        VAL,

        /** Syntax: {@code key=value}. */
        KEY_EQ_VAL,

        /** Syntax: {@code key<space>value}. */
        KEY_SPACE_VAL
    }

    /** A base class for tested parameter value definition. */
    public abstract static class Value<T> {

        /** Field value in {@link SqlCommand} subclass. */
        @GridToStringInclude
        private final T fldVal;

        /** In which kinds of {@link Syntax} this value is supported. */
        @GridToStringInclude
        private EnumSet<Syntax> supportedSyntaxes;

        /** Constructs tested parameter value definition.
         *
         * @param fldVal Field value in {@link SqlCommand} subclass.
         * @param supportedSyntaxes In which kinds of {@link Syntax} this value is supported.
         */
        public Value(T fldVal, EnumSet<Syntax> supportedSyntaxes) {
            this.fldVal = fldVal;
            this.supportedSyntaxes = supportedSyntaxes;
        }

        /**
         * Returns field value in {@link SqlCommand} subclass.
         * @return Field value in {@link SqlCommand} subclass.
         */
        public T fieldValue() {
            return fldVal;
        }

        /**
         * Returns in which kinds of {@link Syntax} this value is supported.
         * @return In which kinds of {@link Syntax} this value is supported.
         */
        public EnumSet<Syntax> supportedSyntaxes() {
            return supportedSyntaxes;
        }
    }

    /** A value for a missing parameter case (when parameter is not specified in the command). */
    public static class MissingValue<T> extends Value<T> {

        /** Creates a value for a missing parameter case (when parameter is not specified in the command). */
        public MissingValue(T fldVal) {
            super(fldVal, EnumSet.of(VAL));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(MissingValue.class, this);
        }
    }

    /** A base class for the case when the parameter is specified (correctly or not). */
    public abstract static class SpecifiedValue<T> extends Value<T> {

        /** The value specified in SQL command. */
        @GridToStringInclude
        private final String cmdVal;

        /** Creates a value for the case when the parameter is specified (correctly or not). */
        public SpecifiedValue(String cmdVal, T fldVal, EnumSet<Syntax> supportedSyntaxes) {
            super(fldVal, supportedSyntaxes);
            this.cmdVal = cmdVal;
        }

        /**
         * Returns the value to specify in SQL command.
         * @return the value to specify in SQL command. */
        public String cmdValue() {
            return cmdVal;
        }
    }

    /** An incorrect value of the parameter. */
    public static class InvalidValue<T> extends SpecifiedValue<T> {

        /** An error message fragment to look for in the exception message. */
        @GridToStringInclude
        private final String errorMsgFragment;

        /**
         * Constructs an incorrect value of the parameter with the default list of supported syntaxes
         * ({@link Syntax#KEY_EQ_VAL} and {@link Syntax#KEY_SPACE_VAL}).
         *
         * @param cmdVal value to specify in SQL command.
         * @param errorMsgFragment an error message fragment to look for in the exception message.
         */
        public InvalidValue(String cmdVal, String errorMsgFragment) {
            this(cmdVal, errorMsgFragment, EnumSet.of(KEY_EQ_VAL, KEY_SPACE_VAL));
        }

        /**
         * Constructs an incorrect value of the parameter.
         *
         * @param cmdVal value to specify in SQL command.
         * @param errorMsgFragment an error message fragment to look for in the exception message.
         * @param supportedSyntaxes in which kinds of {@link Syntax} this value is supported.
         */
        public InvalidValue(String cmdVal, String errorMsgFragment, EnumSet<Syntax> supportedSyntaxes) {
            super(cmdVal, null, supportedSyntaxes);
            this.errorMsgFragment = errorMsgFragment;
        }

        /**
         * Returns an error message fragment to look for in the exception message.
         * @return an error message fragment to look for in the exception message.
         */
        public String errorMsgFragment() {
            return errorMsgFragment;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(InvalidValue.class, this);
        }
    }

    /** Specifies a correct value of the parameter. */
    public static class ValidValue<T> extends SpecifiedValue<T> {

        /** Constructs a correct value of the parameter with the default list of supported syntaxes
         * ({@link Syntax#KEY_EQ_VAL} and {@link Syntax#KEY_SPACE_VAL}).
         *
         * @param cmdVal Value to specify in the SQL command.
         * @param fldVal Value to check in the corresponding field of the {@link SqlCommand} subclass.
         */
        public ValidValue(String cmdVal, T fldVal) {
            this(cmdVal, fldVal, EnumSet.of(KEY_EQ_VAL, KEY_SPACE_VAL));
        }

        /** Constructs a correct value of the parameter.
         *
         * @param cmdVal Value to specify in the SQL command.
         * @param fldVal Value to check in the corresponding field of the {@link SqlCommand} subclass.
         * @param supportedSyntaxes in which kinds of {@link Syntax} this value is supported.
         */
        public ValidValue(String cmdVal, T fldVal, EnumSet<Syntax> supportedSyntaxes) {
            super(cmdVal, fldVal, supportedSyntaxes);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(ValidValue.class, this);
        }
    }

    /** FIXME */
    public static class ValidIdentityValue<T> extends ValidValue<T> {

        /** FIXME */
        public ValidIdentityValue(T val) {
            super(val.toString(), val);
        }

        /** FIXME */
        public ValidIdentityValue(T val, EnumSet<Syntax> supportedSyntaxes) {
            super(val.toString(), val, supportedSyntaxes);
        }
    }

    /** FIXME */
    public static class DefValPair<T> {
        private final TestParamDef<T> def;
        private final Value<T> val;
        private final Syntax syntax;

        public DefValPair(TestParamDef<T> def, Value<T> val, Syntax syntax) {
            this.def = def;
            this.val = val;
            this.syntax = syntax;
        }

        public TestParamDef<T> def() {
            return def;
        }

        public Value<T> val() {
            return val;
        }

        public Syntax syntax() {
            return syntax;
        }
    }
}
