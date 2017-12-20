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

import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.testframework.StringOrPattern;

import java.util.EnumSet;
import java.util.List;

import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.KEY_EQ_VAL;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.KEY_SPACE_VAL;
import static org.apache.ignite.internal.sql.param.TestParamDef.Syntax.VAL;

/** FIXME */
public class TestParamDef<T> {

    /** FIXME */
    private final String cmdParamName;
    /** FIXME */
    private final String fldName;
    /** FIXME */
    private final Class<T> fldCls;
    /** FIXME */
    private final List<Value<T>> testValues;

    /** FIXME */
    public TestParamDef(String cmdParamName, String fldName, Class<T> fldCls, List<Value<T>> testValues) {
        this.cmdParamName = cmdParamName;
        this.fldName = fldName;
        this.fldCls = fldCls;
        this.testValues = testValues;
    }

    /** FIXME */
    public String cmdParamName() {
        return cmdParamName;
    }

    /** FIXME */
    public String cmdFieldName() {
        return fldName;
    }

    /** FIXME */
    public Class<?> fieldClass() {
        return fldCls;
    }

    /** FIXME */
    public List<Value<T>> testValues() {
        return testValues;
    }

    /** FIXME */
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

    /** FIXME */
    public abstract static class Value<T> {

        /** FIXME */
        @GridToStringInclude
        private final T fldVal;

        /** FIXME */
        @GridToStringInclude
        private EnumSet<Syntax> supportedSyntaxes;

        /** FIXME */
        @GridToStringInclude
        private boolean isValidWithoutKey;

        /** FIXME */
        public Value(T fldVal, EnumSet<Syntax> supportedSyntaxes) {
            this.fldVal = fldVal;
            this.supportedSyntaxes = supportedSyntaxes;
        }

        /** FIXME */
        public T fieldValue() {
            return fldVal;
        }

        /** FIXME */
        public EnumSet<Syntax> supportedSyntaxes() {
            return supportedSyntaxes;
        }
    }

    /** FIXME */
    public static class MissingValue<T> extends Value<T> {

        /** FIXME */
        public MissingValue(T fldVal) {
            super(fldVal, EnumSet.of(VAL));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(MissingValue.class, this);
        }
    }

    /** FIXME */
    public abstract static class SpecifiedValue<T> extends Value<T> {

        /** FIXME */
        @GridToStringInclude
        private final String cmdVal;

        /** FIXME */
        public SpecifiedValue(String cmdVal, T fldVal, EnumSet<Syntax> supportedSyntaxes) {
            super(fldVal, supportedSyntaxes);
            this.cmdVal = cmdVal;
        }

        /** FIXME */
        public String cmdValue() {
            return cmdVal;
        }
    }

    /** FIXME */
    public static class InvalidValue<T> extends SpecifiedValue<T> {

        /** FIXME */
        @GridToStringInclude
        private final StringOrPattern errorMsgFragment;

        /** FIXME */
        public InvalidValue(String cmdVal, StringOrPattern errorMsgFragment) {
            this(cmdVal, errorMsgFragment, EnumSet.of(KEY_EQ_VAL, KEY_SPACE_VAL));
        }

        /** FIXME */
        public InvalidValue(String cmdVal, StringOrPattern errorMsgFragment, EnumSet<Syntax> supportedSyntaxes) {
            super(cmdVal, null, supportedSyntaxes);
            this.errorMsgFragment = errorMsgFragment;
        }

        /** FIXME */
        public StringOrPattern errorMsgFragment() {
            return errorMsgFragment;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return GridToStringBuilder.toString(InvalidValue.class, this);
        }
    }

    /** FIXME */
    public static class ValidValue<T> extends SpecifiedValue<T> {

        /** FIXME */
        public ValidValue(String cmdVal, T fldVal) {
            this(cmdVal, fldVal, EnumSet.of(KEY_EQ_VAL, KEY_SPACE_VAL));
        }

        /** FIXME */
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
