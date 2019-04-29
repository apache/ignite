/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.Arrays;
import java.util.Locale;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * Extended parameters of the ENUM data type.
 */
public final class ExtTypeInfoEnum extends ExtTypeInfo {

    private final String[] enumerators, cleaned;

    private TypeInfo type;

    /**
     * Returns enumerators for the two specified values for a binary operation.
     *
     * @param left
     *            left (first) operand
     * @param right
     *            right (second) operand
     * @return enumerators from the left or the right value, or an empty array
     *         if both values do not have enumerators
     */
    public static ExtTypeInfoEnum getEnumeratorsForBinaryOperation(Value left, Value right) {
        if (left.getValueType() == Value.ENUM) {
            return ((ValueEnum) left).getEnumerators();
        } else if (right.getValueType() == Value.ENUM) {
            return ((ValueEnum) right).getEnumerators();
        } else {
            throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1,
                    "type1=" + left.getValueType() + ", type2=" + right.getValueType());
        }
    }

    private static String sanitize(String label) {
        return label == null ? null : label.trim().toUpperCase(Locale.ENGLISH);
    }

    private static String toSQL(String[] enumerators) {
        StringBuilder result = new StringBuilder();
        result.append('(');
        for (int i = 0; i < enumerators.length; i++) {
            if (i != 0) {
                result.append(", ");
            }
            result.append('\'');
            String s = enumerators[i];
            for (int j = 0, length = s.length(); j < length; j++) {
                char c = s.charAt(j);
                if (c == '\'') {
                    result.append('\'');
                }
                result.append(c);
            }
            result.append('\'');
        }
        result.append(')');
        return result.toString();
    }

    /**
     * Creates new instance of extended parameters of the ENUM data type.
     *
     * @param enumerators
     *            the enumerators. May not be modified by caller or this class.
     */
    public ExtTypeInfoEnum(String[] enumerators) {
        if (enumerators == null || enumerators.length == 0) {
            throw DbException.get(ErrorCode.ENUM_EMPTY);
        }
        final String[] cleaned = new String[enumerators.length];
        for (int i = 0; i < enumerators.length; i++) {
            String l = sanitize(enumerators[i]);
            if (l == null || l.isEmpty()) {
                throw DbException.get(ErrorCode.ENUM_EMPTY);
            }
            for (int j = 0; j < i; j++) {
                if (l.equals(cleaned[j])) {
                    throw DbException.get(ErrorCode.ENUM_DUPLICATE, toSQL(enumerators));
                }
            }
            cleaned[i] = l;
        }
        this.enumerators = enumerators;
        this.cleaned = Arrays.equals(cleaned, enumerators) ? enumerators : cleaned;
    }

    TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            int p = 0;
            for (String s : enumerators) {
                int l = s.length();
                if (l > p) {
                    p = l;
                }
            }
            this.type = type = new TypeInfo(Value.ENUM, p, 0, p, this);
        }
        return type;
    }

    @Override
    public Value cast(Value value) {
        switch (value.getValueType()) {
        case Value.ENUM:
            if (value instanceof ValueEnum && ((ValueEnum) value).getEnumerators().equals(this)) {
                return value;
            }
            //$FALL-THROUGH$
        case Value.STRING:
        case Value.STRING_FIXED:
        case Value.STRING_IGNORECASE:
            ValueEnum v = getValueOrNull(value.getString());
            if (v != null) {
                return v;
            }
            break;
        default:
            int ordinal = value.getInt();
            if (ordinal >= 0 && ordinal < enumerators.length) {
                return new ValueEnum(this, enumerators[ordinal], ordinal);
            }
        }
        String s = value.getTraceSQL();
        if (s.length() > 127) {
            s = s.substring(0, 128) + "...";
        }
        throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED, toString(), s);
    }

    /**
     * Returns an enumerator with specified 0-based ordinal value.
     *
     * @param ordinal
     *            ordinal value of an enumerator
     * @return the enumerator with specified ordinal value
     */
    public String getEnumerator(int ordinal) {
        return enumerators[ordinal];
    }

    /**
     * Get ValueEnum instance for an ordinal.
     * @param ordinal ordinal value of an enum
     * @return ValueEnum instance
     */
    public ValueEnum getValue(int ordinal) {
        if (ordinal < 0 || ordinal >= enumerators.length) {
            throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED, enumerators.toString(),
                    Integer.toString(ordinal));
        }
        return new ValueEnum(this, enumerators[ordinal], ordinal);
    }

    /**
     * Get ValueEnum instance for a label string.
     * @param label label string
     * @return ValueEnum instance
     */
    public ValueEnum getValue(String label) {
        ValueEnum value = getValueOrNull(label);
        if (value == null) {
            throw DbException.get(ErrorCode.ENUM_VALUE_NOT_PERMITTED, toString(), label);
        }
        return value;
    }

    private ValueEnum getValueOrNull(String label) {
        String l = sanitize(label);
        if (l != null) {
            for (int ordinal = 0; ordinal < cleaned.length; ordinal++) {
                if (l.equals(cleaned[ordinal])) {
                    return new ValueEnum(this, enumerators[ordinal], ordinal);
                }
            }
        }
        return null;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(enumerators) + 203_117;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != ExtTypeInfoEnum.class) {
            return false;
        }
        return Arrays.equals(enumerators, ((ExtTypeInfoEnum) obj).enumerators);
    }

    @Override
    public String getCreateSQL() {
        return toSQL(enumerators);
    }

}
