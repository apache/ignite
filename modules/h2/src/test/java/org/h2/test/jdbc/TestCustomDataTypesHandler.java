/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.Locale;
import org.h2.api.CustomDataTypesHandler;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDouble;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueString;

/**
 * Tests {@link CustomDataTypesHandler}.
 */
public class TestCustomDataTypesHandler extends TestBase {

    /**
     * The database name.
     */
    public final static String DB_NAME = "customDataTypes";

    /**
     * The system property name.
     */
    public final static String HANDLER_NAME_PROPERTY = "h2.customDataTypesHandler";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        System.setProperty(HANDLER_NAME_PROPERTY, TestOnlyCustomDataTypesHandler.class.getName());
        TestBase test = createCaller().init();
        test.config.traceTest = true;
        test.config.memory = true;
        test.config.networked = true;
        test.config.beforeTest();
        test.test();
        test.config.afterTest();
        System.clearProperty(HANDLER_NAME_PROPERTY);
    }

    @Override
    public void test() throws Exception {
        try {
            JdbcUtils.customDataTypesHandler = new TestOnlyCustomDataTypesHandler();

            deleteDb(DB_NAME);
            Connection conn = getConnection(DB_NAME);

            Statement stat = conn.createStatement();

            //Test cast
            ResultSet rs = stat.executeQuery("select CAST('1-1i' AS complex) + '1+1i' ");
            rs.next();
            assertTrue(rs.getObject(1).equals(new ComplexNumber(2, 0)));

            //Test create table
            stat.execute("create table t(id int, val complex)");
            rs = conn.getMetaData().getColumns(null, null, "T", "VAL");
            rs.next();
            assertEquals(rs.getString("TYPE_NAME"), "complex");
            assertEquals(rs.getInt("DATA_TYPE"), Types.JAVA_OBJECT);

            rs = stat.executeQuery("select val from t");
            assertEquals(ComplexNumber.class.getName(), rs.getMetaData().getColumnClassName(1));

            //Test insert
            PreparedStatement stmt = conn.prepareStatement(
                    "insert into t(id, val) values (0, '1.0+1.0i'), (1, ?), (2, ?), (3, ?)");
            stmt.setObject(1, new ComplexNumber(1, -1));
            stmt.setObject(2, "5.0+2.0i");
            stmt.setObject(3, 100.1);
            stmt.executeUpdate();

            //Test selects
            ComplexNumber[] expected = new ComplexNumber[4];
            expected[0] = new ComplexNumber(1, 1);
            expected[1] = new ComplexNumber(1, -1);
            expected[2] = new ComplexNumber(5, 2);
            expected[3] = new ComplexNumber(100.1, 0);

            for (int id = 0; id < expected.length; ++id) {
                PreparedStatement prepStat =conn.prepareStatement(
                        "select val from t where id = ?");
                prepStat.setInt(1, id);
                rs = prepStat.executeQuery();
                assertTrue(rs.next());
                assertTrue(rs.getObject(1).equals(expected[id]));
            }

            for (int id = 0; id < expected.length; ++id) {
                PreparedStatement prepStat = conn.prepareStatement(
                        "select id from t where val = ?");
                prepStat.setObject(1, expected[id]);
                rs = prepStat.executeQuery();
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), id);
            }

            // Repeat selects with index
            stat.execute("create index val_idx on t(val)");

            for (int id = 0; id < expected.length; ++id) {
                PreparedStatement prepStat = conn.prepareStatement(
                        "select id from t where val = ?");
                prepStat.setObject(1, expected[id]);
                rs = prepStat.executeQuery();
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), id);
            }

            // sum function
            rs = stat.executeQuery("select sum(val) from t");
            rs.next();
            assertTrue(rs.getObject(1).equals(new ComplexNumber(107.1, 2)));

            // user function
            stat.execute("create alias complex_mod for \""
                + getClass().getName() + ".complexMod\"");
            rs = stat.executeQuery("select complex_mod(val) from t where id=2");
            rs.next();
            assertEquals(complexMod(expected[2]), rs.getDouble(1));

            conn.close();
            deleteDb(DB_NAME);
        } finally {
            JdbcUtils.customDataTypesHandler = null;
        }
    }

    /**
     * The modulus function.
     *
     * @param val complex number
     * @return result
     */
    public static double complexMod(ComplexNumber val) {
        return val.mod();
    }

    /**
     * The custom data types handler to use for this test.
     */
    public static class TestOnlyCustomDataTypesHandler implements CustomDataTypesHandler {

        /** Type name for complex number */
        public final static String COMPLEX_DATA_TYPE_NAME = "complex";

        /** Type id for complex number */
        public final static int COMPLEX_DATA_TYPE_ID = 1000;

        /** Order for complex number data type */
        public final static int COMPLEX_DATA_TYPE_ORDER = 100_000;

        /** Cached DataType instance for complex number */
        public final DataType complexDataType;

        /** */
        public TestOnlyCustomDataTypesHandler() {
            complexDataType = createComplex();
        }

        @Override
        public DataType getDataTypeByName(String name) {
            if (name.toLowerCase(Locale.ENGLISH).equals(COMPLEX_DATA_TYPE_NAME)) {
                return complexDataType;
            }

            return null;
        }

        @Override
        public DataType getDataTypeById(int type) {
            if (type == COMPLEX_DATA_TYPE_ID) {
                return complexDataType;
            }
            return null;
        }

        @Override
        public String getDataTypeClassName(int type) {
            if (type == COMPLEX_DATA_TYPE_ID) {
                return ComplexNumber.class.getName();
            }
            throw DbException.get(
                    ErrorCode.UNKNOWN_DATA_TYPE_1, "type:" + type);
        }

        @Override
        public int getTypeIdFromClass(Class<?> cls) {
            if (cls == ComplexNumber.class) {
                return COMPLEX_DATA_TYPE_ID;
            }
            return Value.JAVA_OBJECT;
        }

        @Override
        public Value convert(Value source, int targetType) {
            if (source.getType() == targetType) {
                return source;
            }
            if (targetType == COMPLEX_DATA_TYPE_ID) {
                switch (source.getType()) {
                    case Value.JAVA_OBJECT: {
                        assert source instanceof ValueJavaObject;
                        return ValueComplex.get((ComplexNumber)
                                JdbcUtils.deserialize(source.getBytesNoCopy(), null));
                    }
                    case Value.STRING: {
                        assert source instanceof  ValueString;
                        return ValueComplex.get(
                                ComplexNumber.parseComplexNumber(source.getString()));
                    }
                    case Value.BYTES: {
                        assert source instanceof  ValueBytes;
                        return ValueComplex.get((ComplexNumber)
                                JdbcUtils.deserialize(source.getBytesNoCopy(), null));
                    }
                    case Value.DOUBLE: {
                        assert source instanceof ValueDouble;
                        return ValueComplex.get(new ComplexNumber(source.getDouble(), 0));
                    }
                }

                throw DbException.get(
                        ErrorCode.DATA_CONVERSION_ERROR_1, source.getString());
            } else {
                return source.convertTo(targetType);
            }
        }

        @Override
        public int getDataTypeOrder(int type) {
            if (type == COMPLEX_DATA_TYPE_ID) {
                return COMPLEX_DATA_TYPE_ORDER;
            }
            throw DbException.get(
                    ErrorCode.UNKNOWN_DATA_TYPE_1, "type:" + type);
        }

        @Override
        public Value getValue(int type, Object data, DataHandler dataHandler) {
            if (type == COMPLEX_DATA_TYPE_ID) {
                assert data instanceof ComplexNumber;
                return ValueComplex.get((ComplexNumber)data);
            }
            return ValueJavaObject.getNoCopy(data, null, dataHandler);
        }

        @Override
        public Object getObject(Value value, Class<?> cls) {
            if (cls.equals(ComplexNumber.class)) {
                if (value.getType() == COMPLEX_DATA_TYPE_ID) {
                    return value.getObject();
                }
                return convert(value, COMPLEX_DATA_TYPE_ID).getObject();
            }
            throw DbException.get(
                    ErrorCode.UNKNOWN_DATA_TYPE_1, "type:" + value.getType());
        }

        @Override
        public boolean supportsAdd(int type) {
            if (type == COMPLEX_DATA_TYPE_ID) {
                return true;
            }
            return false;
        }

        @Override
        public int getAddProofType(int type) {
            if (type == COMPLEX_DATA_TYPE_ID) {
                return type;
            }
            throw DbException.get(
                    ErrorCode.UNKNOWN_DATA_TYPE_1, "type:" + type);
        }

        /** Constructs data type instance for complex number type */
        private static DataType createComplex() {
            DataType result = new DataType();
            result.type = COMPLEX_DATA_TYPE_ID;
            result.name = COMPLEX_DATA_TYPE_NAME;
            result.sqlType = Types.JAVA_OBJECT;
            return result;
        }
    }

    /**
     * Value type implementation that holds the complex number
     */
    public static class ValueComplex extends Value {

        private ComplexNumber val;

        /**
         * @param val complex number
         */
        public ValueComplex(ComplexNumber val) {
            assert val != null;
            this.val = val;
        }

        /**
         * Get ValueComplex instance for given ComplexNumber.
         *
         * @param val complex number
         * @return resulting instance
         */
        public static ValueComplex get(ComplexNumber val) {
            return new ValueComplex(val);
        }

        @Override
        public String getSQL() {
            return val.toString();
        }

        @Override
        public int getType() {
            return TestOnlyCustomDataTypesHandler.COMPLEX_DATA_TYPE_ID;
        }

        @Override
        public long getPrecision() {
            return 0;
        }

        @Override
        public int getDisplaySize() {
            return 0;
        }

        @Override
        public String getString() {
            return val.toString();
        }

        @Override
        public Object getObject() {
            return val;
        }

        @Override
        public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
            Object obj = JdbcUtils.deserialize(getBytesNoCopy(), getDataHandler());
            prep.setObject(parameterIndex, obj, Types.JAVA_OBJECT);
        }

        @Override
        protected int compareSecure(Value v, CompareMode mode) {
            return val.compare((ComplexNumber) v.getObject());
        }

        @Override
        public int hashCode() {
            return val.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (!(other instanceof ValueComplex)) {
                return false;
            }
            ValueComplex complex = (ValueComplex)other;
            return complex.val.equals(val);
        }

        @Override
        public Value convertTo(int targetType) {
            if (getType() == targetType) {
                return this;
            }
            switch (targetType) {
                case Value.BYTES: {
                    return ValueBytes.getNoCopy(JdbcUtils.serialize(val, null));
                }
                case Value.STRING: {
                    return ValueString.get(val.toString());
                }
                case Value.DOUBLE: {
                    assert val.im == 0;
                    return ValueDouble.get(val.re);
                }
                case Value.JAVA_OBJECT: {
                    return ValueJavaObject.getNoCopy(JdbcUtils.serialize(val, null));
                }
            }

            throw DbException.get(
                    ErrorCode.DATA_CONVERSION_ERROR_1, getString());
        }

        @Override
        public Value add(Value value) {
            ValueComplex v = (ValueComplex)value;
            return ValueComplex.get(val.add(v.val));
        }
    }

    /**
     * Complex number
     */
    public static class ComplexNumber implements Serializable {
        /** */
        private static final long serialVersionUID = 1L;

        /** */
        public final static DecimalFormat REAL_FMT = new DecimalFormat("###.###");

        /** */
        public final static DecimalFormat IMG_FMT = new DecimalFormat("+###.###i;-###.###i");

        /**
         * Real part
         */
        double re;

        /**
         * Imaginary part
         */
        double im;

        /**
         * @param re real part
         * @param im imaginary part
         */
        public ComplexNumber(double re, double im) {
            this.re = re;
            this.im = im;
        }

        /**
         * Addition
         * @param other value to add
         * @return result
         */
        public ComplexNumber add(ComplexNumber other) {
            return new ComplexNumber(re + other.re, im + other.im);
        }

        /**
         * Returns modulus
         * @return result
         */
        public double mod() {
            return Math.sqrt(re * re + im * im);
        }

        /**
         * Compares two complex numbers
         *
         * True ordering of complex number has no sense,
         * so we apply lexicographical order.
         *
         * @param v number to compare this with
         * @return result of comparison
         */
        public int compare(ComplexNumber v) {
            if (re == v.re && im == v.im) {
                return 0;
            }
            if (re == v.re) {
                return im > v.im ? 1 : -1;
            } else if (re > v.re) {
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public int hashCode() {
            return (int)re | (int)im;
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (!(other instanceof ComplexNumber)) {
                return false;
            }
            ComplexNumber complex = (ComplexNumber)other;
            return (re==complex.re) && (im == complex.im);
        }

        @Override
        public String toString() {
            if (im == 0.0) {
                return REAL_FMT.format(re);
            }
            if (re == 0.0) {
                return IMG_FMT.format(im);
            }
            return REAL_FMT.format(re) + "" + IMG_FMT.format(im);
        }

        /**
         * Simple parser for complex numbers. Both real and im components
         * must be written in non scientific notation.
         * @param s String.
         * @return {@link ComplexNumber} object.
         */
        public static ComplexNumber parseComplexNumber(String s) {
            if (StringUtils.isNullOrEmpty(s))
                return null;

            s = s.replaceAll("\\s", "");

            boolean hasIm = (s.charAt(s.length() - 1) == 'i');
            int signs = 0;

            int pos = 0;

            int maxSignPos = -1;

            while (pos != -1) {
                pos = s.indexOf('-', pos);
                if (pos != -1) {
                    signs++;
                    maxSignPos = Math.max(maxSignPos, pos++);
                }
            }
            pos = 0;

            while (pos != -1) {
                pos = s.indexOf('+', pos);
                if (pos != -1) {
                    signs++;
                    maxSignPos = Math.max(maxSignPos, pos++);
                }
            }

            if (signs > 2 || (signs == 2 && !hasIm))
                throw new NumberFormatException();
            double real;
            double im;

            if (signs == 0 || (signs == 1 && maxSignPos == 0)) {
                if (hasIm) {
                    real = 0;
                    if (signs == 0 && s.length() == 1) {
                        im = 1.0;
                    } else if (signs > 0 && s.length() == 2) {
                        im = (s.charAt(0) == '-') ? -1.0 : 1.0;
                    } else {
                        im = Double.parseDouble(s.substring(0, s.length() - 1));
                    }
                } else {
                    real = Double.parseDouble(s);
                    im = 0;
                }
            } else {
                real = Double.parseDouble(s.substring(0, maxSignPos));
                if (s.length() - maxSignPos == 2) {
                    im = (s.charAt(maxSignPos) == '-') ? -1.0 : 1.0;
                } else {
                    im = Double.parseDouble(s.substring(maxSignPos, s.length() - 1));
                }
            }

            return new ComplexNumber(real, im);
        }
    }
}
