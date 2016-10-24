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

package org.apache.ignite.platform.javaobject;

/**
 * Test object.
 */
public class TestJavaObject {
    /** */
    protected boolean fBoolean;

    /** */
    protected byte fByte;

    /** */
    protected short fShort;

    /** */
    protected char fChar;

    /** */
    protected int fInt;

    /** */
    protected long fLong;

    /** */
    protected float fFloat;

    /** */
    protected double fDouble;

    /** */
    protected Object fObj;

    /** Integer field. */
    protected Integer fIntBoxed;

    /**
     * Default constructor.
     */
    public TestJavaObject() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param fBoolean Boolean field.
     * @param fByte Byte field.
     * @param fShort Short field.
     * @param fChar Char field.
     * @param fInt Integer field.
     * @param fLong Long field.
     * @param fDouble Double field.
     * @param fFloat Float field.
     * @param fObj Object field.
     * @param fIntBoxed Integer boxed field.
     */
    public TestJavaObject(boolean fBoolean, byte fByte, short fShort, char fChar, int fInt, long fLong, float fFloat,
        double fDouble, Object fObj, Integer fIntBoxed) {
        this.fBoolean = fBoolean;
        this.fByte = fByte;
        this.fShort = fShort;
        this.fChar = fChar;
        this.fInt = fInt;
        this.fLong = fLong;
        this.fDouble = fDouble;
        this.fFloat = fFloat;
        this.fObj = fObj;
        this.fIntBoxed = fIntBoxed;
    }

    /**
     * Set boolean field.
     *
     * @param fBoolean Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setBoolean(boolean fBoolean) {
        this.fBoolean = fBoolean;

        return this;
    }

    /**
     * Set byte field.
     *
     * @param fByte Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setByte(byte fByte) {
        this.fByte = fByte;

        return this;
    }

    /**
     * Set short field.
     *
     * @param fShort Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setShort(short fShort) {
        this.fShort = fShort;

        return this;
    }

    /**
     * Set char field.
     *
     * @param fChar Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setChar(char fChar) {
        this.fChar = fChar;

        return this;
    }

    /**
     * Set int field.
     *
     * @param fInt Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setInt(int fInt) {
        this.fInt = fInt;

        return this;
    }

    /**
     * Set long field.
     *
     * @param fLong Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setLong(long fLong) {
        this.fLong = fLong;

        return this;
    }

    /**
     * Set float field.
     *
     * @param fFloat Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setFloat(float fFloat) {
        this.fFloat = fFloat;

        return this;
    }

    /**
     * Set double field.
     *
     * @param fDouble Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setDouble(double fDouble) {
        this.fDouble = fDouble;

        return this;
    }

    /**
     * Set object field.
     *
     * @param fObj Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setObject(Object fObj) {
        this.fObj = fObj;

        return this;
    }

    /**
     * Set wrapped integer field.
     *
     * @param fInteger Value.
     * @return This instance for chaining.
     */
    public TestJavaObject setIntBoxed(Integer fInteger) {
        this.fIntBoxed = fInteger;

        return this;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass", "SimplifiableIfStatement"})
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null)
            return false;

        TestJavaObject that = (TestJavaObject) o;

        if (fBoolean != that.fBoolean)
            return false;

        if (fByte != that.fByte)
            return false;

        if (fShort != that.fShort)
            return false;

        if (fChar != that.fChar)
            return false;

        if (fInt != that.fInt)
            return false;

        if (fLong != that.fLong)
            return false;

        if (Double.compare(that.fDouble, fDouble) != 0)
            return false;

        if (Float.compare(that.fFloat, fFloat) != 0)
            return false;

        if (fObj != null ? !fObj.equals(that.fObj) : that.fObj != null)
            return false;

        return fIntBoxed != null ? fIntBoxed.equals(that.fIntBoxed) : that.fIntBoxed == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res;
        long tmp;

        res = (fBoolean ? 1 : 0);
        res = 31 * res + (int) fByte;
        res = 31 * res + (int) fShort;
        res = 31 * res + (int) fChar;
        res = 31 * res + fInt;
        res = 31 * res + (int) (fLong ^ (fLong >>> 32));

        tmp = Double.doubleToLongBits(fDouble);

        res = 31 * res + (int) (tmp ^ (tmp >>> 32));
        res = 31 * res + (fFloat != +0.0f ? Float.floatToIntBits(fFloat) : 0);
        res = 31 * res + (fObj != null ? fObj.hashCode() : 0);
        res = 31 * res + (fIntBoxed != null ? fIntBoxed.hashCode() : 0);

        return res;
    }
}
