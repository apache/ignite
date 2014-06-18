/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/gridclienthash.hpp"

const int32_t FLOAT_SIGN_BIT_MASK = 0x8000000;
const int32_t FLOAT_EXP_BIT_MASK = 2139095040;
const int32_t FLOAT_SIGNIF_BIT_MASK = 8388607;

/**
 * Returns int value based on float bits representation.
 *
 * @param val Float value.
 * @return <code>int32_t</code> value with the same bit pattern.
 */
int32_t floatToRawIntBits(float val) {
   union {
      int32_t i;
      float f;
    } u;

    u.f = val;

    return u.i;
}

/**
 * Returns a representation of the specified floating-point value
 * according to the IEEE 754 floating-point "single format" bit
 * layout.
 * <p>
 * Bit 31 (the bit that is selected by the mask
 * <code>0x80000000</code>) represents the sign of the floating-point
 * number.
 * Bits 30-23 (the bits that are selected by the mask
 * <code>0x7f800000</code>) represent the exponent.
 * Bits 22-0 (the bits that are selected by the mask
 * <code>0x007fffff</code>) represent the significand (sometimes called
 * the mantissa) of the floating-point number.
 * <p>If the argument is positive infinity, the result is
 * <code>0x7f800000</code>.
 * <p>If the argument is negative infinity, the result is
 * <code>0xff800000</code>.
 * <p>If the argument is NaN, the result is <code>0x7fc00000</code>.
 * <p>
 * In all cases, the result is an integer that, when given to the
 * {@link #intBitsToFloat(int)} method, will produce a floating-point
 * value the same as the argument to <code>floatToIntBits</code>
 * (except all NaN values are collapsed to a single
 * &quot;canonical&quot; NaN value).
 *
 * @param   value   a floating-point number.
 * @return the bits that represent the floating-point number.
 */
int32_t floatToIntBits(float val) {
    int32_t result = floatToRawIntBits(val);

    // Check for NaN based on values of bit fields, maximum
    // exponent and nonzero significand.
    if ( ((result & FLOAT_EXP_BIT_MASK) ==  FLOAT_EXP_BIT_MASK) &&
         (result & FLOAT_SIGNIF_BIT_MASK) != 0)
        result = 0x7fc00000;

    return result;
}

const int64_t DOUBLE_EXP_BIT_MASK = 9218868437227405312L;
const int64_t DOUBLE_SIGNIF_BIT_MASK = 4503599627370495L;

/**
 * Returns a representation of the specified floating-point value
 * according to the IEEE 754 floating-point "double
 * format" bit layout, preserving Not-a-Number (NaN) values.
 * <p>
 * Bit 63 (the bit that is selected by the mask
 * <code>0x8000000000000000L</code>) represents the sign of the
 * floating-point number. Bits
 * 62-52 (the bits that are selected by the mask
 * <code>0x7ff0000000000000L</code>) represent the exponent. Bits 51-0
 * (the bits that are selected by the mask
 * <code>0x000fffffffffffffL</code>) represent the significant
 * (sometimes called the mantissa) of the floating-point number.
 * <p>
 * If the argument is positive infinity, the result is
 * <code>0x7ff0000000000000L</code>.
 * <p>
 * If the argument is negative infinity, the result is
 * <code>0xfff0000000000000L</code>.
 * <p>
 * If the argument is NaN, the result is the <code>long</code>
 * integer representing the actual NaN value.  Unlike the
 * <code>doubleToLongBits</code> method,
 * <code>doubleToRawLongBits</code> does not collapse all the bit
 * patterns encoding a NaN to a single &quot;canonical&quot; NaN
 * value.
 * <p>
 * In all cases, the result is a <code>long</code> integer that,
 * when given to the {@link #longBitsToDouble(long)} method, will
 * produce a floating-point value the same as the argument to
 * <code>doubleToRawLongBits</code>.
 *
 * @param   value   a <code>double</code> precision floating-point number.
 * @return the bits that represent the floating-point number.
 *
 */
int64_t doubleToRawLongBits(double val) {
    union {
        int64_t longVal;
        double d;
    } u;

    u.d  = val;

    return u.longVal;
}

/**
 * Returns a representation of the specified floating-point value
 * according to the IEEE 754 floating-point "double
 * format" bit layout.
 * <p>
 * Bit 63 (the bit that is selected by the mask
 * <code>0x8000000000000000L</code>) represents the sign of the
 * floating-point number. Bits
 * 62-52 (the bits that are selected by the mask
 * <code>0x7ff0000000000000L</code>) represent the exponent. Bits 51-0
 * (the bits that are selected by the mask
 * <code>0x000fffffffffffffL</code>) represent the significand
 * (sometimes called the mantissa) of the floating-point number.
 * <p>
 * If the argument is positive infinity, the result is
 * <code>0x7ff0000000000000L</code>.
 * <p>
 * If the argument is negative infinity, the result is
 * <code>0xfff0000000000000L</code>.
 * <p>
 * If the argument is NaN, the result is
 * <code>0x7ff8000000000000L</code>.
 * <p>
 * In all cases, the result is a <code>long</code> integer that, when
 * given to the {@link #longBitsToDouble(long)} method, will produce a
 * floating-point value the same as the argument to
 * <code>doubleToLongBits</code> (except all NaN values are
 * collapsed to a single &quot;canonical&quot; NaN value).
 *
 * @param  doubleVal a <code>double</code> precision floating-point number.
 * @return the bits that represent the floating-point number.
 */
int64_t doubleToLongBits(double doubleVal) {
    int64_t result = doubleToRawLongBits(doubleVal);

    // Check for NaN based on values of bit fields, maximum
    // exponent and nonzero significant.
    if ( ((result & DOUBLE_EXP_BIT_MASK) == DOUBLE_EXP_BIT_MASK) &&
         (result & DOUBLE_SIGNIF_BIT_MASK) != 0L)
        result = 0x7ff8000000000000LL;

    return result;
}

int32_t gridBoolHash(bool val) {
    return val ? 1231 : 1237;
}

int32_t gridByteHash(int8_t val) {
    return (int32_t)val;
}

int32_t gridInt16Hash(int16_t val) {
    return (int32_t)val;
}

int32_t gridInt32Hash(int32_t val) {
    return val;
}

int32_t gridInt64Hash(int64_t val) {
    return (int32_t)(val ^ (val >> 32));
}

int32_t gridStringHash(const std::string& str) {
    int len = str.length();

    int32_t hash = 0;

    const char* val = str.c_str();

    for (int i = 0; i < len; i++)
        hash = 31 * hash + val[i];

    return hash;
}

int32_t gridWStringHash(const std::wstring& str) {
    int len = str.length();

    int32_t hash = 0;
    
    const wchar_t* val = str.c_str();

    for (int i = 0; i < len; i++)
        hash = 31 * hash + val[i];

    return hash;
}

int32_t gridFloatHash(float val) {
    return floatToIntBits(val);
}

int32_t gridDoubleHash(double val) {
    int64_t bits = doubleToLongBits(val);

    return (int32_t)(bits ^ (bits >> 32));
}

int32_t gridBytesHash(const std::vector<int8_t>& val) {
    int32_t hash = 1;

    for (size_t i = 0; i < val.size(); ++i)
        hash = 31 * hash + val[i];

    return hash;
}
