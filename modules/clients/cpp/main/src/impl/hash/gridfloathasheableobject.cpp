// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/impl/hash/gridclientfloathasheableobject.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"

/** Constants for float hashing */
class FloatConsts  {
public:
    static const int32_t SIGN_BIT_MASK = 0x8000000;
    static const int32_t EXP_BIT_MASK = 2139095040;
    static const int32_t SIGNIF_BIT_MASK = 8388607;
};

/**
 * Public constructor.
 *
 * @param pFloatVal Value to hold.
 */
GridFloatHasheableObject::GridFloatHasheableObject(float pFloatVal) {
    floatVal = pFloatVal;
}

/**
 * Returns a hash code for this <code>float</code> value.
 *
 * @return a hash code value for this object.
 */
int32_t GridFloatHasheableObject::hashCode() const {
    return floatToIntBits(floatVal);
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
int32_t GridFloatHasheableObject::floatToIntBits(float val) {
    int32_t result = floatToRawIntBits(val);

    // Check for NaN based on values of bit fields, maximum
    // exponent and nonzero significand.
    if ( ((result & FloatConsts::EXP_BIT_MASK) ==  FloatConsts::EXP_BIT_MASK) &&
         (result & FloatConsts::SIGNIF_BIT_MASK) != 0)
        result = 0x7fc00000;

    return result;
}

 /**
 * Returns the <code>float</code> value corresponding to a given
 * bit representation.
 * The argument is considered to be a representation of a
 * floating-point value according to the IEEE 754 floating-point
 * "single format" bit layout.
 * <p>
 * If the argument is <code>0x7f800000</code>, the result is positive
 * infinity.
 * <p>
 * If the argument is <code>0xff800000</code>, the result is negative
 * infinity.
 * <p>
 * If the argument is any value in the range
 * <code>0x7f800001</code> through <code>0x7fffffff</code> or in
 * the range <code>0xff800001</code> through
 * <code>0xffffffff</code>, the result is a NaN.  No IEEE 754
 * floating-point operation provided by Java can distinguish
 * between two NaN values of the same type with different bit
 * patterns.  Distinct values of NaN are only distinguishable by
 * use of the <code>Float.floatToRawIntBits</code> method.
 * <p>
 * In all other cases, let <i>s</i>, <i>e</i>, and <i>m</i> be three
 * values that can be computed from the argument:
 * <blockquote><pre>
 * int s = ((bits &gt;&gt; 31) == 0) ? 1 : -1;
 * int e = ((bits &gt;&gt; 23) & 0xff);
 * int m = (e == 0) ?
 *                 (bits & 0x7fffff) &lt;&lt; 1 :
 *                 (bits & 0x7fffff) | 0x800000;
 * </pre></blockquote>
 * Then the floating-point result equals the value of the mathematical
 * expression <i>s</i>&middot;<i>m</i>&middot;2<sup><i>e</i>-150</sup>.
 *<p>
 * Note that this method may not be able to return a
 * <code>float</code> NaN with exactly same bit pattern as the
 * <code>int</code> argument.  IEEE 754 distinguishes between two
 * kinds of NaNs, quiet NaNs and <i>signaling NaNs</i>.  The
 * differences between the two kinds of NaN are generally not
 * visible in Java.  Arithmetic operations on signaling NaNs turn
 * them into quiet NaNs with a different, but often similar, bit
 * pattern.  However, on some processors merely copying a
 * signaling NaN also performs that conversion.  In particular,
 * copying a signaling NaN to return it to the calling method may
 * perform this conversion.  So <code>intBitsToFloat</code> may
 * not be able to return a <code>float</code> with a signaling NaN
 * bit pattern.  Consequently, for some <code>int</code> values,
 * <code>floatToRawIntBits(intBitsToFloat(start))</code> may
 * <i>not</i> equal <code>start</code>.  Moreover, which
 * particular bit patterns represent signaling NaNs is platform
 * dependent; although all NaN bit patterns, quiet or signaling,
 * must be in the NaN range identified above.
 *
 * @param   bits   an integer.
 * @return  the <code>float</code> floating-point value with the same bit
 *          pattern.
 */
float GridFloatHasheableObject::intBitsToFloat(int bits) {
   union {
      int32_t i;
      float f;
    } u;

    u.i = bits;

    return u.f;
}

bool GridFloatHasheableObject::operator <(const GridFloatHasheableObject& right) const {
    return floatVal < const_cast<GridFloatHasheableObject&>(right).floatVal;
}

/**
 * Returns int value based on float bits representation.
 *
 * @param val Float value.
 * @return <code>int32_t</code> value with the same bit pattern.
 */
int32_t GridFloatHasheableObject::floatToRawIntBits(float val) {
   union {
      int32_t i;
      float f;
    } u;

    u.f = val;

    return u.i;
}

/**
 * Converts contained object to byte vector.
 *
 * @param bytes Vector to fill.
 */
void GridFloatHasheableObject::convertToBytes(std::vector<int8_t>& bytes) const {
    int32_t val = floatToRawIntBits(floatVal);

    bytes.resize(sizeof(val));
    memset(&bytes[0], 0, sizeof(val));

    GridClientByteUtils::valueToBytes(val, &bytes[0], sizeof(val), GridClientByteUtils::LITTLE_ENDIAN_ORDER);
}
