// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/impl/hash/gridclientdoublehasheableobject.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"

/** Constants for conversion */
class DoubleConsts {
public:
    //static const int64_t SIGN_BIT_MASK = -9223372036854775808LL;
    static const int64_t EXP_BIT_MASK = 9218868437227405312L;
    static const int64_t SIGNIF_BIT_MASK = 4503599627370495L;
};

/**
 * Public constructor.
 *
 * @param pDoubleVal Value to hold.
 */
GridDoubleHasheableObject::GridDoubleHasheableObject(double pDoubleVal) {
    doubleVal = pDoubleVal;
}

/**
 * Returns a hash code for this <code>Double</code> object. The
 * result is the exclusive OR of the two halves of the
 * <code>long</code> integer bit representation, exactly as
 * produced by the method {@link #doubleToLongBits(double)}, of
 * the primitive <code>double</code> value represented by this
 * <code>Double</code> object. That is, the hash code is the value
 * of the expression:
 * <blockquote><pre>
 * (int)(v^(v&gt;&gt;&gt;32))
 * </pre></blockquote>
 * where <code>v</code> is defined by:
 * <blockquote><pre>
 * long v = Double.doubleToLongBits(this.doubleValue());
 * </pre></blockquote>
 *
 * @return  a <code>hash code</code> value for this object.
 */
int32_t GridDoubleHasheableObject::hashCode() const {
    int64_t bits = doubleToLongBits(doubleVal);

    return (int32_t)(bits ^ (bits >> 32));
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
int64_t GridDoubleHasheableObject::doubleToLongBits(double doubleVal) {
    int64_t result = doubleToRawLongBits(doubleVal);

    // Check for NaN based on values of bit fields, maximum
    // exponent and nonzero significant.
    if ( ((result & DoubleConsts::EXP_BIT_MASK) == DoubleConsts::EXP_BIT_MASK) &&
         (result & DoubleConsts::SIGNIF_BIT_MASK) != 0L)
        result = 0x7ff8000000000000LL;

    return result;
}

/**
 * Returns the <code>double</code> value corresponding to a given
 * bit representation.
 * The argument is considered to be a representation of a
 * floating-point value according to the IEEE 754 floating-point
 * "double format" bit layout.
 * <p>
 * If the argument is <code>0x7ff0000000000000L</code>, the result
 * is positive infinity.
 * <p>
 * If the argument is <code>0xfff0000000000000L</code>, the result
 * is negative infinity.
 * <p>
 * If the argument is any value in the range
 * <code>0x7ff0000000000001L</code> through
 * <code>0x7fffffffffffffffL</code> or in the range
 * <code>0xfff0000000000001L</code> through
 * <code>0xffffffffffffffffL</code>, the result is a NaN.  No IEEE
 * 754 floating-point operation provided by Java can distinguish
 * between two NaN values of the same type with different bit
 * patterns.  Distinct values of NaN are only distinguishable by
 * use of the <code>Double.doubleToRawLongBits</code> method.
 * <p>
 * In all other cases, let <i>s</i>, <i>e</i>, and <i>m</i> be three
 * values that can be computed from the argument:
 * <blockquote><pre>
 * int s = ((bits &gt;&gt; 63) == 0) ? 1 : -1;
 * int e = (int)((bits &gt;&gt; 52) & 0x7ffL);
 * long m = (e == 0) ?
 *                 (bits & 0xfffffffffffffL) &lt;&lt; 1 :
 *                 (bits & 0xfffffffffffffL) | 0x10000000000000L;
 * </pre></blockquote>
 * Then the floating-point result equals the value of the mathematical
 * expression <i>s</i>&middot;<i>m</i>&middot;2<sup><i>e</i>-1075</sup>.
 *<p>
 * Note that this method may not be able to return a
 * <code>double</code> NaN with exactly same bit pattern as the
 * <code>long</code> argument.  IEEE 754 distinguishes between two
 * kinds of NaNs, quiet NaNs and <i>signaling NaNs</i>.  The
 * differences between the two kinds of NaN are generally not
 * visible in Java.  Arithmetic operations on signaling NaNs turn
 * them into quiet NaNs with a different, but often similar, bit
 * pattern.  However, on some processors merely copying a
 * signaling NaN also performs that conversion.  In particular,
 * copying a signaling NaN to return it to the calling method
 * may perform this conversion.  So <code>longBitsToDouble</code>
 * may not be able to return a <code>double</code> with a
 * signaling NaN bit pattern.  Consequently, for some
 * <code>long</code> values,
 * <code>doubleToRawLongBits(longBitsToDouble(start))</code> may
 * <i>not</i> equal <code>start</code>.  Moreover, which
 * particular bit patterns represent signaling NaNs is platform
 * dependent; although all NaN bit patterns, quiet or signaling,
 * must be in the NaN range identified above.
 *
 * @param   bits   any <code>long</code> integer.
 * @return  the <code>double</code> floating-point value with the same
 *          bit pattern.
 */
double GridDoubleHasheableObject::longBitsToDouble(const int64_t& bits) {
    union {
        int64_t longVal;
        double d;
    } u;

    u.longVal  = bits;

    return u.d;
}

bool GridDoubleHasheableObject::operator <(const GridDoubleHasheableObject& right) const {
    return doubleVal < const_cast<GridDoubleHasheableObject&>(right).doubleVal;
}

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
int64_t GridDoubleHasheableObject::doubleToRawLongBits(double val) {
    union {
        int64_t longVal;
        double d;
    } u;

    u.d  = val;

    return u.longVal;
}

/**
 * Converts contained object to byte vector.
 *
 * @param bytes Vector to fill.
 */
void GridDoubleHasheableObject::convertToBytes(std::vector<int8_t>& bytes) const {
    int64_t val = doubleToRawLongBits(doubleVal);

    bytes.resize(sizeof(val));
    memset(&bytes[0],0,sizeof(val));

    GridClientByteUtils::valueToBytes(val, &bytes[0], sizeof(val), GridClientByteUtils::LITTLE_ENDIAN_ORDER);
}
