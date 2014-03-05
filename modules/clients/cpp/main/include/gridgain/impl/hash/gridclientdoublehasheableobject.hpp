/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_DOUBLE_HASHEABLEOBJECT_HPP_INCLUDED
#define GRID_CLIENT_DOUBLE_HASHEABLEOBJECT_HPP_INCLUDED

#include <cstdint>

#include <string>
#include <vector>

#include "gridgain/gridhasheableobject.hpp"

/**
 * Provide the implementation of hash-code for the double type.
 */
class GridDoubleHasheableObject : public GridHasheableObject {
public:
    /** Public constructor.
     *
     * @param pDoubleVal Value to hold.
     */
    GridDoubleHasheableObject(double pDoubleVal);

    /**
     * Calculates hash code for the contained object.
     *
     * @return Hash code.
     */
    virtual int32_t hashCode() const;

    /**
     * Converts contained object to byte vector.
     *
     * @param bytes Vector to fill.
     */
    virtual void convertToBytes(std::vector<int8_t>& bytes) const;

    /**
     * Returns a representation of the specified floating-point value
     * according to the IEEE 754 floating-point "double
     * format" bit layout.
     *
     * @param  doubleVal a <code>double</code> precision floating-point number.
     * @return the bits that represent the floating-point number.
     */
    static int64_t doubleToLongBits(double doubleVal);

    /**
     * Returns the <code>double</code> value corresponding to a given
     * bit representation.
     *
     * @param   bits   any <code>long</code> integer.
     * @return  the <code>double</code> floating-point value with the same
     *          bit pattern.
     */
    static double longBitsToDouble(const int64_t& bits);

    /**
     * Compares with another object of this type.
     *
     * @return True if the right operand is greater than this object.
     */
    virtual bool operator<(const GridDoubleHasheableObject& right) const;
protected:
    /**
     * Returns a representation of the specified floating-point value
     * according to the IEEE 754 floating-point "double
     * format" bit layout, preserving Not-a-Number (NaN) values.
     *
     * @param   value   a <code>double</code> precision floating-point number.
     * @return the bits that represent the floating-point number.
     */
    static int64_t doubleToRawLongBits(double val);

    /** Value stored. */
    double doubleVal;
};

#endif
