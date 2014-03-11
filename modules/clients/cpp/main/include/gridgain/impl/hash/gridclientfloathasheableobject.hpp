/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_FLOAT_HASHEABLEOBJECT_HPP_INCLUDED
#define GRID_CLIENT_FLOAT_HASHEABLEOBJECT_HPP_INCLUDED

#include <cstdint>

#include <string>
#include <vector>

#include "gridgain/gridhasheableobject.hpp"

/**
 * Provide the implementation of hash-code for the float type.
 */
class GridFloatHasheableObject : public GridClientHasheableObject {
public:
    /**
     * Public constructor.
     *
     * @param pFloatVal Value to hold.
     */
    GridFloatHasheableObject(float pFloat);

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
     * Returns <code>int</code> value based on <code>float</code> bits representation.
     *
     * @param val <code>float</code> value.
     * @return <code>int</code> value with the same bit pattern.
     */
    static int32_t floatToIntBits(float val);

    /**
     * Returns <code>float</code> value based on <code>int</code> bits representation.
     *
     * @param bits <code>int</code> value.
     * @return <code>float</code> value with the same bit pattern.
     */
    static float intBitsToFloat(int bits);

    /**
     * Compares with another object of this type.
     *
     * @return True if the right operand is greater than this object.
     */
    virtual bool operator<(const GridFloatHasheableObject& right) const;
protected:
    /**
     * Returns <code>int</code> value based on <code>float</code> bits representation.
     *
     * @param val <code>float</code> value.
     * @return <code>int</code> value with the same bit pattern.
     */
    static int floatToRawIntBits(float val);

    /** Value held. */
    float floatVal;
};

#endif
