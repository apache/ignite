/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_SIMPLE_TYPE_HASHEABLE_HPP_INCLUDED
#define GRID_CLIENT_SIMPLE_TYPE_HASHEABLE_HPP_INCLUDED

#include <memory>

#include "gridgain/gridhasheableobject.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"

typedef std::shared_ptr<GridHasheableObject> TGridHasheableObjectPtr;

/**
 * Template class for GridHashealeObject for simple types like int_...
 */
template<class T> class GridSimpleTypeHasheable: public GridHasheableObject {
protected:
    /**
     * Constructor.
     *
     * @param pVal Value to hold.
     */
    GridSimpleTypeHasheable(const T& pVal) :
            val(pVal) {}

public:
    /**
     * Converts contained object to byte vector.
     *
     * @param bytes Vector to fill.
     */
    virtual void convertToBytes(std::vector < int8_t >& bytes) const {
        bytes.resize(sizeof(val));
        memset(&bytes[0], 0, sizeof(val));

        GridClientByteUtils::valueToBytes(val, &bytes[0], sizeof(val), GridClientByteUtils::LITTLE_ENDIAN_ORDER);
    }

    /**
     * Converts a collection of bytes back to the requested type.
     *
     * @param buffer Byte array.
     * @param nBytes Number of bytes to use in restoring value.
     * @return Restored value.
     */
    static T get(const int8_t* buffer, size_t nBytes) {
        T res;

        GridClientByteUtils::bytesToValue(buffer, nBytes, res, GridClientByteUtils::LITTLE_ENDIAN_ORDER);

        return res;
    }

    /**
     * Compares with another object of this type.
     *
     * @return True if the right operand is greater than this object.
     */
    virtual bool operator<(const GridSimpleTypeHasheable<T>& right) const {
        return val < const_cast<GridSimpleTypeHasheable<T>&>(right).val;
    }

    /**
     * Prints GridSimpleTypeHasheable to the output stream.
     *
     * @param out Stream to output the object to.
     * @param o The object.
     */
    friend std::ostream& operator<< (std::ostream &out, const GridSimpleTypeHasheable<T> &o) {
        return out << o.val;
    }
protected:
    T val;
};

/**
 * Hasheable 64-bit signed integer.
 */
class GridInt64Hasheable: public GridSimpleTypeHasheable<int64_t> {
public:
    /**
     * Constructor.
     */
    GridInt64Hasheable(int64_t val): GridSimpleTypeHasheable<int64_t>(val) {}

    /**
     * Calculates hash code for the contained object.
     *
     * @return Hash code.
     */
    virtual int32_t hashCode() const {
        return (int32_t)(val ^ (val >> 32));
    }
};

/**
 * Creates an instance of GridInt64Hasheable.
 *
 * @return Shared pointer to a newly-created instance.
 */
inline TGridHasheableObjectPtr createHasheable(int64_t val) {
    return TGridHasheableObjectPtr(new GridInt64Hasheable(val));
}

/**
 * Hasheable 32-bit signed integer.
 */
class GridInt32Hasheable: public GridSimpleTypeHasheable<int32_t> {
public:
    /**
     * Constructor.
     */
    GridInt32Hasheable(int32_t val): GridSimpleTypeHasheable<int32_t>(val) {}

    /**
     * Calculates hash code for the contained object.
     *
     * @return Hash code.
     */
    virtual int32_t hashCode() const {
        return val;
    }
};

/**
 * Creates an instance of GridInt32Hasheable.
 *
 * @return Shared pointer to a newly-created instance.
 */
inline TGridHasheableObjectPtr createHasheable(int32_t val) {
    return TGridHasheableObjectPtr(new GridInt32Hasheable(val));
}


#endif
