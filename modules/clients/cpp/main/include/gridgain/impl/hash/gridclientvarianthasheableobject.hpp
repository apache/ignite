/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_VARIANT_HASHEABLE_OBJECT_HPP_INCLUDED
#define GRID_CLIENT_VARIANT_HASHEABLE_OBJECT_HPP_INCLUDED

#include <vector>

#include "gridgain/gridhasheableobject.hpp"
#include "gridgain/gridclientvariant.hpp"

/**
 * Provide the implementation of hash-code for GridClientVariant.
 */
class GridClientVariantHasheableObject : public GridClientHasheableObject {
public:
    /**
     * Public constructor.
     *
     * @param var Value to hold.
     */
    GridClientVariantHasheableObject(const GridClientVariant& var);

    /**
     * Calculates hash code for the contained object.
     *
     * @return Hash code.
     */
    virtual int32_t hashCode() const { return hashCode_; }

    /**
     * Converts contained object to byte vector.
     *
     * @param opBytes Vector to fill.
     */
    virtual void convertToBytes(std::vector<int8_t>& opBytes) const {
        opBytes = bytes;
    }
protected:
    /**
     * Assign another value to same object.
     *
     * @param var New value.
     */
    void init(const GridClientVariant& var);

    /** Stored hash code. */
    int32_t hashCode_;

    /** Byte representation of the object. */
    std::vector<int8_t> bytes;
};

#endif
