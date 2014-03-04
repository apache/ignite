/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTWIDESTRINGHASHEABLEOBJECT_HPP_
#define GRIDCLIENTWIDESTRINGHASHEABLEOBJECT_HPP_

/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_STRINGHASHEABLEOBJECT_HPP_INCLUDED
#define GRID_CLIENT_STRINGHASHEABLEOBJECT_HPP_INCLUDED

#include <cstdint>

#include <string>
#include <vector>

#include "gridgain/gridhasheableobject.hpp"

/**
 * Provides the implementation of hash code for wide strings.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridWideStringHasheableObject : public GridHasheableObject {
public:
    /**
     * Public constructor.
     *
     * @param s Value to hold.
     */
    GridWideStringHasheableObject(const std::wstring& s);

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
     * Compares with another object of this type.
     *
     * @return True if the right operand is greater than this object.
     */
    virtual bool operator<(const GridWideStringHasheableObject& right) const {
        return str < const_cast<GridWideStringHasheableObject&>(right).str;
    }

protected:
    /** Wrapped value. */
    std::wstring str;
};

#endif



#endif /* GRIDCLIENTWIDESTRINGHASHEABLEOBJECT_HPP_ */
