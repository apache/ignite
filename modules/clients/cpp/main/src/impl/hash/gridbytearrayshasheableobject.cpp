// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <cassert>
#include <cstddef>

#include "gridgain/impl/hash/gridclientbytearrayshasheableobject.hpp"

/**
 * Public constructor.
 *
 * @param bytes Value to hold.
 */
GridByteArrayHasheableObject::GridByteArrayHasheableObject(std::vector<int8_t> buf) {
    hashCodeVal = 1;

    for (size_t i = 0; i < buf.size(); ++i)
        hashCodeVal = 31 * hashCodeVal + buf[i];

    data = buf;
}

/**
 * Public constructor.
 *
 * @param bytes Value to hold.
 */
int32_t GridByteArrayHasheableObject::hashCode() const {
    return hashCodeVal;
}

/**
 * Converts contained object to byte vector.
 *
 * @param bytes Vector to fill.
 */
void GridByteArrayHasheableObject::convertToBytes(std::vector<int8_t>& bytes) const {
    bytes.clear();

    bytes = data;
}
