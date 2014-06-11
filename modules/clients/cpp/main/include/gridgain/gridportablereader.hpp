/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDPORTABLEREADER_HPP_INCLUDED
#define GRIDPORTABLEREADER_HPP_INCLUDED

#include <string>
#include <vector>
#include <unordered_map>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableReader {
public:
    virtual int32_t readInt32(char* fieldName) = 0;

    virtual std::vector<int8_t> readBytes(char* fieldName) = 0;

    virtual std::string readString(char* fieldName) = 0;

    virtual GridClientVariant readVariant(char* fieldName) = 0;

    virtual std::vector<GridClientVariant> readCollection(char* fieldName) = 0;

    virtual std::unordered_map<GridClientVariant, GridClientVariant> readMap(char* fieldName) = 0;
};

#endif // GRIDPORTABLEREADER_HPP_INCLUDED
