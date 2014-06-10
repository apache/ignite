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

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>

using namespace std;

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableReader {
public:
    virtual int32_t readInt(char* fieldName) = 0;

    virtual string readString(char* fieldName) = 0;

    virtual GridClientVariant readVariant(char* fieldName) = 0;
};

#endif // GRIDPORTABLEREADER_HPP_INCLUDED
