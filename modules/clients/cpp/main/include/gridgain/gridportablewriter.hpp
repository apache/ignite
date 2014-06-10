/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDPORTABLEWRITER_HPP_INCLUDED
#define GRIDPORTABLEWRITER_HPP_INCLUDED

#include <string>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>

using namespace std;

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableWriter {
public:
    virtual void writeInt(char* fieldName, int32_t val) = 0;

    virtual void writeString(char* fieldName, const string &val) = 0;

    virtual void writeVariant(char* fieldName, const GridClientVariant &str) = 0;
};

#endif // GRIDPORTABLEWRITER_HPP_INCLUDED
