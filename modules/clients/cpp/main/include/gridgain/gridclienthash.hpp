/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTHASH_HPP_INCLUDED
#define GRIDCLIENTHASH_HPP_INCLUDED

#include <string>
#include <vector>

#include <gridgain/gridcommon.hpp>

int32_t gridBoolHash(bool val);

int32_t gridByteHash(int8_t val);

int32_t gridInt16Hash(int16_t val);

int32_t gridInt32Hash(int32_t val);

int32_t gridInt64Hash(int64_t val);

int32_t gridFloatHash(float val);

int32_t gridDoubleHash(double val);

int32_t gridStringHash(const std::string& val);

int32_t gridWStringHash(const std::wstring& val);

int32_t gridBytesHash(const std::vector<int8_t>& val); 

#endif // GRIDCLIENTHASH_HPP_INCLUDED
