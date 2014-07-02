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

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridBoolHash(bool val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridByteHash(int8_t val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridInt16Hash(int16_t val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridInt32Hash(int32_t val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridInt64Hash(int64_t val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridFloatHash(float val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridDoubleHash(double val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridStringHash(const std::string& val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridWStringHash(const std::wstring& val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
int32_t gridByteArrayHash(const std::vector<int8_t>& val); 

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
template<typename T>
int32_t gridHashCode(const T& val);

/**
 * Caclulates java-compatible hash code.
 *
 * @param val Value.
 * @return Hash code.
*/
template<typename T>
int32_t gridCollectionHash(const std::vector<T>& val) {
    int32_t hash = 1;

    for (size_t i = 0; i < val.size(); ++i)
        hash = 31 * hash + gridHashCode(val[i]);

    return hash;
}

#endif // GRIDCLIENTHASH_HPP_INCLUDED
