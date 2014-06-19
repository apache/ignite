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
#include <vector>
#include <boost/unordered_map.hpp>
#include <boost/optional.hpp>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableWriter {
public:
    virtual void writeBool(char* fieldName, bool val) = 0;

    virtual void writeBoolArray(char* fieldName, bool* val, int32_t size) = 0;

    virtual void writeBoolCollection(char* fieldName, const std::vector<bool>& val) = 0;

    virtual void writeByte(char* fieldName, int8_t val) = 0;

    virtual void writeByteArray(char* fieldName, int8_t* val, int32_t size) = 0;

    virtual void writeByteCollection(char* fieldName, const std::vector<int8_t>& val) = 0;

    virtual void writeInt16(char* fieldName, int16_t val) = 0;

    virtual void writeInt16Array(char* fieldName, int16_t* val, int32_t size) = 0;

    virtual void writeInt16Collection(char* fieldName, const std::vector<int16_t>& val) = 0;

    virtual void writeInt32(char* fieldName, int32_t val) = 0;

    virtual void writeInt32Array(char* fieldName, int32_t* val, int32_t size) = 0;

    virtual void writeInt32Collection(char* fieldName, const std::vector<int32_t>& val) = 0;

    virtual void writeInt64(char* fieldName, int64_t val) = 0;

    virtual void writeInt64Array(char* fieldName, int64_t* val, int32_t size) = 0;

    virtual void writeInt64Collection(char* fieldName, const std::vector<int64_t>& val) = 0;

    virtual void writeFloat(char* fieldName, float val) = 0;

    virtual void writeFloatArray(char* fieldName, float* val, int32_t size) = 0;

    virtual void writeFloatCollection(char* fieldName, const std::vector<float>& val) = 0;

    virtual void writeDouble(char* fieldName, double val) = 0;

    virtual void writeDoubleArray(char* fieldName, double* val, int32_t size) = 0;

    virtual void writeDoubleCollection(char* fieldName, const std::vector<double>& val) = 0;

    virtual void writeString(char* fieldName, const std::string& val) = 0;

    virtual void writeStringCollection(char* fieldName, const std::vector<std::string>& val) = 0;

    virtual void writeWString(char* fieldName, const std::wstring& val) = 0;

    virtual void writeWStringCollection(char* fieldName, const std::vector<std::wstring>& val) = 0;

    virtual void writeVariant(char* fieldName, const GridClientVariant& val) = 0;

    virtual void writeVariantCollection(char* fieldName, const std::vector<GridClientVariant>& val) = 0;

    virtual void writeVariantMap(char* fieldName, const boost::unordered_map<GridClientVariant, GridClientVariant>& map) = 0;

    virtual void writeUuid(char* fieldName, const boost::optional<GridClientUuid>& val) = 0;
};

#endif // GRIDPORTABLEWRITER_HPP_INCLUDED
