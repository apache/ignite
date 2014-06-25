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
#include <gridgain/gridclientdate.hpp>

class GridPortableRawWriter;

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableWriter {
public:
    virtual void writeBool(char* fieldName, bool val) = 0;

    virtual void writeBoolArray(char* fieldName, const bool* val, int32_t size) = 0;

    virtual void writeBoolCollection(char* fieldName, const std::vector<bool>& val) = 0;

    virtual void writeByte(char* fieldName, int8_t val) = 0;

    virtual void writeByteArray(char* fieldName, const int8_t* val, int32_t size) = 0;

    virtual void writeByteCollection(char* fieldName, const std::vector<int8_t>& val) = 0;

    virtual void writeInt16(char* fieldName, int16_t val) = 0;

    virtual void writeInt16Array(char* fieldName, const int16_t* val, int32_t size) = 0;

    virtual void writeInt16Collection(char* fieldName, const std::vector<int16_t>& val) = 0;

    virtual void writeInt32(char* fieldName, int32_t val) = 0;

    virtual void writeInt32Array(char* fieldName, const int32_t* val, int32_t size) = 0;

    virtual void writeInt32Collection(char* fieldName, const std::vector<int32_t>& val) = 0;

    virtual void writeInt64(char* fieldName, int64_t val) = 0;

    virtual void writeInt64Array(char* fieldName, const int64_t* val, int32_t size) = 0;

    virtual void writeInt64Collection(char* fieldName, const std::vector<int64_t>& val) = 0;

    virtual void writeFloat(char* fieldName, float val) = 0;

    virtual void writeFloatArray(char* fieldName, const float* val, int32_t size) = 0;

    virtual void writeFloatCollection(char* fieldName, const std::vector<float>& val) = 0;

    virtual void writeDouble(char* fieldName, double val) = 0;

    virtual void writeDoubleArray(char* fieldName, const double* val, int32_t size) = 0;

    virtual void writeDoubleCollection(char* fieldName, const std::vector<double>& val) = 0;

    virtual void writeChar(char* fieldName, uint16_t val) = 0;

    virtual void writeCharCollection(char* fieldName, const std::vector<uint16_t> val) = 0;

    virtual void writeCharArray(char* fieldName, const uint16_t* val, int32_t size) = 0;

    virtual void writeString(char* fieldName, const std::string& val) = 0;

    virtual void writeStringCollection(char* fieldName, const std::vector<std::string>& val) = 0;

    virtual void writeWString(char* fieldName, const std::wstring& val) = 0;

    virtual void writeWStringCollection(char* fieldName, const std::vector<std::wstring>& val) = 0;

    virtual void writeUuid(char* fieldName, const boost::optional<GridClientUuid>& val) = 0;

    virtual void writeUuidCollection(char* fieldName, const std::vector<GridClientUuid>& val) = 0;

    virtual void writeDate(char* fieldName, const boost::optional<GridClientDate>& val) = 0;

    virtual void writeDateCollection(char* fieldName, const std::vector<boost::optional<GridClientDate>>& val) = 0;

    virtual void writeVariant(char* fieldName, const GridClientVariant& val) = 0;
     
    virtual void writeVariantCollection(char* fieldName, const TGridClientVariantSet& val) = 0;

    virtual void writeVariantMap(char* fieldName, const TGridClientVariantMap& map) = 0;

    virtual GridPortableRawWriter& rawWriter() = 0;
};

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableRawWriter {
public:
    virtual void writeBool(bool val) = 0;

    virtual void writeBoolArray(const bool* val, int32_t size) = 0;

    virtual void writeBoolCollection(const std::vector<bool>& val) = 0;

    virtual void writeByte(int8_t val) = 0;

    virtual void writeByteArray(const int8_t* val, int32_t size) = 0;

    virtual void writeByteCollection(const std::vector<int8_t>& val) = 0;

    virtual void writeInt16(int16_t val) = 0;

    virtual void writeInt16Array(const int16_t* val, int32_t size) = 0;

    virtual void writeInt16Collection(const std::vector<int16_t>& val) = 0;

    virtual void writeInt32(int32_t val) = 0;

    virtual void writeInt32Array(const int32_t* val, int32_t size) = 0;

    virtual void writeInt32Collection(const std::vector<int32_t>& val) = 0;

    virtual void writeInt64(int64_t val) = 0;

    virtual void writeInt64Array(const int64_t* val, int32_t size) = 0;

    virtual void writeInt64Collection(const std::vector<int64_t>& val) = 0;

    virtual void writeFloat(float val) = 0;

    virtual void writeFloatArray(const float* val, int32_t size) = 0;

    virtual void writeFloatCollection(const std::vector<float>& val) = 0;

    virtual void writeDouble(double val) = 0;

    virtual void writeDoubleArray(const double* val, int32_t size) = 0;

    virtual void writeDoubleCollection(const std::vector<double>& val) = 0;

    virtual void writeChar(uint16_t val) = 0;

    virtual void writeCharCollection(const std::vector<uint16_t> val) = 0;

    virtual void writeCharArray(const uint16_t* val, int32_t size) = 0;

    virtual void writeString(const std::string& val) = 0;

    virtual void writeStringCollection(const std::vector<std::string>& val) = 0;

    virtual void writeWString(const std::wstring& val) = 0;

    virtual void writeWStringCollection(const std::vector<std::wstring>& val) = 0;

    virtual void writeUuid(const boost::optional<GridClientUuid>& val) = 0;

    virtual void writeUuidCollection(const std::vector<GridClientUuid>& val) = 0;

    virtual void writeDate(const boost::optional<GridClientDate>& val) = 0;

    virtual void writeDateCollection(const std::vector<boost::optional<GridClientDate>>& val) = 0;

    virtual void writeVariant(const GridClientVariant& val) = 0;

    virtual void writeVariantCollection(const TGridClientVariantSet& val) = 0;

    virtual void writeVariantMap(const TGridClientVariantMap& map) = 0;
};

#endif // GRIDPORTABLEWRITER_HPP_INCLUDED
