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
#include <boost/unordered_map.hpp>
#include <boost/optional.hpp>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>
#include <gridgain/gridclientdate.hpp>

class GridPortableRawReader;

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableReader {
public:
    virtual bool readBool(char* fieldName) = 0;

    virtual std::pair<bool*, int32_t> readBoolArray(char* fieldName) = 0;

    virtual boost::optional<std::vector<bool>> readBoolCollection(char* fieldName) = 0;

    virtual int8_t readByte(char* fieldName) = 0;

    virtual std::pair<int8_t*, int32_t> readByteArray(char* fieldName) = 0;

    virtual boost::optional<std::vector<int8_t>> readByteCollection(char* fieldName) = 0;

    virtual int16_t readInt16(char* fieldName) = 0;

    virtual std::pair<int16_t*, int32_t> readInt16Array(char* fieldName) = 0;

    virtual boost::optional<std::vector<int16_t>> readInt16Collection(char* fieldName) = 0;

    virtual int32_t readInt32(char* fieldName) = 0;

    virtual std::pair<int32_t*, int32_t> readInt32Array(char* fieldName) = 0;

    virtual boost::optional<std::vector<int32_t>> readInt32Collection(char* fieldName) = 0;

    virtual int64_t readInt64(char* fieldName) = 0;

    virtual std::pair<int64_t*, int32_t> readInt64Array(char* fieldName) = 0;

    virtual boost::optional<std::vector<int64_t>> readInt64Collection(char* fieldName) = 0;

    virtual float readFloat(char* fieldName) = 0;

    virtual std::pair<float*, int32_t> readFloatArray(char* fieldName) = 0;

    virtual boost::optional<std::vector<float>> readFloatCollection(char* fieldName) = 0;

    virtual double readDouble(char* fieldName) = 0;

    virtual std::pair<double*, int32_t> readDoubleArray(char* fieldName) = 0;

    virtual uint16_t readChar(char* fieldName) = 0;

    virtual std::pair<uint16_t*, int32_t> readCharArray(char* fieldName) = 0;

    virtual boost::optional<std::vector<uint16_t>> readCharCollection(char* fieldName) = 0;

    virtual boost::optional<std::vector<double>> readDoubleCollection(char* fieldName) = 0;

    virtual boost::optional<std::string> readString(char* fieldName) = 0;

    virtual boost::optional<std::vector<std::string>> readStringCollection(char* fieldName) = 0;

    virtual boost::optional<std::wstring> readWString(char* fieldName) = 0;

    virtual boost::optional<std::vector<std::wstring>> readWStringCollection(char* fieldName) = 0;

    virtual boost::optional<GridClientUuid> readUuid(char* fieldName) = 0;

    virtual boost::optional<std::vector<GridClientUuid>> readUuidCollection(char* fieldName) = 0;

    virtual boost::optional<GridClientDate> readDate(char* fieldName) = 0;

    virtual boost::optional<std::vector<boost::optional<GridClientDate>>> readDateCollection(char* fieldName) = 0;

    virtual GridClientVariant readVariant(char* fieldName) = 0;

    virtual boost::optional<TGridClientVariantSet> readVariantCollection(char* fieldName) = 0;

    virtual boost::optional<TGridClientVariantMap> readVariantMap(char* fieldName) = 0;

    virtual GridPortableRawReader& rawReader() = 0;

    virtual ~GridPortableReader() {
    }
};

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableRawReader {
public:
    virtual bool readBool() = 0;

    virtual std::pair<bool*, int32_t> readBoolArray() = 0;

    virtual boost::optional<std::vector<bool>> readBoolCollection() = 0;

    virtual int8_t readByte() = 0;

    virtual std::pair<int8_t*, int32_t> readByteArray() = 0;

    virtual boost::optional<std::vector<int8_t>> readByteCollection() = 0;

    virtual int16_t readInt16() = 0;

    virtual std::pair<int16_t*, int32_t> readInt16Array() = 0;

    virtual boost::optional<std::vector<int16_t>> readInt16Collection() = 0;

    virtual int32_t readInt32() = 0;

    virtual std::pair<int32_t*, int32_t> readInt32Array() = 0;

    virtual boost::optional<std::vector<int32_t>> readInt32Collection() = 0;

    virtual int64_t readInt64() = 0;

    virtual std::pair<int64_t*, int32_t> readInt64Array() = 0;

    virtual boost::optional<std::vector<int64_t>> readInt64Collection() = 0;

    virtual float readFloat() = 0;

    virtual std::pair<float*, int32_t> readFloatArray() = 0;

    virtual boost::optional<std::vector<float>> readFloatCollection() = 0;

    virtual double readDouble() = 0;

    virtual std::pair<double*, int32_t> readDoubleArray() = 0;

    virtual boost::optional<std::vector<double>> readDoubleCollection() = 0;

    virtual uint16_t readChar() = 0;

    virtual std::pair<uint16_t*, int32_t> readCharArray() = 0;

    virtual boost::optional<std::vector<uint16_t>> readCharCollection() = 0;

    virtual boost::optional<std::string> readString() = 0;

    virtual boost::optional<std::vector<std::string>> readStringCollection() = 0;

    virtual boost::optional<std::wstring> readWString() = 0;

    virtual boost::optional<std::vector<std::wstring>> readWStringCollection() = 0;

    virtual boost::optional<GridClientUuid> readUuid() = 0;

    virtual boost::optional<std::vector<GridClientUuid>> readUuidCollection() = 0;

    virtual boost::optional<GridClientDate> readDate() = 0;

    virtual boost::optional<std::vector<boost::optional<GridClientDate>>> readDateCollection() = 0;

    virtual GridClientVariant readVariant() = 0;

    virtual boost::optional<TGridClientVariantSet> readVariantCollection() = 0;

    virtual boost::optional<TGridClientVariantMap> readVariantMap() = 0;

    virtual ~GridPortableRawReader() {
    }
};

class GridPortableFactory {
public: 
    virtual void* newInstance(GridPortableReader& reader) = 0;

    virtual ~GridPortableFactory() {
    }
};

void registerPortableFactory(int32_t typeId, GridPortableFactory* factory);

#define REGISTER_TYPE(TYPE_ID, TYPE) \
    class GridPortableFactory_##TYPE : public GridPortableFactory {\
    public:\
        \
        GridPortableFactory_##TYPE() {\
            registerPortableFactory(TYPE_ID, this);\
        }\
        \
        virtual ~GridPortableFactory_##TYPE() {\
        }\
        \
        void* newInstance(GridPortableReader& reader) {\
            GridPortable* p = new TYPE;\
            \
            return p;\
        }\
    };\
    \
    GridPortableFactory_##TYPE factory_##TYPE;

#define REGISTER_TYPE_SERIALIZER(TYPE_ID, TYPE, SERIALIZER) \
    class GridPortableFactory_##TYPE : public GridPortableFactory {\
    public:\
        GridPortableFactory_##TYPE() {\
            registerPortableFactory(TYPE_ID, this);\
        }\
        \
        void* newInstance(GridPortableReader& reader) {\
            return new GridExternalPortable<TYPE>(ser.readPortable(reader), ser);\
        }\
        \
        private: SERIALIZER ser;\
    };\
    \
    GridPortableFactory_##TYPE factory_##TYPE;

#endif // GRIDPORTABLEREADER_HPP_INCLUDED
