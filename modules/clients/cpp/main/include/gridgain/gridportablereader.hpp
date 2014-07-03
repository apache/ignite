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
#include <boost/optional.hpp>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>
#include <gridgain/gridclientdate.hpp>

class GridPortableRawReader;

/**
 * Reader for portable objects.
 */
class GRIDGAIN_API GridPortableReader {
public:
    /**
     * @param fieldName Field name.
     * @return Bool value.
     */
    virtual bool readBool(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<bool*, int32_t> readBoolArray(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readBoolArray(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, bool>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readBoolArray(char* fieldName, std::vector<bool>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Byte value.
     */
    virtual int8_t readByte(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int8_t*, int32_t> readByteArray(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readByteArray(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, int8_t>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readByteArray(char* fieldName, std::vector<int8_t>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Short value.
     */
    virtual int16_t readInt16(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int16_t*, int32_t> readInt16Array(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readInt16Array(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, int16_t>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readInt16Array(char* fieldName, std::vector<int16_t>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Int value.
     */
    virtual int32_t readInt32(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int32_t*, int32_t> readInt32Array(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readInt32Array(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, int32_t>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readInt32Array(char* fieldName, std::vector<int32_t>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Long value.
     */
    virtual int64_t readInt64(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int64_t*, int32_t> readInt64Array(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readInt64Array(char* fieldName, std::vector<int64_t>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readInt64Array(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, int64_t>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @return Float value.
     */
    virtual float readFloat(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<float*, int32_t> readFloatArray(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readFloatArray(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, float>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readFloatArray(char* fieldName, std::vector<float>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Double value.
     */
    virtual double readDouble(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<double*, int32_t> readDoubleArray(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readDoubleArray(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, double>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readDoubleArray(char* fieldName, std::vector<double>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Char value.
     */
    virtual uint16_t readChar(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<uint16_t*, int32_t> readCharArray(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readCharArray(char* fieldName, OutputIterator out) {
        return readArray<OutputIterator, uint16_t>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readCharArray(char* fieldName, std::vector<uint16_t>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return String value.
     */
    virtual boost::optional<std::string> readString(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readStringArray(char* fieldName, OutputIterator out) {
        return readObjectArray<OutputIterator, std::string>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readStringArray(char* fieldName, std::vector<std::string>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Wide string value.
     */
    virtual boost::optional<std::wstring> readWString(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readWStringArray(char* fieldName, OutputIterator out) {
        return readObjectArray<OutputIterator, std::wstring>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @return arr Output array.
     */
    virtual bool readWStringArray(char* fieldName, std::vector<std::wstring>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Uuid value.
     */
    virtual boost::optional<GridClientUuid> readUuid(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readUuidArray(char* fieldName, OutputIterator out) {
        return readObjectArray<OutputIterator, GridClientUuid>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readUuidArray(char* fieldName, std::vector<GridClientUuid>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Date value.
     */
    virtual boost::optional<GridClientDate> readDate(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readDateArray(char* fieldName, OutputIterator out) {
        return readObjectArray<OutputIterator, GridClientDate>(fieldName, out);
    }

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readDateArray(char* fieldName, std::vector<GridClientDate>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @return Variant value.
     */
    virtual GridClientVariant readVariant(char* fieldName) = 0;

    /**
     * @param fieldName Field name.
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readVariantArray(char* fieldName, std::vector<GridClientVariant>& arr) = 0;

    /**
     * @param fieldName Field name.
     * @param col Output array.
     * @return Whether non null collection was read.
     */
    virtual bool readVariantCollection(char* fieldName, std::vector<GridClientVariant>& col) = 0;

    /**
     * @param fieldName Field name.
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readVariantCollection(char* fieldName, OutputIterator out) {
        return readCollection(fieldName, out);   
    }

    /**
     * @param fieldName Field name.
     * @param map Output array.
     * @return Whether non null map was read.
     */
    virtual bool readVariantMap(char* fieldName, TGridClientVariantMap& map) = 0;

    /**
     * Gets raw reader.
     *
     * @return Raw reader.
     */
    virtual GridPortableRawReader& rawReader() = 0;

    /**
     * Destructor.
     */
    virtual ~GridPortableReader() {
    }

private:
    template<typename OutputIterator, typename T>
    bool readArray(char* fieldName, OutputIterator out) {
        if (!startReadArray<T>(fieldName))
            return false;

        int32_t size = readArraySize(false);

        if (size < 0)
            return false;

        for (int32_t i = 0; i < size; i++) {
            *out = readArrayElement<T>(false);

            out++;
        }

        return true;
    }

    template<typename OutputIterator, typename T>
    bool readObjectArray(char* fieldName, OutputIterator out) {
        if (!startReadArray<T>(fieldName))
            return false;

        int32_t size = readArraySize(false);

        if (size < 0)
            return false;

        for (int32_t i = 0; i < size; i++) {
            if (doReadByte(false) != 101)
                *out = std::move(readObject<T>(false));

            out++;
        }

        return true;
    }

    template<typename OutputIterator>
    bool readCollection(char* fieldName, OutputIterator out) {
        if (!startReadVariantCollection(fieldName))
            return false;

        int32_t size = readArraySize(false);

        if (size < 0)
            return false;

        doReadByte(false);

        for (int32_t i = 0; i < size; i++) {
            *out = std::move(doReadVariant(false));

            out++;
        }

        return true;
    }

    template<typename T>
    T readArrayElement(bool raw);

    template<typename T>
    bool startReadArray(char* fieldName);

    virtual int32_t readArraySize(bool) = 0;

    virtual bool doReadBool(bool) = 0;

    virtual int8_t doReadByte(bool) = 0;

    virtual int16_t doReadInt16(bool) = 0;

    virtual int32_t doReadInt32(bool) = 0;

    virtual int64_t doReadInt64(bool) = 0;

    virtual float doReadFloat(bool) = 0;

    virtual double doReadDouble(bool) = 0;

    virtual uint16_t doReadChar(bool) = 0;

    virtual GridClientVariant doReadVariant(bool) = 0;

    template<typename T>
    T readObject(bool raw);

    virtual std::string doReadString(bool) = 0;

    virtual std::wstring doReadWString(bool) = 0;

    virtual GridClientUuid doReadUuid(bool) = 0;

    virtual GridClientDate doReadDate(bool) = 0;

    virtual bool startReadBoolArray(char* fieldName) = 0;

    virtual bool startReadByteArray(char* fieldName) = 0;

    virtual bool startReadInt16Array(char* fieldName) = 0;

    virtual bool startReadCharArray(char* fieldName) = 0;

    virtual bool startReadInt32Array(char* fieldName) = 0;

    virtual bool startReadInt64Array(char* fieldName) = 0;

    virtual bool startReadFloatArray(char* fieldName) = 0;

    virtual bool startReadDoubleArray(char* fieldName) = 0;

    virtual bool startReadUuidArray(char* fieldName) = 0;

    virtual bool startReadDateArray(char* fieldName) = 0;

    virtual bool startReadStringArray(char* fieldName) = 0;

    virtual bool startReadVariantCollection(char* fieldName) = 0;
};

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableRawReader {
public:
    /**
     * @return Bool value.
     */
    virtual bool readBool() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<bool*, int32_t> readBoolArray() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readBoolArray(std::vector<bool>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readBoolArray(OutputIterator out) {
        return readArray<OutputIterator, bool>(out);
    }

    /**
     * @return Byte value.
     */
    virtual int8_t readByte() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int8_t*, int32_t> readByteArray() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readByteArray(std::vector<int8_t>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readByteArray(OutputIterator out) {
        return readArray<OutputIterator, int8_t>(out);
    }

    /**
     * @return Short value.
     */
    virtual int16_t readInt16() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int16_t*, int32_t> readInt16Array() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readInt16Array(std::vector<int16_t>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readInt16Array(OutputIterator out) {
        return readArray<OutputIterator, int16_t>(out);
    }

    /**
     * @return Int value.
     */
    virtual int32_t readInt32() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int32_t*, int32_t> readInt32Array() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readInt32Array(std::vector<int32_t>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readInt32Array(OutputIterator out) {
        return readArray<OutputIterator, int32_t>(out);
    }

    /**
     * @return Long value.
     */
    virtual int64_t readInt64() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<int64_t*, int32_t> readInt64Array() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readInt64Array(std::vector<int64_t>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readInt64Array(OutputIterator out) {
        return readArray<OutputIterator, int64_t>(out);
    }

    /**
     * @return Float value.
     */
    virtual float readFloat() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<float*, int32_t> readFloatArray() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readFloatArray(std::vector<float>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readFloatArray(OutputIterator out) {
        return readArray<OutputIterator, float>(out);
    }

    /**
     * @return Double value.
     */
    virtual double readDouble() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<double*, int32_t> readDoubleArray() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readDoubleArray(std::vector<double>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readDoubleArray(OutputIterator out) {
        return readArray<OutputIterator, double>(out);
    }

    /**
     * @return Char value.
     */
    virtual uint16_t readChar() = 0;

    /**
     * @return Pair containing array pointer and array size.
     */
    virtual std::pair<uint16_t*, int32_t> readCharArray() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readCharArray(std::vector<uint16_t>& arr) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readCharArray(OutputIterator out) {
        return readArray<OutputIterator, uint16_t>(out);
    }

    /**
     * @return String value.
     */
    virtual boost::optional<std::string> readString() = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readStringArray(OutputIterator out) {
        return readObjectArray<OutputIterator, std::string>(out);
    }

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readStringArray(std::vector<std::string>& arr) = 0;

    /**
     * @return String value.
     */
    virtual boost::optional<std::wstring> readWString() = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readWStringArray(OutputIterator out) {
        return readObjectArray<OutputIterator, std::wstring>(out);
    }

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readWStringArray(std::vector<std::wstring>& arr) = 0;

    /**
     * @return Uuid value.
     */
    virtual boost::optional<GridClientUuid> readUuid() = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readUuidArray(OutputIterator out) {
        return readObjectArray<OutputIterator, GridClientUuid>(out);
    }

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readUuidArray(std::vector<GridClientUuid>& arr) = 0;

    /**
     * @return Date value.
     */
    virtual boost::optional<GridClientDate> readDate() = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readDateArray(OutputIterator out) {
        return readObjectArray<OutputIterator, GridClientDate>(out);
    }

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readDateArray(std::vector<GridClientDate>& arr) = 0;

    /**
     * @return Variant value.
     */
    virtual GridClientVariant readVariant() = 0;

    /**
     * @param arr Output array.
     * @return Whether non null array was read.
     */
    virtual bool readVariantArray(TGridClientVariantSet& arr) = 0;

    /**
     * @param arr Output collection.
     * @return Whether non null collection was read.
     */
    virtual bool readVariantCollection(TGridClientVariantSet& col) = 0;

    /**
     * @param out Output iterator to the initial position in destination array.
     * @return Whether non null array was read.
     */
    template<class OutputIterator>
    bool readVariantCollection(OutputIterator out) {
        return readCollection(out);
    }

    /**
     * @param arr Output map.
     * @return Whether non null array was read.
     */
    virtual bool readVariantMap(TGridClientVariantMap& map) = 0;

    /**
     * Destructor.
     */
    virtual ~GridPortableRawReader() {
    }

private:
    template<typename OutputIterator, typename T>
    bool readArray(OutputIterator out) {
        int32_t size = readArraySize(true);

        if (size < 0)
            return false;

        for (int32_t i = 0; i < size; i++) {
            *out = readArrayElement<T>(true);

            out++;
        }

        return true;
    }

    template<typename OutputIterator, typename T>
    bool readObjectArray(OutputIterator out) {
        int32_t size = readArraySize(true);

        if (size < 0)
            return false;

        for (int32_t i = 0; i < size; i++) {
            if (doReadByte(true) != 101)
                *out = std::move(readObject<T>(true));

            out++;
        }

        return true;
    }

    template<typename OutputIterator>
    bool readCollection(OutputIterator out) {
        int32_t size = readArraySize(true);

        if (size < 0)
            return false;

        doReadByte(true);

        for (int32_t i = 0; i < size; i++) {
            *out = std::move(doReadVariant(true));

            out++;
        }

        return true;
    }

    template<typename T>
    T readArrayElement(bool raw);

    virtual int32_t readArraySize(bool) = 0;

    virtual bool doReadBool(bool) = 0;

    virtual int8_t doReadByte(bool) = 0;

    virtual int16_t doReadInt16(bool) = 0;

    virtual int32_t doReadInt32(bool) = 0;

    virtual int64_t doReadInt64(bool) = 0;

    virtual float doReadFloat(bool) = 0;

    virtual double doReadDouble(bool) = 0;

    virtual uint16_t doReadChar(bool) = 0;

    template<typename T>
    T readObject(bool raw);

    virtual std::string doReadString(bool) = 0;

    virtual std::wstring doReadWString(bool) = 0;

    virtual GridClientUuid doReadUuid(bool) = 0;

    virtual GridClientDate doReadDate(bool) = 0;

    virtual GridClientVariant doReadVariant(bool) = 0;
};

class GRIDGAIN_API GridPortableFactory {
public: 
    virtual void* newInstance(GridPortableReader& reader) = 0;

    virtual ~GridPortableFactory() {
    }
};

void GRIDGAIN_API registerPortableFactory(int32_t typeId, GridPortableFactory* factory);

#define REGISTER_TYPE(TYPE) \
    class GridPortableFactory_##TYPE : public GridPortableFactory {\
    public:\
        \
        GridPortableFactory_##TYPE() {\
            TYPE t;\
            registerPortableFactory(t.typeId(), this);\
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

#endif // GRIDPORTABLEREADER_HPP_INCLUDED
