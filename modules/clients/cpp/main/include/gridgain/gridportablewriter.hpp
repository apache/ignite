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
 * Collection type. Can be set to during collection writing using method 
 * {@link GridPortableWriter#writeVariantCollection} to specify type of collection 
 * instantiated on Java/.NET side during deserialization.
 */
enum GridCollectionType {
    USER_COLLECTION = 0,
    ARRAY_LIST,
    LINKED_LIST,
    HASH_SET,
    LINKED_HASH_SET,
    TREE_SET,
    CONCURRENT_SKIP_LIST_SET
};

/**
 * Map type. Can be set to during map writing using method 
 * {@link GridPortableWriter#writeVariantCollection} to specify type of collection 
 * instantiated on Java/.NET side during deserialization.
 */
enum GridMapType {
    USER_MAP = 0,
    HASH_MAP,
    LINKED_HASH_MAP,
    TREE_MAP,
    CONCURRENT_HASH_MAP
};

/**
 * Writer for portable objects.
 */
class GRIDGAIN_API GridPortableWriter {
public:
    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeBool(char* fieldName, bool val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeBoolArray(char* fieldName, const bool* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeBoolArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, bool>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeByte(char* fieldName, int8_t val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeByteArray(char* fieldName, const int8_t* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeByteArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, int8_t>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeInt16(char* fieldName, int16_t val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeInt16Array(char* fieldName, const int16_t* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeInt16Array(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, int16_t>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeInt32(char* fieldName, int32_t val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeInt32Array(char* fieldName, const int32_t* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeInt32Array(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, int32_t>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeInt64(char* fieldName, int64_t val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeInt64Array(char* fieldName, const int64_t* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeInt64Array(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, int64_t>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeFloat(char* fieldName, float val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeFloatArray(char* fieldName, const float* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeFloatArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, float>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeDouble(char* fieldName, double val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeDoubleArray(char* fieldName, const double* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeDoubleArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, double>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeChar(char* fieldName, uint16_t val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeCharArray(char* fieldName, const uint16_t* val, int32_t size) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeCharArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, uint16_t>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeString(char* fieldName, const std::string& val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeString(char* fieldName, const boost::optional<std::string>& val) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeStringArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, std::string>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeWString(char* fieldName, const std::wstring& val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeWString(char* fieldName, const boost::optional<std::wstring>& val) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeWStringArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, std::wstring>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeUuid(char* fieldName, const boost::optional<GridClientUuid>& val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeUuid(char* fieldName, const GridClientUuid& val) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeUuidArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, GridClientUuid>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeDate(char* fieldName, const boost::optional<GridClientDate>& val) = 0;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeDate(char* fieldName, const GridClientDate& val) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeDateArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, GridClientDate>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     */
    virtual void writeVariant(char* fieldName, const GridClientVariant& col) = 0;

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeVariantArray(char* fieldName, InputIterator first, InputIterator last) {
        writeArray<InputIterator, GridClientVariant>(fieldName, first, last);            
    }

    /**
     * @param fieldName Field name.
     * @param col Value to write.
     */
    void writeVariantCollection(char* fieldName, const TGridClientVariantSet& col) {
        writeCollection(fieldName, ARRAY_LIST, col.begin(), col.end());
    }

    /**
     * @param fieldName Field name.
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeVariantCollection(char* fieldName, GridCollectionType type, InputIterator first, InputIterator last) {
        writeCollection(fieldName, type, first, last);
    }

    /**
     * @param fieldName Field name.
     * @param map Value to write.
     */
    virtual void writeVariantMap(char* fieldName, const TGridClientVariantMap& map) = 0;

    /**
     * Gets raw writer.
     *
     * @return Raw writer.
     */
    virtual GridPortableRawWriter& rawWriter() = 0;

    /**
     * Destructor.
     */
    virtual ~GridPortableWriter() {
    }

protected:
    template<class InputIterator, typename T>
    void writeArray(char* fieldName, InputIterator first, InputIterator last) {
        writeFieldName(fieldName);

        int32_t pos = startArray<T>();

        int32_t cnt = 0;

        while (first != last) {
            const T& val = *first;
            
            writeArrayElement<T>(val);

            first++;
            
            cnt++;
        }

        endArray(pos, cnt);
    }

    template<class InputIterator>
    void writeCollection(char* fieldName, GridCollectionType type, InputIterator first, InputIterator last) {
        writeFieldName(fieldName);

        int32_t pos = startVariantCollection();

        doWriteByte(static_cast<int8_t>(type));

        int32_t cnt = 0;

        while (first != last) {
            const GridClientVariant& val = *first;
            
            doWriteVariant(val);

            first++;
            
            cnt++;
        }

        endArray(pos, cnt);
    }

    template<typename T>
    int32_t startArray();

    template<class T>
    void writeArrayElement(const T& val);

    virtual void writeFieldName(char* fieldName) = 0;

    virtual void endArray(int32_t, int32_t) = 0;

    virtual void doWriteBool(bool val) = 0;

    virtual void doWriteByte(int8_t val) = 0;

    virtual void doWriteInt16(int16_t val) = 0;

    virtual void doWriteChar(uint16_t val) = 0;

    virtual void doWriteInt32(int32_t val) = 0;

    virtual void doWriteInt64(int64_t val) = 0;

    virtual void doWriteFloat(float val) = 0;

    virtual void doWriteDouble(double val) = 0;

    virtual void doWriteString(const std::string&) = 0;

    virtual void doWriteWString(const std::wstring&) = 0;

    virtual void doWriteUuid(const GridClientUuid&) = 0;

    virtual void doWriteDate(const GridClientDate&) = 0;

    virtual void doWriteVariant(const GridClientVariant&) = 0;

    virtual int32_t startBoolArray() = 0;

    virtual int32_t startByteArray() = 0;

    virtual int32_t startInt16Array() = 0;

    virtual int32_t startCharArray() = 0;

    virtual int32_t startInt32Array() = 0;

    virtual int32_t startInt64Array() = 0;

    virtual int32_t startFloatArray() = 0;

    virtual int32_t startDoubleArray() = 0;

    virtual int32_t startStringArray() = 0;

    virtual int32_t startUuidArray() = 0;

    virtual int32_t startDateArray() = 0;

    virtual int32_t startVariantArray() = 0;

    virtual int32_t startVariantCollection() = 0;
};

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableRawWriter {
public:
    /**
     * @param val Value to write.
     */
    virtual void writeBool(bool val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeBoolArray(const bool* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeBoolArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, bool>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeByte(int8_t val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeByteArray(const int8_t* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeByteArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, int8_t>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeInt16(int16_t val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeInt16Array(const int16_t* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeInt16Array(InputIterator first, InputIterator last) {
        writeArray<InputIterator, int16_t>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeInt32(int32_t val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeInt32Array(const int32_t* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeInt32Array(InputIterator first, InputIterator last) {
        writeArray<InputIterator, int32_t>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeInt64(int64_t val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeInt64Array(const int64_t* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeInt64Array(InputIterator first, InputIterator last) {
        writeArray<InputIterator, int64_t>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeFloat(float val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeFloatArray(const float* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeFloatArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, float>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeDouble(double val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeDoubleArray(const double* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeDoubleArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, double>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeChar(uint16_t val) = 0;

    /**
     * @param val Pointer to an array.
     * @param size Array size.
     */
    virtual void writeCharArray(const uint16_t* val, int32_t size) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeCharArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, uint16_t>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeString(const std::string& val) = 0;

    /**
     * @param val Value to write.
     */
    virtual void writeString(const boost::optional<std::string>& val) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeStringArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, std::string>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeWString(const std::wstring& val) = 0;

    /**
     * @param val Value to write.
     */
    virtual void writeWString(const boost::optional<std::wstring>& val) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeWStringArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, std::wstring>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeUuid(const boost::optional<GridClientUuid>& val) = 0;

    /**
     * @param val Value to write.
     */
    virtual void writeUuid(const GridClientUuid& val) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeUuidArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, GridClientUuid>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeDate(const boost::optional<GridClientDate>& val) = 0;

    /**
     * @param val Value to write.
     */
    virtual void writeDate(const GridClientDate& val) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeDateArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, GridClientDate>(first, last);            
    }

    /**
     * @param val Value to write.
     */
    virtual void writeVariant(const GridClientVariant& val) = 0;

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeVariantArray(InputIterator first, InputIterator last) {
        writeArray<InputIterator, GridClientVariant>(first, last);            
    }
     
    /**
     * @param col Value to write.
     */
    void writeVariantCollection(const TGridClientVariantSet& col) {
        writeCollection(ARRAY_LIST, col.begin(), col.end());
    }

    /**
     * @param first Input iterator to the initial position in a array to be written.
     * @param last Input iterator to the final position in a array to be written.
     */
    template<class InputIterator>
    void writeVariantCollection(GridCollectionType type, InputIterator first, InputIterator last) {
        writeCollection(type, first, last);
    }

    /**
     * @param map Value to write.
     */
    virtual void writeVariantMap(const TGridClientVariantMap& map) = 0;

    /**
     * Destructor.
     */
    virtual ~GridPortableRawWriter() {
    }

private:
    template<class InputIterator, typename T>
    void writeArray(InputIterator first, InputIterator last) {
        int32_t pos = startArrayRaw();

        int32_t cnt = 0;

        while (first != last) {
            const T& val = *first;
            
            writeArrayElementRaw<T>(val);

            first++;
            
            cnt++;
        }

        endArrayRaw(pos, cnt);
    }

    template<class InputIterator>
    void writeCollection(GridCollectionType type, InputIterator first, InputIterator last) {
        int32_t pos = startArrayRaw();

        doWriteByte(static_cast<int8_t>(type));

        int32_t cnt = 0;

        while (first != last) {
            const GridClientVariant& val = *first;
            
            doWriteVariant(val);

            first++;
            
            cnt++;
        }

        endArrayRaw(pos, cnt);
    }

    virtual int32_t startArrayRaw() = 0;

    virtual void endArrayRaw(int32_t, int32_t) = 0;

    template<class T>
    void writeArrayElementRaw(const T& val);

    virtual void doWriteBool(bool val) = 0;

    virtual void doWriteByte(int8_t val) = 0;

    virtual void doWriteInt16(int16_t val) = 0;

    virtual void doWriteChar(uint16_t val) = 0;

    virtual void doWriteInt32(int32_t val) = 0;

    virtual void doWriteInt64(int64_t val) = 0;

    virtual void doWriteFloat(float val) = 0;

    virtual void doWriteDouble(double val) = 0;

    virtual void doWriteString(const std::string&) = 0;

    virtual void doWriteWString(const std::wstring&) = 0;

    virtual void doWriteUuid(const GridClientUuid&) = 0;

    virtual void doWriteDate(const GridClientDate&) = 0;

    virtual void doWriteVariant(const GridClientVariant&) = 0;
};

#endif // GRIDPORTABLEWRITER_HPP_INCLUDED
