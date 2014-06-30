/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/gridportablereader.hpp"

template<>
int8_t GridPortableReader::readArrayElement(bool raw) {
    return doReadByte(raw);
}

template<>
bool GridPortableReader::startReadArray<bool>(char* fieldName) {
    return startReadBoolArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<int8_t>(char* fieldName) {
    return startReadByteArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<int16_t>(char* fieldName) {
    return startReadInt16Array(fieldName);
}

template<>
bool GridPortableReader::startReadArray<uint16_t>(char* fieldName) {
    return startReadCharArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<int32_t>(char* fieldName) {
    return startReadInt32Array(fieldName);
}

template<>
bool GridPortableReader::startReadArray<int64_t>(char* fieldName) {
    return startReadInt64Array(fieldName);
}

template<>
bool GridPortableReader::startReadArray<float>(char* fieldName) {
    return startReadFloatArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<double>(char* fieldName) {
    return startReadDoubleArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<GridClientUuid>(char* fieldName) {
    return startReadUuidArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<GridClientData>(char* fieldName) {
    return startReadDateArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<std::string>(char* fieldName) {
    return startReadStringArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<std::wstring>(char* fieldName) {
    return startReadStringArray(fieldName);
}

template<>
bool GridPortableReader::startReadArray<GridClientVariant>(char* fieldName) {
    return startReadVariantCollection(fieldName);
}

template<>
std::string GridPortableReader::readString(int32_t len, bool raw) {
    return doReadString(len, raw);
}

template<>
std::wstring GridPortableReader::readString(int32_t len, bool raw) {
    return doReadWString(len, raw);
}

template<>
GridClientUuid GridPortableReader::readObject(bool raw) {
    return doReadUuidObject(raw);
}

template<>
GridClientDate GridPortableReader::readObject(bool raw) {
    return doReadDateObject(raw);
}

template<>
int8_t GridPortableRawReader::readArrayElement(bool raw) {
    return doReadByte(raw);
}

template<>
std::string GridPortableRawReader::readString(int32_t len, bool raw) {
    return doReadString(len, raw);
}

template<>
std::wstring GridPortableRawReader::readString(int32_t len, bool raw) {
    return doReadWString(len, raw);
}

template<>
GridClientUuid GridPortableRawReader::readObject(bool raw) {
    return doReadUuidObject(raw);
}

template<>
GridClientDate GridPortableRawReader::readObject(bool raw) {
    return doReadDateObject(raw);
}