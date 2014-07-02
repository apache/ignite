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
bool GridPortableReader::readArrayElement(bool raw) {
    return doReadBool(raw);
}

template<>
int16_t GridPortableReader::readArrayElement(bool raw) {
    return doReadInt16(raw);
}

template<>
uint16_t GridPortableReader::readArrayElement(bool raw) {
    return doReadChar(raw);
}

template<>
int32_t GridPortableReader::readArrayElement(bool raw) {
    return doReadInt32(raw);
}

template<>
int64_t GridPortableReader::readArrayElement(bool raw) {
    return doReadInt64(raw);
}

template<>
float GridPortableReader::readArrayElement(bool raw) {
    return doReadFloat(raw);
}

template<>
double GridPortableReader::readArrayElement(bool raw) {
    return doReadDouble(raw);
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
bool GridPortableReader::startReadArray<GridClientDate>(char* fieldName) {
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
GridClientUuid GridPortableReader::readObject(bool raw) {
    return doReadUuid(raw);
}

template<>
GridClientDate GridPortableReader::readObject(bool raw) {
    return doReadDate(raw);
}

template<>
std::string GridPortableReader::readObject(bool raw) {
    return doReadString(raw);
}

template<>
std::wstring GridPortableReader::readObject(bool raw) {
    return doReadWString(raw);
}

template<>
int8_t GridPortableRawReader::readArrayElement(bool raw) {
    return doReadByte(raw);
}

template<>
bool GridPortableRawReader::readArrayElement(bool raw) {
    return doReadBool(raw);
}

template<>
int16_t GridPortableRawReader::readArrayElement(bool raw) {
    return doReadInt16(raw);
}

template<>
uint16_t GridPortableRawReader::readArrayElement(bool raw) {
    return doReadChar(raw);
}

template<>
int32_t GridPortableRawReader::readArrayElement(bool raw) {
    return doReadInt32(raw);
}

template<>
int64_t GridPortableRawReader::readArrayElement(bool raw) {
    return doReadInt64(raw);
}

template<>
float GridPortableRawReader::readArrayElement(bool raw) {
    return doReadFloat(raw);
}

template<>
double GridPortableRawReader::readArrayElement(bool raw) {
    return doReadDouble(raw);
}

template<>
GridClientUuid GridPortableRawReader::readObject(bool raw) {
    return doReadUuid(raw);
}

template<>
GridClientDate GridPortableRawReader::readObject(bool raw) {
    return doReadDate(raw);
}

template<>
std::string GridPortableRawReader::readObject(bool raw) {
    return doReadString(raw);
}

template<>
std::wstring GridPortableRawReader::readObject(bool raw) {
    return doReadWString(raw);
}
