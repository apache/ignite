/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/gridportablewriter.hpp"

template<>
int32_t GridPortableWriter::startArray<bool>() {
    return startBoolArray();
}

template<>
int32_t GridPortableWriter::startArray<int8_t>() {
    return startByteArray();
}

template<>
int32_t GridPortableWriter::startArray<int16_t>() {
    return startInt16Array();
}

template<>
int32_t GridPortableWriter::startArray<uint16_t>() {
    return startCharArray();
}

template<>
int32_t GridPortableWriter::startArray<int32_t>() {
    return startInt32Array();
}

template<>
int32_t GridPortableWriter::startArray<int64_t>() {
    return startInt64Array();
}

template<>
int32_t GridPortableWriter::startArray<float>() {
    return startFloatArray();
}

template<>
int32_t GridPortableWriter::startArray<double>() {
    return startDoubleArray();
}

template<>
int32_t GridPortableWriter::startArray<GridClientUuid>() {
    return startUuidArray();
}

template<>
int32_t GridPortableWriter::startArray<GridClientDate>() {
    return startDateArray();
}

template<>
int32_t GridPortableWriter::startArray<std::string>() {
    return startStringArray();
}

template<>
int32_t GridPortableWriter::startArray<std::wstring>() {
    return startStringArray();
}

template<>
int32_t GridPortableWriter::startArray<GridClientVariant>() {
    return startVariantArray();
}

template<>
void GridPortableWriter::writeArrayElement(const bool& val) {
    doWriteBool(val);
}

template<>
void GridPortableWriter::writeArrayElement(const int8_t& val) {
    doWriteByte(val);
}

template<>
void GridPortableWriter::writeArrayElement(const int16_t& val) {
    doWriteInt16(val);
}

template<>
void GridPortableWriter::writeArrayElement(const int32_t& val) {
    doWriteInt32(val);
}

template<>
void GridPortableWriter::writeArrayElement(const int64_t& val) {
    doWriteInt64(val);
}

template<>
void GridPortableWriter::writeArrayElement(const float& val) {
    doWriteFloat(val);
}

template<>
void GridPortableWriter::writeArrayElement(const double& val) {
    doWriteDouble(val);
}

template<>
void GridPortableWriter::writeArrayElement(const uint16_t& val) {
    doWriteChar(val);
}

template<>
void GridPortableWriter::writeArrayElement(const GridClientUuid& val) {
    doWriteUuid(val);
}

template<>
void GridPortableWriter::writeArrayElement(const GridClientDate& val) {
    doWriteDate(val);
}

template<>
void GridPortableWriter::writeArrayElement(const std::string& val) {
    doWriteString(val);
}

template<>
void GridPortableWriter::GridPortableWriter::writeArrayElement(const std::wstring& val) {
    doWriteWString(val);
}

template<>
void GridPortableWriter::writeArrayElement(const GridClientVariant& val) {
    doWriteVariant(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const bool& val) {
    doWriteBool(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const int8_t& val) {
    doWriteByte(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const int16_t& val) {
    doWriteInt16(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const int32_t& val) {
    doWriteInt32(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const int64_t& val) {
    doWriteInt64(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const float& val) {
    doWriteFloat(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const double& val) {
    doWriteDouble(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const uint16_t& val) {
    doWriteChar(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const GridClientUuid& val) {
    doWriteUuid(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const GridClientDate& val) {
    doWriteDate(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const std::string& val) {
    doWriteString(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const std::wstring& val) {
    doWriteWString(val);
}

template<>
void GridPortableRawWriter::writeArrayElementRaw(const GridClientVariant& val) {
    doWriteVariant(val);
}