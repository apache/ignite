/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/gridportable.hpp"

GridPortableObject::GridPortableObject(std::vector<int8_t> bytes) : bytes(bytes) {
}

GridPortableObject::GridPortableObject(const GridPortableObject& other) : bytes(other.bytes) {
}

GridPortableObject::GridPortableObject(const GridPortableObject&& other) : bytes(std::move(other.bytes)) {
}

int32_t GridPortableObject::typeId() {
    return 0;
}

int32_t GridPortableObject::fieldTypeId(const std::string& fieldName) {
    return 0;
}

std::string GridPortableObject::typeName() {
    std::string str;

    return str;
}

int32_t GridPortableObject::hashCode() {
    return 0;
}

std::vector<std::string> GridPortableObject::fields() {
    std::vector<std::string> fields;
    
    return fields;
}

GridClientVariant GridPortableObject::field(const std::string& fieldName) {
    GridClientVariant var;

    return var;
}

GridClientVariant GridPortableObject::deserialize() {
    GridClientVariant var;

    return var;
}

GridPortableObject GridPortableObject::copy(boost::unordered_map<std::string, GridClientVariant> fields) {
    GridPortableObject obj(*this);

    return obj;
}

GridPortableObjectBuilder::GridPortableObjectBuilder(int32_t typeId) {
}

void GridPortableObjectBuilder::set(std::string fieldName, const GridClientVariant& val) {
}

void GridPortableObjectBuilder::set(boost::unordered_map<std::string, GridClientVariant> fieldVals) {
}

GridPortableObject GridPortableObjectBuilder::build() {
    std::vector<int8_t> bytes;

    return GridPortableObject(bytes);
}
