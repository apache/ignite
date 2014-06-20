/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/gridportable.hpp"
#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

GridPortableObject::GridPortableObject(std::vector<int8_t>&& bytes, GridPortableIdResolver* idRslvr) : bytes(bytes), idRslvr(idRslvr) {
}

GridPortableObject::GridPortableObject(const GridPortableObject& other) : bytes(other.bytes), idRslvr(other.idRslvr)  {
}

GridPortableObject::GridPortableObject(const GridPortableObject&& other) : bytes(std::move(other.bytes)),
    idRslvr(other.idRslvr) {
}

int32_t GridPortableObject::typeId() const {
    return 0;
}

int32_t GridPortableObject::hashCode() const {
    return 0;
}

GridClientVariant GridPortableObject::field(const std::string& fieldName) const {
    ReadContext ctx(bytes, 10, idRslvr);

    GridPortableReaderImpl reader(ctx);

    return std::move(reader.unmarshalFieldStr(fieldName));
}

GridPortable* GridPortableObject::deserialize() const {
    ReadContext ctx(bytes, 10, idRslvr);

    GridPortableReaderImpl reader(ctx);

    return reader.deserializePortable();
}

GridPortableObject GridPortableObject::copy(boost::unordered_map<std::string, GridClientVariant> fields) const {
    GridPortableObject obj(*this);

    return obj;
}

bool GridPortableObject::operator==(const GridPortableObject& other) const {
    return false;
}

GridPortableObjectBuilder::GridPortableObjectBuilder(int32_t typeId) {
}

void GridPortableObjectBuilder::set(std::string fieldName, const GridClientVariant& val) {
}

void GridPortableObjectBuilder::set(boost::unordered_map<std::string, GridClientVariant> fieldVals) {
}

GridPortableObject GridPortableObjectBuilder::build() {
    std::vector<int8_t> bytes;

    return GridPortableObject(std::move(bytes), nullptr);
}
