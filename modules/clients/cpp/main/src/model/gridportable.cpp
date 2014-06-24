/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/gridportable.hpp"
#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

class GridPortableObject::Impl {
public:
    Impl(const boost::shared_ptr<std::vector<int8_t>>& dataPtr, int32_t start, GridPortableIdResolver* idRslvr) : 
        dataPtr(dataPtr), start(start), ctx(dataPtr, idRslvr), reader(ctx, start) {
    }

    Impl(const Impl& other) : dataPtr(other.dataPtr), start(other.start), ctx(dataPtr, other.ctx.idRslvr), reader(ctx, other.start) {
    }
    
    GridClientVariant field(const std::string& fieldName) {
        return reader.unmarshalFieldStr(fieldName);
    }

    GridPortable* deserialize() {
        return reader.deserializePortable();
    }

private:
    const boost::shared_ptr<std::vector<int8_t>> dataPtr;

    ReadContext ctx;

    GridPortableReaderImpl reader;

    const int32_t start;
};

GridPortableObject::GridPortableObject(const boost::shared_ptr<std::vector<int8_t>>& dataPtr, int32_t start, GridPortableIdResolver* idRslvr) {
    pImpl = new Impl(dataPtr, start, idRslvr);
}

GridPortableObject::GridPortableObject(const GridPortableObject& other) {
    pImpl = new Impl(*other.pImpl);
}

GridPortableObject::~GridPortableObject() {
    delete pImpl;
}

int32_t GridPortableObject::typeId() const {
    return 0;
}

int32_t GridPortableObject::hashCode() const {
    return 0;
}

GridClientVariant GridPortableObject::field(const std::string& fieldName) const {
    return pImpl->field(fieldName);
}

GridPortable* GridPortableObject::deserialize() const {
    return pImpl->deserialize();
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
    boost::shared_ptr<std::vector<int8_t>> dataPtr;
    
    return GridPortableObject(dataPtr, 0, nullptr);
}
