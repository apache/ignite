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
    Impl(const boost::shared_ptr<PortableReadContext>& ctxPtr, int32_t start) : 
        ctxPtr(ctxPtr), reader(ctxPtr, start) {
    }

    Impl(const Impl& other) : ctxPtr(other.ctxPtr), reader(ctxPtr, other.reader.start) {
    }
    
    GridClientVariant field(const std::string& fieldName) {
        return reader.unmarshalFieldStr(fieldName);
    }

    GridPortable* deserialize() {
        return reader.deserializePortable();
    }

private:
    const boost::shared_ptr<PortableReadContext> ctxPtr;

    GridPortableReaderImpl reader;
};

GridPortableObject::GridPortableObject(boost::shared_ptr<PortableReadContext>& ctxPtr, int32_t start) {
    pImpl = new Impl(ctxPtr, start);
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
    boost::shared_ptr<PortableReadContext> ctxPtr;
    
    return GridPortableObject(ctxPtr, 0);
}
