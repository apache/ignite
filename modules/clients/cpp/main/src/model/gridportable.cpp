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

    Impl(const Impl& other) : ctxPtr(other.ctxPtr), reader(other.ctxPtr, other.reader.start) {
    }

    GridClientVariant field(const std::string& fieldName) {
        return reader.unmarshalFieldStr(fieldName);
    }

    bool userType() {
        return reader.in.readInt32(reader.start + 1) != 0;
    }

    int32_t typeId() {
        return reader.in.readInt32(reader.start + 2);
    }

    int32_t hashCode() {
        return reader.in.readInt32(reader.start + 6);
    }

    bool compare(Impl& other) {
        if (typeId() != other.typeId())
            return false;

        int32_t len = reader.in.readInt32(reader.start + 10);
        int32_t otherLen = other.reader.in.readInt32(other.reader.start + 10);

        if (len != otherLen)
            return false;

        return memcmp(data()->data() + start(), other.data()->data() + other.start(), len) == 0;
    }

    GridPortable* deserialize() {
        return reader.deserializePortable();
    }

    int32_t start() {
        return reader.start;
    }

    std::vector<int8_t>* data() {
        boost::shared_ptr<std::vector<int8_t>>& ptr = (ctxPtr.get())->dataPtr;

        return ptr.get();
    }

private:
    GridPortableReaderImpl reader;

    const boost::shared_ptr<PortableReadContext> ctxPtr;
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

bool GridPortableObject::userType() const {
    return pImpl->userType();
}

int32_t GridPortableObject::typeId() const {
    return pImpl->typeId();
}

int32_t GridPortableObject::hashCode() const {
    return pImpl->hashCode();
}

GridClientVariant GridPortableObject::field(const std::string& fieldName) const {
    return pImpl->field(fieldName);
}

GridPortable* GridPortableObject::deserialize() const {
    return pImpl->deserialize();
}

bool GridPortableObject::operator==(const GridPortableObject& other) const {
    return pImpl->compare(*other.pImpl);
}

std::vector<int8_t>* GridPortableObject::data() {
    return pImpl->data();
}

int32_t GridPortableObject::start() {
    return pImpl->start();
}
