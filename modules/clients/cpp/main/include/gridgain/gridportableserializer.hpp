/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDPORTABLESERIALIZER_HPP_INCLUDED
#define GRIDPORTABLESERIALIZER_HPP_INCLUDED

#include <string>
#include <vector>
#include <boost/unordered_map.hpp>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>

/**
 * C++ client API.
 */
template<typename T>
class GRIDGAIN_API GridPortableSerializer {
public:
    virtual void writePortable(T* obj, GridPortableWriter& writer) = 0;

    virtual T* readPortable(GridPortableReader& reader) = 0;

    virtual int32_t typeId(T* obj) = 0;

    virtual int32_t hashCode(T* obj) = 0;

    virtual bool compare(T* obj1, T* obj2) = 0;
};

template<typename T>
class GRIDGAIN_API GridExternalPortable : public GridPortable {
public:
    GridExternalPortable(T* obj, GridPortableSerializer<T>& ser) : object(obj), serializer(ser) {
    }

    void writePortable(GridPortableWriter &writer) const override {
        serializer.writePortable(object, writer);
    }

    void readPortable(GridPortableReader &reader) override {
        object = serializer.readPortable(reader);
    }

    int32_t typeId() const override {
        return serializer.typeId(object);
    }

    int hashCode() const override {
        return serializer.hashCode(object);
    }

    bool operator==(const GridPortable& other) const override {
        const GridExternalPortable* externalPortable = static_cast<const GridExternalPortable*>(&other);

        return serializer.compare(object, externalPortable->object);
    }

    T* getObject() {
        return object;
    }

    T *operator->() const {
        return object;
    }

private:
    T* object;

    GridPortableSerializer<T>& serializer;
};

#endif // GRIDPORTABLESERIALIZER_HPP_INCLUDED
