/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDPORTABLEREADER_HPP_INCLUDED
#define GRIDPORTABLEREADER_HPP_INCLUDED

#include <string>
#include <vector>
#include <unordered_map>
#include <boost/optional.hpp>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>
#include <gridgain/gridclientuuid.hpp>

/**
 * C++ client API.
 */
class GRIDGAIN_API GridPortableReader {
public:
    virtual bool readBool(char* fieldName) = 0;

    virtual int32_t readInt32(char* fieldName) = 0;

    virtual std::vector<int8_t> readBytes(char* fieldName) = 0;

    virtual std::string readString(char* fieldName) = 0;

    virtual GridClientVariant readVariant(char* fieldName) = 0;

    virtual boost::optional<GridClientUuid> readUuid(char* fieldName) = 0;

    virtual std::vector<GridClientVariant> readCollection(char* fieldName) = 0;

    virtual std::unordered_map<GridClientVariant, GridClientVariant> readMap(char* fieldName) = 0;
};

class GridPortableFactory {
public: 
    virtual void* newInstance(GridPortableReader& reader) = 0; 
};

void registerPortableFactory(int32_t typeId, GridPortableFactory* factory);

#define REGISTER_TYPE(TYPE_ID, TYPE) class GridPortableFactory_##TYPE : public GridPortableFactory { public: GridPortableFactory_##TYPE() {registerPortableFactory(TYPE_ID, this);} void* newInstance(GridPortableReader& reader) { GridPortable* p = new TYPE; p->readPortable(reader); return p;}; }; GridPortableFactory_##TYPE factory_##TYPE;

#define REGISTER_TYPE_SERIALIZER(TYPE_ID, TYPE, SERIALIZER) class GridPortableFactory_##TYPE : public GridPortableFactory { public: GridPortableFactory_##TYPE() {registerPortableFactory(TYPE_ID, this);}  public: void* newInstance(GridPortableReader& reader) { return new GridExternalPortable<TYPE>(ser.readPortable(reader), ser); } private: SERIALIZER ser; }; GridPortableFactory_##TYPE factory_##TYPE;

#endif // GRIDPORTABLEREADER_HPP_INCLUDED
