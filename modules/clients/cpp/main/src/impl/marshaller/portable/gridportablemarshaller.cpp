/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include <boost/unordered_map.hpp>

#include "gridgain/gridportable.hpp"
#include "gridgain/gridportableserializer.hpp"
#include "gridgain/gridportablereader.hpp"
#include "gridgain/gridportablewriter.hpp"

#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

int32_t cStringHash(const char* str) {
    int32_t hash = 0;

    int i = 0;

    while(str[i]) {
        hash = 31 * hash + str[i];

        i++;
    }

    return hash;
}

int32_t getFieldId(const char* fieldName, int32_t typeId, GridPortableIdResolver* idRslvr) {
    if (idRslvr) {
        boost::optional<int32_t> rslvrId = idRslvr->fieldId(typeId, fieldName);

        if (rslvrId.is_initialized())
            return rslvrId.get();
    }

    return cStringHash(fieldName);
}

int32_t getFieldId(const std::string& fieldName, int32_t typeId, GridPortableIdResolver* idRslvr) {
    if (idRslvr) {
        boost::optional<int32_t> rslvrId = idRslvr->fieldId(typeId, fieldName);

        if (rslvrId.is_initialized())
            return rslvrId.get();
    }

    return gridStringHash(fieldName);
}

boost::unordered_map<int32_t, GridPortableFactory*>& portableFactories() {
    static boost::unordered_map<int32_t, GridPortableFactory*> portableFactories;

    return portableFactories;
}

void registerPortableFactory(int32_t typeId, GridPortableFactory* factory) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = portableFactories();

    factories[typeId] = factory;
}

boost::unordered_map<int32_t, GridPortableFactory*>& systemPortableFactories() {
    static boost::unordered_map<int32_t, GridPortableFactory*> portableFactories;

    return portableFactories;
}

void registerSystemPortableFactory(int32_t typeId, GridPortableFactory* factory) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = systemPortableFactories();

    factories[typeId] = factory;
}

GridPortable* createPortable(int32_t typeId, GridPortableReader &reader) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = portableFactories();

    GridPortableFactory* factory = factories[typeId];

    if (!factory)
        throw GridClientPortableException("Unknown type id.");

    return static_cast<GridPortable*>(factory->newInstance(reader));
}

bool systemPortable(int32_t typeId) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = systemPortableFactories();
    
    boost::unordered_map<int32_t, GridPortableFactory*>::const_iterator factory = factories.find(typeId);
    
    return factory != factories.end();
}

GridPortable* createSystemPortable(int32_t typeId, GridPortableReader &reader) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = systemPortableFactories();

    GridPortableFactory* factory = factories[typeId];

    if (!factory)
        throw GridClientPortableException("Unknown system type id.");

    return static_cast<GridPortable*>(factory->newInstance(reader));
}

#define REGISTER_SYSTEM_TYPE(TYPE_ID, TYPE) \
    class GridPortableFactory_##TYPE : public GridPortableFactory {\
    public:\
        \
        GridPortableFactory_##TYPE() {\
            registerSystemPortableFactory(TYPE_ID, this);\
        }\
        \
        void* newInstance(GridPortableReader& reader) {\
            GridPortable* p = new TYPE;\
            \
            return p;\
        }\
    };\
    \
    GridPortableFactory_##TYPE factory_##TYPE;

REGISTER_SYSTEM_TYPE(GridClientAuthenticationRequest::TYPE_ID, GridClientAuthenticationRequest);
REGISTER_SYSTEM_TYPE(GridClientCacheRequest::TYPE_ID, GridClientCacheRequest);
REGISTER_SYSTEM_TYPE(GridClientLogRequest::TYPE_ID, GridClientLogRequest);
REGISTER_SYSTEM_TYPE(GridClientNodeBean::TYPE_ID, GridClientNodeBean);
REGISTER_SYSTEM_TYPE(GridClientMetricsBean::TYPE_ID, GridClientMetricsBean);
REGISTER_SYSTEM_TYPE(GridClientResponse::TYPE_ID, GridClientResponse);
REGISTER_SYSTEM_TYPE(GridClientTaskRequest::TYPE_ID, GridClientTaskRequest);
REGISTER_SYSTEM_TYPE(GridClientTaskResultBean::TYPE_ID, GridClientTaskResultBean);
REGISTER_SYSTEM_TYPE(GridClientTopologyRequest::TYPE_ID, GridClientTopologyRequest);
