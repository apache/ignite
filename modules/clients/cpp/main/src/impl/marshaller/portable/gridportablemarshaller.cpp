/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>

#include "gridgain/gridportable.hpp"
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
        int32_t rslvrId = idRslvr->fieldId(typeId, fieldName);

        if (rslvrId != 0)
            return rslvrId;
    }

    std::string name(fieldName);

    boost::to_lower(name);

    return gridStringHash(name);
}

int32_t getFieldId(const std::string& fieldName, int32_t typeId, GridPortableIdResolver* idRslvr) {
    if (idRslvr) {
        int32_t rslvrId = idRslvr->fieldId(typeId, fieldName);

        if (rslvrId != 0)
            return rslvrId;
    }

    return gridStringHash(boost::to_lower_copy(fieldName));
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

GridPortable* createPortable(int32_t typeId) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = portableFactories();

    GridPortableFactory* factory = factories[typeId];

    if (!factory) {
        std::ostringstream msg;

        msg << "Unknown type id: " << typeId;
        
        throw GridClientPortableException(msg.str());
    }

    return static_cast<GridPortable*>(factory->newInstance());
}

bool systemPortable(int32_t typeId) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = systemPortableFactories();
    
    boost::unordered_map<int32_t, GridPortableFactory*>::const_iterator factory = factories.find(typeId);
    
    return factory != factories.end();
}

GridPortable* createSystemPortable(int32_t typeId) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = systemPortableFactories();

    GridPortableFactory* factory = factories[typeId];

    if (!factory) {
        std::ostringstream msg;

        msg << "Unknown system type id: " << typeId;
        
        throw GridClientPortableException(msg.str());
    }

    return static_cast<GridPortable*>(factory->newInstance());
}

#define REGISTER_SYSTEM_TYPE(TYPE) \
    class GridPortableFactory_##TYPE : public GridPortableFactory {\
    public:\
        \
        GridPortableFactory_##TYPE() {\
            TYPE t;\
            registerSystemPortableFactory(t.typeId(), this);\
        }\
        \
        virtual ~GridPortableFactory_##TYPE() {\
        }\
        \
        void* newInstance() {\
            GridPortable* p = new TYPE;\
            \
            return p;\
        }\
    };\
    \
    GridPortableFactory_##TYPE factory_##TYPE;

REGISTER_SYSTEM_TYPE(GridClientAuthenticationRequest);
REGISTER_SYSTEM_TYPE(GridClientCacheRequest);
REGISTER_SYSTEM_TYPE(GridClientLogRequest);
REGISTER_SYSTEM_TYPE(GridClientNodeBean);
REGISTER_SYSTEM_TYPE(GridClientMetricsBean);
REGISTER_SYSTEM_TYPE(GridClientResponse);
REGISTER_SYSTEM_TYPE(GridClientTaskRequest);
REGISTER_SYSTEM_TYPE(GridClientTaskResultBean);
REGISTER_SYSTEM_TYPE(GridClientTopologyRequest);
REGISTER_SYSTEM_TYPE(GridClientCacheQueryRequest);
REGISTER_SYSTEM_TYPE(GridClientDataQueryResult);
