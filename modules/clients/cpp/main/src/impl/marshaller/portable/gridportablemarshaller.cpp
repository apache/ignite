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

GridPortable* createPortable(int32_t typeId, GridPortableReader &reader) {
    boost::unordered_map<int32_t, GridPortableFactory*>& factories = portableFactories();

    GridPortableFactory* factory = factories[typeId];

    if(!factory)
        throw GridClientPortableException("Unknown type id.");

    return static_cast<GridPortable*>(factory->newInstance(reader));
}

REGISTER_TYPE(-1, GridClientAuthenticationRequest);
REGISTER_TYPE(-2, GridClientCacheRequest);
REGISTER_TYPE(-3, GridClientLogRequest);
REGISTER_TYPE(-4, GridClientNodeBean);
REGISTER_TYPE(-5, GridClientMetricsBean);
REGISTER_TYPE(-6, GridClientResponse);
REGISTER_TYPE(-7, GridClientTaskRequest);
REGISTER_TYPE(-8, GridClientTaskResultBean);
REGISTER_TYPE(-9, GridClientTopologyRequest);
