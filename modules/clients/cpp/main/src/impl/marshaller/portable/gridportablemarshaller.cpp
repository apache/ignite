/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include <unordered_map>

#include "gridgain/gridportable.hpp"
#include "gridgain/gridportableserializer.hpp"
#include "gridgain/gridportablereader.hpp"
#include "gridgain/gridportablewriter.hpp"

#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

using namespace std;

unordered_map<int32_t, GridPortableFactory*>& portableFactories() {
    static unordered_map<int32_t, GridPortableFactory*> portableFactories;

    return portableFactories;
}

void registerPortableFactory(int32_t typeId, GridPortableFactory* factory) {
    unordered_map<int32_t, GridPortableFactory*>& factories = portableFactories();

    factories[typeId] = factory;
}

GridPortable* createPortable(int32_t typeId, GridPortableReader &reader) {
    unordered_map<int32_t, GridPortableFactory*>& factories = portableFactories();

    GridPortableFactory* factory = factories[typeId];

    assert(factory);

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
