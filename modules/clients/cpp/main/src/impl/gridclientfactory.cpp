/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <cassert>

#include "gridgain/gridclienttypedef.hpp"
#include "gridgain/gridclientvariant.hpp"
#include "gridgain/gridclientfactory.hpp"
#include "gridgain/impl/gridclientimpl.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/cmd/gridclienttcpcommandexecutor.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

class FactoryImpl {
public:

    TGridClientPtr start(const GridClientConfiguration& cfg) {
        boost::shared_ptr<GridClientConnectionPool> connPool(new GridClientConnectionPool(cfg));

        std::shared_ptr<GridClientCommandExecutorPrivate> exec;

        exec.reset(new GridClientTcpCommandExecutor(connPool));

        std::shared_ptr<GridClientImpl> client(new GridClientImpl(cfg, exec));

        boost::lock_guard<boost::mutex> lock(mux);

        clients[client->id()] = client;

        GG_LOG_INFO("Client started [id=%s, protocol=TCP]",
            client->id().uuid().c_str());

        return client;
    }

    void stopAll(bool wait) {
        boost::lock_guard<boost::mutex> lock(mux);

        for (auto it = clients.begin(); it != clients.end(); ++it) {
            std::shared_ptr<GridClientImpl> cliPtr = it->second;

            assert(cliPtr.get() !=NULL);

            GG_LOG_INFO("Stopping client [id=%s, wait=%s]", cliPtr->id().uuid().c_str(), wait ? "true" : "false");

            cliPtr->stop(wait);

            GG_LOG_INFO("Client stopped [id=%s]", cliPtr->id().uuid().c_str());
        }

        clients.clear();
    }

    void stop(const GridClientUuid clientId, bool wait) {
        std::shared_ptr<GridClientImpl> client;

        {
            boost::lock_guard<boost::mutex> lock(mux);

            if (clients.count(clientId) == 0)
                return;

            client = clients[clientId];

            clients.erase(clientId);
        }

        assert(client.get() != NULL);

        client->stop(wait);
    }

    /** Typedef for list of produced client. */
    typedef std::map<GridClientUuid, std::shared_ptr<GridClientImpl> > TClientList;

    /** List of produced client. */
    TClientList clients;

    /** Mutex for operations that should be synchronized. */
    boost::mutex mux;
};

/** A single instance of the client factory */
static FactoryImpl impl;

/**
 * Starts a client with given configuration. Starting client will be assigned a randomly generated
 * UUID which can be obtained by {@link GridClient#id()} method.
 *
 * @param cfg Client configuration.
 * @return Started client.
 * @throws GridClientException If client could not be created.
 */
TGridClientPtr GridClientFactory::start(const GridClientConfiguration& cfg) {
    return impl.start(cfg);
}

/**
 * Stops all currently open clients.
 *
 * @param wait If {@code true} then each client will wait to finish all ongoing requests before
 *      closing (however, no new requests will be accepted). If {@code false}, clients will be
 *      closed immediately and all ongoing requests will be failed.
 */
void GridClientFactory::stopAll(bool wait) {
    impl.stopAll(wait);
}

/**
 * Stops particular client.
 *
 * @param clientId Client identifier to close.
 * @param wait If {@code true} then client will wait to finish all ongoing requests before
 *      closing (however, no new requests will be accepted). If {@code false}, client will be
 *      closed immediately and all ongoing requests will be failed.
 */
void GridClientFactory::stop(const GridClientUuid& clientId, bool wait) {
    impl.stop(clientId, wait);
}
