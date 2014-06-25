/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include "gridtestcommon.hpp"

#include "gridgain/impl/connection/gridclientconnectionpool.hpp"

BOOST_AUTO_TEST_SUITE(GridClientConnectionPoolSelfTest)

BOOST_AUTO_TEST_CASE(testIdleConnection) {
    GridClientConfiguration cfg = clientConfig();

    const int64_t idleTime = 15000;

    cfg.maxConnectionIdleTime(idleTime);

    GridClientConnectionPool pool(cfg);

    GridClientSocketAddress serverAddr = cfg.servers()[0];

    std::shared_ptr<GridClientTcpConnection> conn =
            pool.rentTcpConnection(serverAddr.host(), serverAddr.port());

    pool.turnBack(conn);

    BOOST_CHECK_EQUAL( pool.getAvailableConnectionsCount(), 1 );

    boost::this_thread::sleep(boost::posix_time::milliseconds(idleTime + 5000));

    BOOST_CHECK_EQUAL( pool.getAvailableConnectionsCount(), 0 );
}

BOOST_AUTO_TEST_CASE(testIdleConnection2) {
    GridClientConfiguration cfg = clientConfig();

    const int64_t idleTime = 15000;

    cfg.maxConnectionIdleTime(idleTime);

    GridClientConnectionPool pool(cfg);

    GridClientSocketAddress serverAddr = cfg.servers()[0];

    std::shared_ptr<GridClientTcpConnection> conn =
            pool.rentTcpConnection(serverAddr.host(), serverAddr.port());

    BOOST_CHECK_EQUAL( pool.getAvailableConnectionsCount(), 0 );

    boost::this_thread::sleep(boost::posix_time::milliseconds(idleTime + 5000));

    pool.turnBack(conn);

    BOOST_CHECK_EQUAL( pool.getAvailableConnectionsCount(), 1 );

    boost::this_thread::sleep(boost::posix_time::milliseconds(5000));

    BOOST_CHECK_EQUAL( pool.getAvailableConnectionsCount(), 1 );

    boost::this_thread::sleep(boost::posix_time::milliseconds(idleTime));

    BOOST_CHECK_EQUAL( pool.getAvailableConnectionsCount(), 0 );
}

void doRentTurnBackConnections(std::shared_ptr<GridClientConnectionPool> pool) {
    TConnectionPtr conn = pool->rentTcpConnection(TEST_HOST, TEST_TCP_PORT);

    pool->turnBack(conn);
}

void doRentConnections(std::shared_ptr<GridClientConnectionPool> pool) {
    pool->rentTcpConnection(TEST_HOST, TEST_TCP_PORT);
}

BOOST_AUTO_TEST_CASE(testMultithreaded) {
    GridClientConfiguration cfg = clientConfig();

    const int64_t idleTime = 15000;

    cfg.maxConnectionIdleTime(idleTime);

    std::shared_ptr<GridClientConnectionPool> pool(new GridClientConnectionPool(cfg));

    const int nthreads = DLFT_THREADS_CNT;

    BOOST_CHECKPOINT( "Before first multithreaded run." );

    multithreaded(boost::bind(&doRentTurnBackConnections, pool), nthreads);

    BOOST_CHECKPOINT( "Passed first multithreaded run." );

    multithreaded(boost::bind(&doRentConnections, pool), nthreads);

    BOOST_CHECK_EQUAL( pool->getAvailableConnectionsCount(), 0 );
    BOOST_CHECK_EQUAL( pool->getAllConnectionsCount(), nthreads * 2 );

    pool->close();

    BOOST_CHECK_EQUAL( pool->getAllConnectionsCount(), 0 );
}

BOOST_AUTO_TEST_SUITE_END()
