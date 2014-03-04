/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <iostream>
#include <vector>
#include <signal.h>

#include <boost/thread/thread.hpp>

#include <gridgain/gridgain.hpp>

#include "gridtestcommon.hpp"

using namespace std;

/** Interruption flag. */
TGridAtomicBool interrupt;

/** Global ever-living client. */
TGridClientPtr globalClient;

/**
 * SIGINT signal handler.
 */
void onSigint(int sig) {
    cout << "Interrupting...\n";

    interrupt = true;
}

/**
 * Performs a test actions on a client on each
 * thread iteration.
 *
 * @param client A client on which to perform iterations.
 */
void clientIteration(TGridClientPtr client) {
    TGridClientDataPtr data = client->data(CACHE_NAME);

    GridClientVariant key = getRandomName(boost::lexical_cast<string>(boost::this_thread::get_id()));
    GridClientVariant value = getRandomName("val");
    GridClientVariant value2 = getRandomName("val2");

    TGridClientVariantMap keyMap;
    keyMap[key] = value;

    TGridClientVariantSet keySet;
    keySet.push_back(key);

    data->affinity(key);

    data->put(key, value);
    data->get(key);
    data->replace(key, value2);
    data->remove(key);

    data->putAll(keyMap);
    data->getAll(keySet);
    data->removeAll(keySet);

    data->putAsync(key, value)->get();
    data->getAsync(key)->get();
    data->replaceAsync(key, value2)->get();
    data->removeAsync(key)->get();

    data->putAllAsync(keyMap)->get();
    data->getAllAsync(keySet)->get();
    data->replaceAsync(key, value2)->get();
    data->removeAllAsync(keySet)->get();

    data->metrics();

    data->metricsAsync()->get();

    TGridClientComputePtr compute = client->compute();

    try {
        compute->execute("MyUnexistentTask");
    }
    catch (GridClientException&) {}

    try {
        TGridClientFutureVariant fut = compute->executeAsync("MyUnexistentTask");
        fut->get();
    }
    catch (GridClientException&) {}

    GridClientVariant val1("val1");

    compute->execute("org.gridgain.client.GridClientStringLengthTask", val1);

    TGridClientFutureVariant fut = compute->executeAsync("org.gridgain.client.GridClientStringLengthTask", val1);
    fut->get();
}

/**
 * A worker thread's main loop.
 *
 * Each thread performs operations, described in clientIteration() with
 * globalClient and a locally created transient client.
 *
 * Thus we are trying to find memory leaks both in long-living and in
 * short-living clients.
 */
void worker(int niters) {
    for (int i = 0; i < niters && !interrupt; i++) {
        cout << "Thread " << boost::this_thread::get_id() << std::dec << ": running iteration " << i + 1 << "\n";

        if (i % 2 == 0) {
            clientIteration(globalClient);
        }
        else {
            TGridClientPtr transientClient = GridClientFactory::start(clientConfig());

            clientIteration(transientClient);

            GridClientFactory::stop(transientClient->id(), true);
        }
    }
}

int main(int argc, const char** argv) {
    interrupt = false;

    ::signal(SIGINT, onSigint);

    int nthreads = readArg(argc, argv, "-nthreads", 4);

    int niters = readArg(argc, argv, "-niters", 1000);

    globalClient = GridClientFactory::start(clientConfig());

    vector<boost::thread> threads;

    cout << "Starting threads...\n";

    for (int i = 0; i < nthreads; i++) {
        threads.push_back(boost::thread(boost::bind(&worker, niters)));
    }

    // Wait for threads to finish.
    for (vector<boost::thread>::iterator i = threads.begin(); i < threads.end(); i++) {
        i->join();

        cout << "Thread finished.\n";
    }

    GridClientFactory::stop(globalClient->id(), true);

    cout << "All done.\n";

    return EXIT_SUCCESS;
}
