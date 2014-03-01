/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <ctime>
#include <sys/time.h>
#include <deque>
#include <string>

#include <gridgain/gridgain.hpp>
#include <boost/lexical_cast.hpp>

#include "gridtestcommon.hpp"

using namespace std;

/**
 * The test performs the specified number of cache puts
 * using async operations, and then waits for all those
 * async operations to complete via the received
 * futures.
 *
 * Commmand line parameters:
 *
 * - "-nops" Number of cache operations to perform.
 */
int main(int argc, const char** argv) {
    const unsigned int ntasks = readArg(argc, argv, "-nops", 6000000); //6M by default

    GridClientConfiguration cfg = clientConfig();

    TGridClientPtr client = GridClientFactory::start(cfg);

    TGridClientDataPtr data = client->data(CACHE_NAME);

    std::deque<TGridBoolFuturePtr> futureQueue;

    double startTime = currentTimeMillis();

    double prevTime = startTime;

    cout << "The test will perform " << ntasks << " asynchronous operations with cache.\n";

    for(unsigned int i = 0; i < ntasks; i++) {
        try {
            GridClientVariant varKey(getRandomName("key"));
            GridClientVariant varValue("val");

            TGridBoolFuturePtr fut = data->putAsync(varKey, varValue);

            futureQueue.push_back(fut);

            if(i > 0 && i % 1000000 == 0) {
                cout << "Started " << i << " operations.\n";
            }
        }
        catch(...) {
            cerr << "Got exception at operation " << i << "\n";
            throw;
        }
    }

    cout << "Waiting for operations to complete...\n";

    while(!futureQueue.empty()) {
        TGridBoolFuturePtr fut = futureQueue.front();
        futureQueue.pop_front();

        if(!fut->get()) {
            cerr << "Operation number " << ntasks - futureQueue.size() << " failed!\n";
        }
        else {
            size_t qsize = futureQueue.size();

            if(qsize > 0 && qsize % 1000 == 0) {
                double currentTime = currentTimeMillis();

                cout << qsize << " operations left (current rate: 1000 ops/"
                        << currentTime - prevTime << " ms)\n";

                prevTime = currentTime;
            }
        }
    }

    cout << "All operations have completed. Total time: " << currentTimeMillis() - startTime << "ms.\n";

    return 0;
}
