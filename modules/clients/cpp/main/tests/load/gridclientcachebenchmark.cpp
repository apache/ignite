// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <vector>

#include <gridgain/gridgain.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/foreach.hpp>
#include <boost/program_options.hpp>
#include <boost/timer.hpp>

#include "gridtestcommon.hpp"
#include "gridgain/impl/gridclientpartitionedaffinity.hpp"

std::atomic < unsigned long long > gIters;

/** Maximum number of distinct keys. */
const int KEY_COUNT = 1000000;

/** Map of command line arguments */
boost::program_options::variables_map vm;

/** GridGain client interface. */
TGridClientPtr client;

/** Used to stop worker threads. */
TGridAtomicBool gExit;

/** Used to stop only collect status after warmup. */
TGridAtomicBool gWarmupDone;

enum GridClientCacheTestType {
    PUT = 0, GET, PUT_TX, GET_TX, NUM_TEST_TYPES
};

void StatsPrinterThreadProc()
{
    while(true)
    {
        std::cout << "Operations for last second: " << gIters << std::endl;
        gIters = 0;
        boost::this_thread::sleep(boost::posix_time::seconds(1));
    }
}


GridClientCacheTestType testTypeFromString(std::string typeName)
{
    if (!strcmp(typeName.c_str(), "PUT"))
        return PUT;
    else if (!strcmp(typeName.c_str(), "PUT_TX"))
        return PUT_TX;
    else if (!strcmp(typeName.c_str(), "GET_TX"))
        return GET_TX;
    else if (!strcmp(typeName.c_str(), "GET"))
        return GET;
    return NUM_TEST_TYPES;

}

/**
 * Returns a random int between 0 and max.
 *
 * @param max A maximum value of a random integer.
 * @param seed A seed to use. Modifiable. Needs to be passed each time.
 */
int randomInt(int max, unsigned int* seed) {
    return rand_r(seed) % (max + 1);
}

class TestThread: private boost::noncopyable {
public:
    /**
     * Constructs the test thread.
     *
     * @param iterationCnt How many iterations to perform.
     */
    TestThread(GridClientCacheTestType op) {
        iters = 1;

        seed = time(NULL);

        thread = boost::thread(boost::bind(&TestThread::run, this, op));
    }

    void run(GridClientCacheTestType opType) {
        try {
            TGridClientDataPtr data = client->data(vm["cachename"].as<string>());

            int value = 42;

            switch (opType) {
                case PUT: {
                    TGridClientVariantMap theMap;
                    theMap[42] = 42;
                    while (!gExit )
                    {
                        //                      data->put(randomInt(KEY_COUNT - 1, &seed), value);
                                                data->putAll(theMap);
                                                ++gIters;
                    }
                }
                    break;
                case PUT_TX: {
                }
                    break;
                case GET: {
                    while (!gExit && ++iters )
                        data->get((int16_t) randomInt(KEY_COUNT - 1, &seed));
                }
                    break;
                case GET_TX: {
                }
                    break;
                default:
                    std::cerr << "Invalid test operation.\n";
                    break;
                }

        } catch (GridClientException& e) {
            std::cerr << "GridClientException: " << e.what() << "\n";
        } catch (...) {
            std::cerr << "Unknown exception.\n";
        }
    }

    void join() {
        thread.join();
    }

    int getIters() {
        return iters;
    }

private:
    boost::thread thread;

    int iters;

    /** A random seed used as a state for thread-safe random functions. */
    unsigned int seed;
};

typedef std::shared_ptr<TestThread> TestThreadPtr;

int main(int argc, const char** argv) {

    gExit = false;
    gWarmupDone = false;

    using namespace std;
    using namespace boost::program_options;

    // initialize random seed
    srand(time(NULL));

    // Declare the supported options.
    options_description desc("Allowed options");
    desc.add_options()("help", "produce help message")("host", value<string>(),
            "Host to connect to")("port", value<int>(), "Port to connect to")(
            "threads", value<int>(), "Number of threads")("testtype",
            value<string>(), "Type of operations to run")("cachename",
            value<string>(), "Cache name")
            ("warmupseconds", value<int>(), "Seconds to warm up")
            ("runseconds", value<int>(), "Seconds to run")
            ("usetransactions",boost::program_options::value<bool>(), "Use transactions (bool)");

    store(parse_command_line(argc, argv, desc), vm);
    notify(vm);

    GridClientConfiguration cfg = clientConfig();

    std::vector<GridSocketAddress> servers;
    servers.push_back(GridSocketAddress(vm["host"].as<string>(), vm["port"].as<int>())); // localhost:11211

    cfg.servers(servers);

    GridClientDataConfiguration cacheCfg;

    // Set remote cache name.
    cacheCfg.name(vm["cachename"].as<string>());

    std::shared_ptr<GridClientDataAffinity> ptrAffinity(new GridClientPartitionAffinity());

    // Set client partitioned affinity for this cache.
    cacheCfg.affinity(ptrAffinity);

    std::vector<GridClientDataConfiguration> dataConfigurations;
    dataConfigurations.push_back(cacheCfg);

    cfg.dataConfiguration(dataConfigurations);


    client = GridClientFactory::start(cfg);
    std::vector<TestThreadPtr> workers;

    boost::thread printerThread(StatsPrinterThreadProc);

    int numThreads = vm["threads"].as<int>();
    for (int i = 0; i < numThreads; i++) {
        workers.push_back(TestThreadPtr(new TestThread(testTypeFromString(vm["testtype"].as<string>()))));
    }

    // let it warm up for requested amount of time and start gathering stats
    boost::this_thread::sleep(boost::posix_time::seconds( vm["warmupseconds"].as<int>()));
    gWarmupDone = true;

    // Let tests run for requested amount of time and then signal the exit
    boost::this_thread::sleep(boost::posix_time::seconds( vm["runseconds"].as<int>()));
    gExit = true;


    //join all threads
    for (std::vector<TestThreadPtr>::iterator i = workers.begin();
            i != workers.end(); i++) {
        (*i)->join();
    }

    workers.clear();
    client.reset();


    GridClientFactory::stopAll();



    return EXIT_SUCCESS;
}
