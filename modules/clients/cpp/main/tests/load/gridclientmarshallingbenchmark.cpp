/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <vector>

#include <gridgain/gridgain.hpp>
#include <boost/thread.hpp>
#include <boost/program_options.hpp>
#include "boost/date_time/posix_time/posix_time.hpp"

#include "gridtestcommon.hpp"
#include "gridgain/gridclientnodemetricsbean.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacheresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacherequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemodifyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemetricsresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachegetresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskresult.hpp"
#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

/** Map of command line arguments. */
boost::program_options::variables_map vm;

/**
 * Returns a random int between 0 and max. Thread-safe.
 *
 * @param max A maximum value of a random integer.
 * @param seed A seed to use. Modifiable. Needs to be passed each time.
 */
int randomInt(int max, unsigned int* seed) {
    return rand() % (max + 1);
}

const long ITERS = 1000000;

/**
 * Class representing one thread testing the marshalling.
 */
class TestThread : private boost::noncopyable {

public:
    /**
     * Constructs the test thread.
     *
     * @param iterationCnt How many iterations to perform.
     */
    TestThread(int seed) {
        //seed = time(NULL);

        thread = boost::thread(boost::bind(&TestThread::run, this, seed));
    }

    // TODO: 8536 change to portable marshal test .
    void run(int seed) {
        srand(seed);
        
        GridPortableMarshaller marsh(false);

        GridCacheRequestCommand cmd = GridCacheRequestCommand(GridCacheRequestCommand::GridCacheOperation::PUT);

        vector<int8_t> token(1000, 1);

        cmd.sessionToken(token);
        cmd.setCacheName("partitioned");
        cmd.setClientId(GridClientUuid("550e8400-e29b-41d4-a716-446655440000"));
        cmd.setDestinationId(GridClientUuid("550e8400-e29b-41d4-a716-446655440000"));
        cmd.setKey(42.0f);
        cmd.setRequestId(42);
        cmd.setValue(42.0f);
        
        /*
        std::cout << "Run " << seed << "\n";

        for (int i = 0; i < 1000; i++)
            std::cout << "Rnd " << seed << " it " << i << " " << randomInt(100, nullptr) << "\n";
        */
        
        for (int i = 0; i < ITERS; i++) {
            // randomInt(100, nullptr);
            GridClientCacheRequest msg;

            msg.init(cmd);

            //vector<int8_t> bytes = marsh.marshal(msg);

            //if (i == 0)
               // std::cout << "Size " << bytes.size() << "\n";
        }
    }

    /**
     * Thread proc for marshalling specific types of messaging.
     *
     * @param messageType Type of messages to marshal.
     */
     /*
    void run(ObjectWrapperType messageType) {

        using namespace org::gridgain::grid::kernal::processors::rest::client::message;

        iters = 0;

        try {
            bool unmarshal = vm["unmarshal"].as<bool>();
            ObjectWrapper marshalledObject;
            int maxiterations = vm["nummessages"].as<int>();

            if ((messageType > NONE && messageType <= STRING) || messageType == UUID) {
                GridClientVariant value;

                switch (messageType) {
                    case BOOL:
                        value = true;

                        break;

                    case BYTE:
                        value = (int8_t) 42;

                        break;

                    case SHORT:
                        value = (int16_t) 42;

                        break;

                    case INT32:
                        value = (int32_t) 42;

                        break;

                    case INT64:
                        value = (int64_t) 42;

                        break;

                    case FLOAT:
                        value = (float) 0.42f;

                        break;

                    case DOUBLE:
                        value = (double) 0.42f;

                        break;

                    case STRING:
                        value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.";

                        break;

                    case UUID: { // block of code to avoid compilation error in gcc
                            value.set(GridClientUuid("550e8400-e29b-41d4-a716-446655440000"));
                            if (unmarshal)
                                std::cerr << "Unmarshalling UUID is not supported yet";
                        }

                        break;

                    default:
                        std::cerr << "Unsupported message type.\n";
                        return;
                }

                while (++iters != maxiterations) {
                    GridClientObjectWrapperConvertor::wrapSimpleType(value, marshalledObject);

                    if (unmarshal && messageType != UUID) {
                        value.clear();
                        GridClientObjectWrapperConvertor::unwrapSimpleType(marshalledObject, value);
                    }
                }
            }
            else {
                switch (messageType) {

                    case CACHE_REQUEST: { // block of code to avoid compilation error in gcc
                            GridCacheRequestCommand cmd = GridCacheRequestCommand(
                                            GridCacheRequestCommand::GridCacheOperation::PUT);

                            cmd.sessionToken("Something");
                            cmd.setCacheName("partitioned");
                            cmd.setClientId(GridClientUuid("550e8400-e29b-41d4-a716-446655440000"));
                            cmd.setDestinationId(GridClientUuid("550e8400-e29b-41d4-a716-446655440000"));
                            cmd.setKey(42.0f);
                            cmd.setRequestId(42);
                            cmd.setValue(42.0f);

                            // typical reply for cache PUT. 15 bytes length;
                            unsigned char daReply[] = { 8, 0, 26, 5, 8, 1, 18, 1, 1, 34, 0 };

                            GridClientMessageCacheModifyResult resp;

                            while (++iters != maxiterations) {
                                GridClientProtobufMarshaller::wrap(cmd, marshalledObject);
                                if (unmarshal) {
                                    marshalledObject.set_binary(daReply, sizeof(daReply));
                                    marshalledObject.set_type(RESPONSE);
                                    GridClientProtobufMarshaller::unwrap(marshalledObject, resp);
                                }
                            }
                        }

                        break;

                    case TOPOLOGY_REQUEST: { // block of code to avoid compilation error in gcc
                            GridTopologyRequestCommand cmd;

                            // typical reply for topology. Contains info about 2 local nodes. 185 bytes length;
                            unsigned char daReply[] = { 8, 0, 26, 173, 1, 8, 10, 18, 168, 1, 10, 165, 1, 8, 60, 18, 160,
                                            1, 10, 16, 6, 225, 6, 102, 61, 76, 77, 123, 129, 135, 99, 184, 120, 177, 249,
                                            186, 18, 9, 49, 50, 55, 46, 48, 46, 48, 46, 49, 26, 9, 49, 50, 55, 46, 48,
                                            46, 48, 46, 49, 32, 224, 78, 40, 144, 63, 74, 88, 10, 25, 10, 6, 8, 9, 18,
                                            2, 116, 120, 18, 15, 8, 9, 18, 11, 80, 65, 82, 84, 73, 84, 73, 79, 78, 69,
                                            68, 10, 28, 10, 9, 8, 9, 18, 5, 113, 117, 101, 114, 121, 18, 15, 8, 9, 18,
                                            11, 80, 65, 82, 84, 73, 84, 73, 79, 78, 69, 68, 10, 29, 10, 10, 8, 9, 18, 6,
                                            97, 116, 111, 109, 105, 99, 18, 15, 8, 9, 18, 11, 80, 65, 82, 84, 73, 84, 73,
                                            79, 78, 69, 68, 104, 128, 1, 114, 19, 8, 9, 18, 15, 49, 50, 55, 46, 48, 46,
                                            48, 46, 49, 58, 52, 55, 53, 48, 48, 34, 0 };

                            GridClientMessageTopologyResult resp;

                            while (++iters != maxiterations) {
                                GridClientProtobufMarshaller::wrap(cmd, marshalledObject);
                                if (unmarshal) {
                                    marshalledObject.set_binary(daReply, sizeof(daReply));
                                    marshalledObject.set_type(RESPONSE);
                                    GridClientProtobufMarshaller::unwrap(marshalledObject, resp);
                                }
                            }
                        }

                        break;

                    default:
                        std::cerr << "Unsupported message type.\n";
                        return;
                }
            }
        }
        catch (GridClientException& e) {
            std::cerr << "GridClientException: " << e.what() << "\n";
            exit(1);
        }
        catch (...) {
            std::cerr << "Unknown exception.\n";
            exit(1);
        }
    }
    */

    /** Joins the test thread. */
    void join() {
        thread.join();
    }

    /** Returns number of iterations completed. */
    int getIters() {
        return iters;
    }

private:
    /** Thread implementation. */
    boost::thread thread;

    /** Number of completed iterations. */
    int iters;

    /** A random seed used as a state for thread-safe random functions. */
    unsigned int seed;
};

typedef std::shared_ptr<TestThread> TestThreadPtr;

int main(int argc, const char** argv) {
    using namespace std;
    using namespace boost::program_options;

    // initialize random seed
    /*
    srand(time(NULL));

    // Declare the supported options.
    options_description desc("Allowed options");
    desc.add_options()("help", "produce help message")("threads", value<int>(), "Number of threads")("messagetype",
        value<int>(), "Type of messages to process, value from enum ObjectWrapperType in ClientMessages.pb.h")(
        "nummessages", value<int>(), "Number of messages to process per thread")("unmarshal",
        boost::program_options::value<bool>(), "Unmarshal marshalled messages (bool)");

    store(parse_command_line(argc, argv, desc), vm);
    notify(vm);

    if (!vm["help"].empty()) {
        std::cout << desc;
        exit(1);
    }
    int totalOps = 0;
    double totalSecs = 0.0f;
    /*

    /*
    if ((ObjectWrapperType) vm["messagetype"].as<int>() != NONE) {
        std::vector<TestThreadPtr> workers;

        for (int i = 0; i < vm["threads"].as<int>(); i++) {
            workers.push_back(TestThreadPtr(new TestThread((ObjectWrapperType) vm["messagetype"].as<int>())));
        }

        boost::posix_time::ptime time_start(boost::posix_time::microsec_clock::local_time());

        //join all threads
        for (std::vector<TestThreadPtr>::iterator i = workers.begin(); i != workers.end(); i++) {
            (*i)->join();
            totalOps += (*i)->getIters();
        }

        boost::posix_time::ptime time_end(boost::posix_time::microsec_clock::local_time());
        boost::posix_time::time_duration duration(time_end - time_start);
        totalSecs += duration.total_milliseconds() / 1000.0f;

        workers.clear();
    }
    else {
        for (int itertype = (int) BOOL; itertype <= (int) TASK_BEAN; ++itertype) {
            std::vector<TestThreadPtr> workers;

            for (int i = 0; i < vm["threads"].as<int>(); i++) {
                workers.push_back(TestThreadPtr(new TestThread((ObjectWrapperType) itertype)));
            }

            boost::posix_time::ptime time_start(boost::posix_time::microsec_clock::local_time());

            //join all threads
            for (std::vector<TestThreadPtr>::iterator i = workers.begin(); i != workers.end(); i++) {
                (*i)->join();
                totalOps += (*i)->getIters();
            }

            boost::posix_time::ptime time_end(boost::posix_time::microsec_clock::local_time());
            boost::posix_time::time_duration duration(time_end - time_start);
            totalSecs += duration.total_milliseconds() / 1000.0f;

            workers.clear();
        }
    }
    */

    int totalOps = 0;
    double totalSecs = 0.0f;

    std::vector<TestThreadPtr> workers;

    for (int i = 0; i < 1; i++) {
        workers.push_back(TestThreadPtr(new TestThread(i)));
    }
    
    boost::posix_time::ptime time_start(boost::posix_time::microsec_clock::local_time());

    for (std::vector<TestThreadPtr>::iterator i = workers.begin(); i != workers.end(); i++) {
        (*i)->join();
        
        totalOps += ITERS;
    }

    boost::posix_time::ptime time_end(boost::posix_time::microsec_clock::local_time());
    boost::posix_time::time_duration duration(time_end - time_start);
    totalSecs += duration.total_milliseconds() / 1000.0f;

    workers.clear();

    std::cout << "Total ops/sec " << totalOps / totalSecs << std::endl;

    return EXIT_SUCCESS;
}
