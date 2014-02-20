// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <iostream>
#include <stdio.h> //printf
#include <string.h>    //strlen
#include <signal.h>
#include <sys/socket.h>    //socket
#include <arpa/inet.h> //inet_addr
#include <netdb.h> //hostent
#include <boost/program_options.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

namespace ggtest {
    using namespace std;

    /**
     Simple class that wraps TCP connection.
     */
    class tcp_client {
    private:
        int sock;
        std::string address;
        int port;
        struct sockaddr_in server;

    public:
        tcp_client();
        bool conn(string, int);
        bool sendbuffers(char * pBuffer, unsigned int bufferSize);
        int recvbuffers(char * pBuffer, unsigned bufferSize);
    };

    tcp_client::tcp_client() {
        sock = -1;
        port = 0;
        address = "";
    }

    /**
     Connect to a host on a certain port number.
     */
    bool tcp_client::conn(string address, int port) {
        //create socket if it is not already created
        if (sock == -1) {
            //Create socket
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock == -1) {
                perror("Could not create socket.");

                exit(-1);
            }

            cout << "Socket created\n";
        } else { /* OK , nothing */
        }

        //setup address structure
        if (inet_addr(address.c_str()) == (uint32_t) -1) {
            struct hostent *he;
            struct in_addr **addr_list;

            //resolve the hostname, its not an ip address
            if ((he = gethostbyname(address.c_str())) == NULL) {
                //gethostbyname failed
                herror("gethostbyname");
                cout << "Failed to resolve hostname\n";

                return false;
            }

            //Cast the h_addr_list to in_addr , since h_addr_list also has the ip address in long format only
            addr_list = (struct in_addr **) he->h_addr_list;

            for (int i = 0; addr_list[i] != NULL; i++) {
                //strcpy(ip , inet_ntoa(*addr_list[i]) );
                server.sin_addr = *addr_list[i];

                cout << address << " resolved to " << inet_ntoa(*addr_list[i]) << endl;

                break;
            }
        }

        //plain ip address
        else {
            server.sin_addr.s_addr = inet_addr(address.c_str());
        }

        server.sin_family = AF_INET;
        server.sin_port = htons(port);

        //Connect to remote server
        if (connect(sock, (struct sockaddr *) &server, sizeof(server)) < 0) {
            perror("connect failed. Error");
            return 1;
        }

        cout << "Connected\n";
        return true;
    }

    /**
     Send data to the connected host
     */
    bool tcp_client::sendbuffers(char * pBuffer, unsigned int bufferSize) {
        //Send some data
        if (send(sock, pBuffer, bufferSize, 0) < 0) {
            perror("Send failed : ");
            return false;
        }

        return true;
    }

    /**
     Receive data from the connected host
     */
    int tcp_client::recvbuffers(char * pBuffer, unsigned bufferSize) {
        int bytesRead;
        bytesRead = recv(sock, pBuffer, bufferSize, 0);

        //Receive a reply from the server
        if (bytesRead < 0) {
            perror("recv failed");
        }

        return bytesRead;
    }

    // single socket for communication
    tcp_client gSharedSocket;
    // mutex to guard single socket writes
    boost::mutex gWriteMutex;
    // number of sockets to test with
    int gNumSockets;
    // NIO echo server port and host
    std::string gHost;
    int gPort;
    // Default values for buffer size and single message size in bytes
    static const int MAX_BUFFER_SIZE = 4 * 1024 * 1024; // 4Mb
    static const int MESSAGE_SIZE = 512;
    // Stats used to measure performance
    unsigned long long gBytesSent, gBytesReceived, gMessagesSent, gMessagesReceived;
    boost::posix_time::ptime gStart, gStop;
    boost::posix_time::time_duration gDiff;
    unsigned long long * gArrayPendingReads;
    // two arrays that are used to syncronize transactions across threads
    boost::condition_variable * gArrayPeningReadsCVs;
    boost::mutex * gArrayReadMutexes;
    // global bool to handle control flow
    bool gExit;

    void ctrlchandler(int) {
        // print stats and exit
        std::cout << "Bytes sent: " << gBytesSent << " ";
        std::cout << "Bytes recvd: " << gBytesReceived << " ";
        std::cout << "Messages sent: " << gMessagesSent << " ";
        std::cout << "Messages sent: " << gMessagesReceived << std::endl;
        boost::this_thread::sleep(boost::posix_time::seconds(1));
        gStop = boost::posix_time::microsec_clock::local_time();
        gDiff = gStop - gStart;
        unsigned long long totalMSSinceStart = gDiff.total_milliseconds();
        std::cout << "TX/RX throughput: " << (gBytesSent * 2) / ((double) totalMSSinceStart / 1000 * 1024 * 1024)
                << " Mb/sec ";
        std::cout << gMessagesReceived / ((double) totalMSSinceStart / 1000) << " transactions/sec " << std::endl;
        gExit = true;
    }

    void printOutStats() {
        while (!gExit) {
            boost::this_thread::sleep(boost::posix_time::seconds(1));
            gStop = boost::posix_time::microsec_clock::local_time();
            gDiff = gStop - gStart;
            unsigned long long totalMSSinceStart = gDiff.total_milliseconds();
            std::cout << "TX/RX throughput: " << (gBytesSent * 2) / ((double) totalMSSinceStart / 1000 * 1024 * 1024)
                    << " Mb/sec ";
            std::cout << gMessagesReceived / ((double) totalMSSinceStart / 1000) << " transactions/sec " << std::endl;
        }
    }

    // this function is used to exchange a single message with NIO echo server in thread-per-socket mode
    void doFullTransaction(tcp_client * pSocket, char * pRecvBuffers) {

        int bytesToSend = MESSAGE_SIZE; //rand()%MAX_BUFFER_SIZE;

        int payloadLenInt = bytesToSend - 4;

        // invert endianness ( native is LE, Java is BE )
        pRecvBuffers[3] = (char) (payloadLenInt & 0xFF);
        pRecvBuffers[2] = (char) ((payloadLenInt & 0xFF00) >> 8);
        pRecvBuffers[1] = (char) ((payloadLenInt & 0xFF0000) >> 16);
        pRecvBuffers[0] = (char) ((payloadLenInt & 0xFF000000) >> 24);

        //send some data
        pSocket->sendbuffers(pRecvBuffers, bytesToSend);

        //receive and assert length
        int bytesRecievedTotal = 0;

        while (bytesRecievedTotal < bytesToSend)
            bytesRecievedTotal += pSocket->recvbuffers(pRecvBuffers, bytesToSend);

        if (bytesRecievedTotal != bytesToSend)
            perror("Echo request/reply size mismatch!");

        // update global atomic stats
        gBytesSent += bytesToSend;
        gBytesReceived += bytesRecievedTotal;
        ++gMessagesSent;
        ++gMessagesReceived;
    }

    // thread proc for "Reader" thread. Continuously reads from single shared socket and dispatches notifications to worker threads
    void threadReaderSharedSocket() {

        char * pRecvBuffers = new char[MAX_BUFFER_SIZE];
        memset(pRecvBuffers, 'A', MAX_BUFFER_SIZE);

        while (!gExit) {
            int bytesRecievedTotal = 0;
            while (bytesRecievedTotal < MESSAGE_SIZE)
                bytesRecievedTotal += gSharedSocket.recvbuffers(pRecvBuffers, MESSAGE_SIZE);

            if (bytesRecievedTotal != MESSAGE_SIZE)
                perror("Echo request/reply size mismatch!");

            int threadId = *((int*) &pRecvBuffers[4]);

            {
                boost::lock_guard<boost::mutex> lock(gArrayReadMutexes[threadId]);
                ++(gArrayPendingReads[threadId]);
            }
            gArrayPeningReadsCVs[threadId].notify_one();

            // update global atomic stats
            gBytesReceived += bytesRecievedTotal;
            ++gMessagesReceived;
        }

        // wake up all threads so they have a chance to exit
        for (int i=0; i < gNumSockets; ++i)
        {
            {
                boost::lock_guard<boost::mutex> lock(gArrayReadMutexes[i]);
                ++(gArrayPendingReads[i]);
            }
            gArrayPeningReadsCVs[i].notify_one();
        }

        delete[] pRecvBuffers;
    }

    void threadProcSharedSocket(int threadId) {

        char * pSendBuffers = new char[MAX_BUFFER_SIZE];
        memset(pSendBuffers, 'A', MAX_BUFFER_SIZE);

        while (!gExit) {
            int bytesToSend;
            // Write message
            {
                boost::unique_lock<boost::mutex> lock(gWriteMutex);

                bytesToSend = MESSAGE_SIZE; //rand()%MAX_BUFFER_SIZE;

                int payloadLenInt = bytesToSend - 4;

                // Invert endianness ( Native is LE, Java is BE )
                pSendBuffers[3] = (char) (payloadLenInt & 0xFF);
                pSendBuffers[2] = (char) ((payloadLenInt & 0xFF00) >> 8);
                pSendBuffers[1] = (char) ((payloadLenInt & 0xFF0000) >> 16);
                pSendBuffers[0] = (char) ((payloadLenInt & 0xFF000000) >> 24);

                // Stamp on threadId for reply dispatch
                *((int*) &pSendBuffers[4]) = threadId;

                //send some data
                gSharedSocket.sendbuffers(pSendBuffers, bytesToSend);
                gBytesSent += bytesToSend;
                ++gMessagesSent;
            }
            // wait for notification from reader thread that reply has been received
            {
                boost::unique_lock<boost::mutex> lock(gArrayReadMutexes[threadId]);
                while (gArrayPendingReads[threadId] == 0) {
                    gArrayPeningReadsCVs[threadId].wait(lock);
                }
                    --(gArrayPendingReads[threadId]);
            }
        }

        delete[] pSendBuffers;
    }

    // threadproc for thread-per-socket mode
    void threadProcOwnSocket() {
        tcp_client c;
        c.conn(gHost, gPort);

        char * pRecvBuffers = new char[MAX_BUFFER_SIZE];
        memset(pRecvBuffers, 'A', MAX_BUFFER_SIZE);

        while (!gExit) {
            doFullTransaction(&c, pRecvBuffers);
        }
        delete[] pRecvBuffers;
    }
}

int main(int argc, char * argv[]) {

    using namespace boost::program_options;
    using namespace ggtest;

    gBytesSent = 0;
    gBytesReceived = 0;
    gMessagesSent = 0;
    gMessagesReceived = 0;
    gExit = false;

    // Declare the supported options.
    options_description desc("Allowed options");
    desc.add_options()("help", "produce help message")("host", value<string>(), "Host to connect to")("port",
            value<int>(), "Port to connect to")("sockets", value<int>(), "Number of sockets")("singlethreaded",
            boost::program_options::value<bool>(), "Singlethreaded implementation if set to true");

    variables_map vm;
    store(parse_command_line(argc, argv, desc), vm);
    notify(vm);

    if (vm.count("help")) {
        cout << desc << "\n";
    }

    bool bValidConfig = true;

    if (vm.count("sockets")) {
        cout << "Number of sockets set to " << vm["sockets"].as<int>() << endl;
    } else
        bValidConfig = false;

    if (vm.count("host") && vm.count("port")) {
        cout << "Server is " << vm["host"].as<string>() << ":" << vm["port"].as<int>() << endl;
    } else
        bValidConfig = false;

    if (vm.count("singlethreaded")) {
        if (vm["singlethreaded"].as<bool>())
            cout << "Single thread" << endl;
        else
            cout << "Thread per socket" << endl;
    } else
        bValidConfig = false;

    if (bValidConfig) {
        std::cout << std::setprecision(12) << "Starting!" << std::endl;

        // Register for SIGINT
        signal(SIGINT, ctrlchandler);

        gStart = boost::posix_time::microsec_clock::local_time();

        gNumSockets = vm["sockets"].as<int>();
        gHost = vm["host"].as<string>();
        gPort = vm["port"].as<int>();

        if (vm["singlethreaded"].as<bool>()) {
            // do stuff using single thread

            //connect to host
            gSharedSocket.conn(gHost, gPort);

            gArrayPendingReads = new unsigned long long[gNumSockets];
            gArrayPeningReadsCVs = new boost::condition_variable[gNumSockets];
            gArrayReadMutexes = new boost::mutex[gNumSockets];

            boost::thread printStatsThread(printOutStats);
            boost::thread sharedSocketReaderThread(threadReaderSharedSocket);
            std::vector<boost::shared_ptr<boost::thread> > writerThreads;
            writerThreads.resize(gNumSockets);

            int counter = 0;
            for (std::vector<boost::shared_ptr<boost::thread> >::iterator itThread = writerThreads.begin();
                    itThread != writerThreads.end(); ++itThread) {
                gArrayPendingReads[counter] = 0;
                itThread->reset(new boost::thread(threadProcSharedSocket, counter));
                ++counter;
            }

            for (std::vector<boost::shared_ptr<boost::thread> >::iterator itThread = writerThreads.begin();
                    itThread != writerThreads.end(); ++itThread)
                itThread->get()->join();
            printStatsThread.join();

            delete[] gArrayPendingReads;
            delete[] gArrayPeningReadsCVs;
            delete[] gArrayReadMutexes;

        } else {
            // do stuff using one thread per socket
            boost::thread printStatsThread(printOutStats);
            std::vector<boost::shared_ptr<boost::thread> > threads;
            threads.resize(gNumSockets);

            for (std::vector<boost::shared_ptr<boost::thread> >::iterator itThread = threads.begin();
                    itThread != threads.end(); ++itThread)
                itThread->reset(new boost::thread(threadProcOwnSocket));

            for (std::vector<boost::shared_ptr<boost::thread> >::iterator itThread = threads.begin();
                    itThread != threads.end(); ++itThread)
                itThread->get()->join();
            printStatsThread.join();
        }
    } else {
        cout << desc << endl;
    }

    return 0;
}
