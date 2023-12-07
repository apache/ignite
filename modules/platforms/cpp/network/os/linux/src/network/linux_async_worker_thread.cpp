/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <cstring>

#include <ignite/network/utils.h>
#include <ignite/common/utils.h>

#include "network/linux_async_worker_thread.h"
#include "network/linux_async_client_pool.h"

namespace
{
    ignite::common::FibonacciSequence<10> fibonacci10;
}

namespace ignite
{
    namespace network
    {
        LinuxAsyncWorkerThread::LinuxAsyncWorkerThread(LinuxAsyncClientPool &clientPool) :
            clientPool(clientPool),
            stopping(true),
            epoll(-1),
            stopEvent(-1),
            nonConnected(),
            currentConnection(),
            currentClient(),
            failedAttempts(0),
            lastConnectionTime(),
            minAddrs(0)
        {
            memset(&lastConnectionTime, 0, sizeof(lastConnectionTime));
        }

        LinuxAsyncWorkerThread::~LinuxAsyncWorkerThread()
        {
            Stop();
        }

        void LinuxAsyncWorkerThread::Start0(size_t limit, const std::vector<TcpRange> &addrs)
        {
            epoll = epoll_create(1);
            if (epoll < 0)
                common::ThrowLastSystemError("Failed to create epoll instance");

            stopEvent = eventfd(0, EFD_NONBLOCK);
            if (stopEvent < 0)
            {
                std::string msg = common::GetLastSystemError("Failed to create stop event instance");
                close(stopEvent);
                common::ThrowSystemError(msg);
            }

            epoll_event event;
            memset(&event, 0, sizeof(event));

            event.events = EPOLLIN;

            int res = epoll_ctl(epoll, EPOLL_CTL_ADD, stopEvent, &event);
            if (res < 0)
            {
                std::string msg = common::GetLastSystemError("Failed to create stop event instance");
                close(stopEvent);
                close(epoll);
                common::ThrowSystemError(msg);
            }

            stopping = false;
            failedAttempts = 0;
            nonConnected = addrs;

            currentConnection.reset();
            currentClient = SP_LinuxAsyncClient();

            if (!limit || limit > addrs.size())
                minAddrs = 0;
            else
                minAddrs = addrs.size() - limit;

            Thread::Start();
        }

        void LinuxAsyncWorkerThread::Stop()
        {
            if (stopping)
                return;

            stopping = true;

            int64_t value = 1;
            ssize_t res = write(stopEvent, &value, sizeof(value));

            IGNITE_UNUSED(res);
            assert(res == sizeof(value));

            Thread::Join();

            close(stopEvent);
            close(epoll);

            nonConnected.clear();
            currentConnection.reset();
        }

        void LinuxAsyncWorkerThread::Run()
        {
            while (!stopping)
            {
                HandleNewConnections();

                if (stopping)
                    break;

                HandleConnectionEvents();
            }
        }

        void LinuxAsyncWorkerThread::HandleNewConnections()
        {
            if (!ShouldInitiateNewConnection())
                return;

            if (CalculateConnectionTimeout() > 0)
                return;

            addrinfo* addr = 0;
            if (currentConnection.get())
                addr = currentConnection->Next();

            if (!addr)
            {
                size_t idx = rand() % nonConnected.size();
                const TcpRange& range = nonConnected.at(idx);

                currentConnection.reset(new ConnectingContext(range));
                addr = currentConnection->Next();
                if (!addr)
                {
                    currentConnection.reset();
                    ReportConnectionError(EndPoint(), "Can not resolve a single address from range: " + range.ToString());
                    ++failedAttempts;

                    return;
                }
            }

            // Create a SOCKET for connecting to server
            int socketFd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
            if (SOCKET_ERROR == socketFd)
            {
                ReportConnectionError(currentConnection->GetAddress(),
                    "Socket creation failed: " + sockets::GetLastSocketErrorMessage());

                return;
            }

            sockets::TrySetSocketOptions(socketFd, LinuxAsyncClient::BUFFER_SIZE, true, true, true);
            bool success = sockets::SetNonBlockingMode(socketFd, true);
            if (!success)
            {
                ReportConnectionError(currentConnection->GetAddress(),
                    "Can not make non-blocking socket: " + sockets::GetLastSocketErrorMessage());

                return;
            }

            currentClient = currentConnection->ToClient(socketFd);
            bool ok = currentClient.Get()->StartMonitoring(epoll);
            if (!ok)
                common::ThrowLastSystemError("Can not add file descriptor to epoll");

            // Connect to server.
            int res = connect(socketFd, addr->ai_addr, addr->ai_addrlen);
            if (SOCKET_ERROR == res)
            {
                int lastError = errno;

                clock_gettime(CLOCK_MONOTONIC, &lastConnectionTime);

                if (lastError != EWOULDBLOCK && lastError != EINPROGRESS)
                {
                    HandleConnectionFailed("Failed to establish connection with the host: " +
                        sockets::GetSocketErrorMessage(lastError));

                    return;
                }
            }
        }

        void LinuxAsyncWorkerThread::HandleConnectionEvents()
        {
            enum { MAX_EVENTS = 16 };
            epoll_event events[MAX_EVENTS];

            int timeout = CalculateConnectionTimeout();

            int res = epoll_wait(epoll, events, MAX_EVENTS, timeout);

            if (res <= 0)
                return;

            for (int i = 0; i < res; ++i)
            {
                epoll_event& currentEvent = events[i];
                LinuxAsyncClient* client = static_cast<LinuxAsyncClient*>(currentEvent.data.ptr);
                if (!client)
                    continue;

                if (client == currentClient.Get())
                {
                    if (currentEvent.events & (EPOLLRDHUP | EPOLLERR))
                    {
                        HandleConnectionFailed("Can not establish connection");

                        continue;
                    }

                    HandleConnectionSuccess(client);
                }

                if (currentEvent.events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP))
                {
                    HandleConnectionClosed(client);

                    continue;
                }

                if (currentEvent.events & EPOLLIN)
                {
                    DataBuffer msg = client->Receive();
                    if (msg.IsEmpty())
                    {
                        HandleConnectionClosed(client);

                        continue;
                    }

                    clientPool.HandleMessageReceived(client->GetId(), msg);
                }

                if (currentEvent.events & EPOLLOUT)
                {
                    bool ok = client->ProcessSent();
                    if (!ok)
                    {
                        HandleConnectionClosed(client);

                        continue;
                    }
                }
            }
        }

        void LinuxAsyncWorkerThread::ReportConnectionError(const EndPoint& addr, const std::string& msg)
        {
            IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());
            clientPool.HandleConnectionError(addr, err);
        }

        void LinuxAsyncWorkerThread::HandleConnectionFailed(const std::string& msg)
        {
            LinuxAsyncClient* client = currentClient.Get();
            assert(client != 0);

            client->StopMonitoring();
            client->Close();

            ReportConnectionError(client->GetAddress(), msg);

            currentClient = SP_LinuxAsyncClient();
            ++failedAttempts;
        }

        void LinuxAsyncWorkerThread::HandleConnectionClosed(LinuxAsyncClient *client)
        {
            client->StopMonitoring();

            nonConnected.push_back(client->GetRange());

            clientPool.CloseAndRelease(client->GetId(), 0);
        }

        void LinuxAsyncWorkerThread::HandleConnectionSuccess(LinuxAsyncClient* client)
        {
            nonConnected.erase(std::find(nonConnected.begin(), nonConnected.end(), client->GetRange()));

            clientPool.AddClient(currentClient);

            currentClient = SP_LinuxAsyncClient();
            currentConnection.reset();

            failedAttempts = 0;

            clock_gettime(CLOCK_MONOTONIC, &lastConnectionTime);
        }

        int LinuxAsyncWorkerThread::CalculateConnectionTimeout() const
        {
            if (!ShouldInitiateNewConnection())
                return -1;

            if (lastConnectionTime.tv_sec == 0)
                return 0;

            int timeout = fibonacci10.GetValue(failedAttempts) * 1000;

            timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);

            int passed = (now.tv_sec - lastConnectionTime.tv_sec) * 1000 +
                         (now.tv_nsec - lastConnectionTime.tv_nsec) / 1000000;

            timeout -= passed;
            if (timeout < 0)
                timeout = 0;

            return timeout;
        }

        bool LinuxAsyncWorkerThread::ShouldInitiateNewConnection() const
        {
            return !currentClient.Get() && nonConnected.size() > minAddrs;
        }
    }
}
