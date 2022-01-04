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

        void LinuxAsyncWorkerThread::Start(size_t limit, const std::vector<TcpRange> &addrs)
        {
            epoll = epoll_create(1);
            if (epoll < 0)
                ThrowSystemError("Failed to create epoll instance");

            stopEvent = eventfd(0, EFD_NONBLOCK);
            if (stopEvent < 0)
            {
                close(stopEvent);
                ThrowSystemError("Failed to create stop event instance");
            }

            epoll_event event;
            memset(&event, 0, sizeof(event));

            event.events = EPOLLIN;

            int res = epoll_ctl(epoll, EPOLL_CTL_ADD, stopEvent, &event);
            if (res < 0)
            {
                close(stopEvent);
                close(epoll);
                ThrowSystemError("Failed to add stop event to epoll");
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
            write(stopEvent, &value, sizeof(value));

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

            size_t idx = rand() % nonConnected.size();
            const TcpRange& range = nonConnected.at(idx);

            currentConnection.reset(new ConnectingContext(range));

            addrinfo* addr = currentConnection->Next();
            if (!addr)
            {
                std::string msg = "Can not resolve a single address from range: " + range.ToString();
                IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());

                clientPool.HandleConnectionError(EndPoint(), err);
                currentConnection.reset();
                ++failedAttempts;

                return;
            }

            // Create a SOCKET for connecting to server
            int socketFd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
            if (SOCKET_ERROR == socketFd)
            {
                std::string msg = "Socket creation failed: " + sockets::GetLastSocketErrorMessage();
                IgniteError err(IgniteError::IGNITE_ERR_GENERIC, msg.c_str());
                EndPoint ep(currentConnection->GetAddress());

                clientPool.HandleConnectionError(ep, err);
                return;
            }

            sockets::TrySetSocketOptions(socketFd, LinuxAsyncClient::BUFFER_SIZE, true, true, true);
            bool success = sockets::SetNonBlockingMode(socketFd, true);
            if (!success)
            {
                std::string msg = "Can not make non-blocking socket: " + sockets::GetLastSocketErrorMessage();
                IgniteError err(IgniteError::IGNITE_ERR_GENERIC, msg.c_str());
                EndPoint ep(currentConnection->GetAddress());

                clientPool.HandleConnectionError(ep, err);
                ++failedAttempts;
                return;
            }

            currentClient = currentConnection->ToClient(socketFd);
            bool ok = currentClient.Get()->StartMonitoring(epoll);
            if (!ok)
                ThrowSystemError("Can not add file descriptor to epoll");

            // Connect to server.
            int res = connect(socketFd, addr->ai_addr, static_cast<int>(addr->ai_addrlen));
            if (SOCKET_ERROR == res)
            {
                currentClient.Get()->StopMonitoring();
                close(socketFd);
                int lastError = errno;

                if (lastError != EWOULDBLOCK && lastError != EINPROGRESS)
                {
                    std::string msg = "Failed to establish connection with the host: " +
                            sockets::GetSocketErrorMessage(lastError);
                    IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());

                    clientPool.HandleConnectionError(currentClient.Get()->GetAddress(), err);
                    ++failedAttempts;
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
                        HandleConnectionError(client);
                        continue;
                    }

                    nonConnected.erase(std::find(nonConnected.begin(), nonConnected.end(), client->GetRange()));

                    clientPool.AddClient(currentClient);

                    currentClient = SP_LinuxAsyncClient();
                    currentConnection.reset();

                    clock_gettime(CLOCK_MONOTONIC, &lastConnectionTime);

                    continue;
                }

                if (currentEvent.events & (EPOLLRDHUP | EPOLLERR))
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

        void LinuxAsyncWorkerThread::HandleConnectionError(LinuxAsyncClient* client)
        {
            client->StopMonitoring();

            IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Can not establsih connection");
            clientPool.HandleConnectionError(client->GetAddress(), err);

            client->Close();
        }

        void LinuxAsyncWorkerThread::HandleConnectionClosed(LinuxAsyncClient *client)
        {
            client->StopMonitoring();

            nonConnected.push_back(client->GetRange());

            IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed");
            clientPool.CloseAndRelease(client->GetId(), &err);

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
            return currentConnection.get() == 0 && nonConnected.size() > minAddrs;
        }

        void LinuxAsyncWorkerThread::ThrowSystemError(const std::string &msg)
        {
            // TODO: refactor
            std::stringstream buf;

            buf << "Linux system error: " << msg << ", system error code: " << errno;

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }
    }
}
