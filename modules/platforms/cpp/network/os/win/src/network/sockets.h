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

#ifndef _IGNITE_NETWORK_SOCKETS
#define _IGNITE_NETWORK_SOCKETS

#define WIN32_LEAN_AND_MEAN
#define _WINSOCKAPI_

#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mstcpip.h>

#include <stdint.h>
#include <string>

namespace ignite
{
    namespace network
    {
        namespace sockets
        {
            /** Socket handle type. */
            typedef SOCKET SocketHandle;

            /**
             * Get socket error.
             * @return Last socket error.
             */
            int GetLastSocketError();

            /**
             * Get socket error.
             * @param handle Socket handle.
             * @return Last socket error.
             */
            int GetLastSocketError(int handle);

            /**
             * Get socket error message for the error code.
             * @param error Error code.
             * @return Socket error message string.
             */
            std::string GetSocketErrorMessage(HRESULT error);

            /**
             * Get last socket error message.
             * @return Last socket error message string.
             */
            std::string GetLastSocketErrorMessage();

            /**
             * Check whether socket operation was interupted.
             * @return @c true if the socket operation was interupted.
             */
            bool IsSocketOperationInterrupted(int errorCode);

            /**
             * Wait on the socket for any event for specified time.
             * This function uses poll to achive timeout functionality
             * for every separate socket operation.
             *
             * @param socket Socket handle.
             * @param timeout Timeout.
             * @param rd Wait for read if @c true, or for write if @c false.
             * @return -errno on error, WaitResult::TIMEOUT on timeout and
             *     WaitResult::SUCCESS on success.
             */
            int WaitOnSocket(SocketHandle socket, int32_t timeout, bool rd);

            /**
             * Try and set socket options.
             *
             * @param socket Socket.
             * @param bufSize Buffer size.
             * @param noDelay Set no-delay mode.
             * @param outOfBand Set out-of-Band mode.
             * @param keepAlive Keep alive mode.
             */
            void TrySetSocketOptions(SOCKET socket, int bufSize, BOOL noDelay, BOOL outOfBand, BOOL keepAlive);

            /**
             * Set non blocking mode for socket.
             *
             * @param socket Socket.
             * @param nonBlocking Non-blocking mode.
             */
            bool SetNonBlockingMode(SOCKET socket, bool nonBlocking);

            /**
             * Init windows sockets.
             *
             * Thread-safe.
             */
            void InitWsa();
        }
    }
}

#endif //_IGNITE_NETWORK_SOCKETS