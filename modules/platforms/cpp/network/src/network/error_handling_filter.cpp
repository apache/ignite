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

#include "network/error_handling_filter.h"

#define CLOSE_CONNECTION_ON_EXCEPTION(...)                                  \
    try                                                                     \
    {                                                                       \
        __VA_ARGS__;                                                        \
    }                                                                       \
    catch (const IgniteError& err)                                          \
    {                                                                       \
        DataFilterAdapter::Close(id, &err);                                 \
    }                                                                       \
    catch (std::exception& err)                                             \
    {                                                                       \
        std::string msg("Standard library exception is thrown: ");          \
        msg += err.what();                                                  \
        IgniteError err0(IgniteError::IGNITE_ERR_GENERIC, msg.c_str());     \
        DataFilterAdapter::Close(id, &err0);                                \
    }                                                                       \
    catch (...)                                                             \
    {                                                                       \
        IgniteError err0(IgniteError::IGNITE_ERR_UNKNOWN,                   \
            "Unknown error is encountered when processing network event");  \
        DataFilterAdapter::Close(id, &err0);                                \
    }

namespace ignite
{
    namespace network
    {
        void ErrorHandlingFilter::OnConnectionSuccess(const EndPoint &addr, uint64_t id)
        {
            CLOSE_CONNECTION_ON_EXCEPTION(DataFilterAdapter::OnConnectionSuccess(addr, id))
        }

        void ErrorHandlingFilter::OnConnectionError(const EndPoint &addr, const IgniteError &err)
        {
            try
            {
                DataFilterAdapter::OnConnectionError(addr, err);
            }
            catch (...)
            {
                // No-op.
            }
        }

        void ErrorHandlingFilter::OnConnectionClosed(uint64_t id, const IgniteError *err)
        {
            try
            {
                DataFilterAdapter::OnConnectionClosed(id, err);
            }
            catch (...)
            {
                // No-op.
            }
        }

        void ErrorHandlingFilter::OnMessageReceived(uint64_t id, const DataBuffer &data)
        {
            CLOSE_CONNECTION_ON_EXCEPTION(DataFilterAdapter::OnMessageReceived(id, data))
        }

        void ErrorHandlingFilter::OnMessageSent(uint64_t id)
        {
            CLOSE_CONNECTION_ON_EXCEPTION(DataFilterAdapter::OnMessageSent(id))
        }
    }
}
