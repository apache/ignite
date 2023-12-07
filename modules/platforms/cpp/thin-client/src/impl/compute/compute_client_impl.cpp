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

#include <ignite/common/promise.h>

#include "impl/compute/compute_client_impl.h"
#include "impl/message.h"

using namespace ignite::common::concurrent;

namespace
{
    using namespace ignite;
    using namespace impl;
    using namespace impl::thin;

    /**
     * Handler for java task notification.
     */
    class JavaTaskNotificationHandler : public NotificationHandler
    {
    public:
        /**
         * Constructor.
         * @param channel Channel.
         * @param res Result.
         */
        JavaTaskNotificationHandler(DataChannel& channel, Readable& res) :
            channel(channel),
            res(res)
        {
            // No-op.
        }

        virtual ~JavaTaskNotificationHandler()
        {
            // No-op.
        }

        virtual void OnNotification(const network::DataBuffer& msg)
        {
            ComputeTaskFinishedNotification notification(res);
            channel.DeserializeMessage(msg, notification);

            if (notification.IsFailure())
            {
                promise.SetError(IgniteError(IgniteError::IGNITE_ERR_COMPUTE_EXECUTION_REJECTED,
                    notification.GetError().c_str()));
            }
            else
                promise.SetValue();
        }

        virtual void OnDisconnected()
        {
            promise.SetError(IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed"));
        }

        /**
         * Get future result.
         *
         * @return Future.
         */
        ignite::Future<void> GetFuture() const
        {
            return promise.GetFuture();
        }

    private:
        /** Channel. */
        DataChannel& channel;

        /** Result. */
        Readable& res;

        /** Completion promise. */
        ignite::common::Promise<void> promise;
    };
}

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace compute
            {
                void ComputeClientImpl::ExecuteJavaTask(int8_t flags, int64_t timeout, const std::string& taskName,
                    Writable& wrArg, Readable& res)
                {
                    ComputeTaskExecuteRequest req(flags, timeout, taskName, wrArg);
                    ComputeTaskExecuteResponse rsp;

                    SP_DataChannel channel = router.Get()->SyncMessage(req, rsp);

                    common::concurrent::SharedPointer<JavaTaskNotificationHandler> handler(
                        new JavaTaskNotificationHandler(*channel.Get(), res));

                    channel.Get()->RegisterNotificationHandler(rsp.GetNotificationId(), handler);

                    handler.Get()->GetFuture().GetValue();

                    channel.Get()->DeregisterNotificationHandler(rsp.GetNotificationId());
                }
            }
        }
    }
}
