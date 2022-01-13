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

#ifndef _IGNITE_IMPL_THIN_NOTIFICATION_HANDLER
#define _IGNITE_IMPL_THIN_NOTIFICATION_HANDLER

#include <stdint.h>

#include <vector>

#include <ignite/ignite_error.h>
#include <ignite/common/thread_pool.h>
#include <ignite/network/data_buffer.h>

#include <ignite/impl/interop/interop_memory.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /** Notification handler. */
            class NotificationHandler
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~NotificationHandler()
                {
                    // No-op.
                }

                /**
                 * Handle notification.
                 *
                 * @param msg Message.
                 * @return @c true if processing complete.
                 */
                virtual void OnNotification(const network::DataBuffer& msg) = 0;
            };

            /** Shared pointer to notification handler. */
            typedef common::concurrent::SharedPointer<NotificationHandler> SP_NotificationHandler;

            /**
             * Task that handles notification
             */
            class HandleNotificationTask : public common::ThreadPoolTask
            {
            public:
                /**
                 * Constructor.
                 */
                HandleNotificationTask(const network::DataBuffer& msg, const SP_NotificationHandler& handler) :
                    msg(msg),
                    handler(handler)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~HandleNotificationTask()
                {
                    // No-op.
                }

                /**
                 * Execute task.
                 */
                virtual void Execute()
                {
                    handler.Get()->OnNotification(msg);
                }

            private:
                /** Message. */
                network::DataBuffer msg;

                /** Handler. */
                SP_NotificationHandler handler;
            };

            /** Notification handler. */
            class NotificationHandlerHolder
            {
                /** Message queue. */
                typedef std::vector<network::DataBuffer> MessageQueue;

            public:
                /**
                 * Default constructor.
                 */
                NotificationHandlerHolder() :
                    queue(),
                    handler()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~NotificationHandlerHolder()
                {
                    // No-op.
                }

                /**
                 * Process notification.
                 *
                 * @param msg Notification message to process.
                 * @return Task for dispatching if handler is present and null otherwise.
                 */
                common::SP_ThreadPoolTask ProcessNotification(const network::DataBuffer& msg)
                {
                    network::DataBuffer notification(msg.Clone());

                    if (handler.IsValid())
                        return common::SP_ThreadPoolTask(new HandleNotificationTask(notification, handler));
                    else
                    {
                        queue.push_back(notification);

                        return common::SP_ThreadPoolTask();
                    }
                }

                /**
                 * Set handler.
                 *
                 * @param handler Notification handler.
                 */
                void SetHandler(const SP_NotificationHandler& handler)
                {
                    if (this->handler.IsValid())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Internal error: handler is already set for the notification");

                    this->handler = handler;
                    for (MessageQueue::iterator it = queue.begin(); it != queue.end(); ++it)
                        this->handler.Get()->OnNotification(*it);

                    queue.clear();
                }

            private:
                /** Notification queue. */
                MessageQueue queue;

                /** Notification handler. */
                SP_NotificationHandler handler;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_NOTIFICATION_HANDLER
