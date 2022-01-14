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

/**
 * @file
 * Declares ignite::common::ThreadPool class.
 */

#ifndef _IGNITE_COMMON_THREAD_POOL
#define _IGNITE_COMMON_THREAD_POOL

#include <deque>
#include <vector>

#include <ignite/ignite_error.h>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace common
    {
        /**
         * Thread pool task.
         */
        class ThreadPoolTask
        {
        public:
            /**
             * Destructor.
             */
            virtual ~ThreadPoolTask()
            {
                // No-op.
            }

            /**
             * Execute task.
             */
            virtual void Execute() = 0;

            /**
             * Called if error occurred during task processing.
             *
             * @param err Error.
             */
             virtual void OnError(const IgniteError& err) = 0;
        };

        /** Shared pointer to thread pool task. */
        typedef concurrent::SharedPointer< ThreadPoolTask > SP_ThreadPoolTask;

        /**
         * Thread Pool.
         */
        class ThreadPool
        {
        public:
            /**
             * Constructor.
             *
             * @param threadsNum Number of threads. If set to 0 current number of processors is used.
             */
            IGNITE_IMPORT_EXPORT explicit ThreadPool(uint32_t threadsNum);

            /**
             * Destructor.
             */
            IGNITE_IMPORT_EXPORT virtual ~ThreadPool();

            /**
             * Start threads in pool.
             */
            IGNITE_IMPORT_EXPORT void Start();

            /**
             * Stop threads in pool.
             *
             * @warning Once stopped it can not be restarted.
             */
            IGNITE_IMPORT_EXPORT void Stop();

            /**
             * Dispatch task.
             *
             * @param task Task.
             */
            IGNITE_IMPORT_EXPORT void Dispatch(const SP_ThreadPoolTask& task);

        private:
            IGNITE_NO_COPY_ASSIGNMENT(ThreadPool);

            /**
             * Task queue.
             */
            class TaskQueue
            {
            public:
                /**
                 * Constructor.
                 */
                TaskQueue();

                /**
                 * Destructor.
                 */
                ~TaskQueue();

                /**
                 * Push a new task to the queue.
                 *
                 * @param task Task. Should not be null.
                 */
                void Push(const SP_ThreadPoolTask& task);

                /**
                 * Pop a task from the queue.
                 *
                 * @return New task or null when unblocked.
                 */
                SP_ThreadPoolTask Pop();

                /**
                 * Unblock queue. When unblocked queue will not block or return new tasks.
                 */
                void Unblock();

            private:
                /** If true queue will not block. */
                volatile bool unblocked;

                /** Tasks queue. */
                std::deque< SP_ThreadPoolTask > tasks;

                /** Critical section. */
                concurrent::CriticalSection mutex;

                /** Condition variable. */
                concurrent::ConditionVariable waitPoint;
            };

            /**
             * Worker thread.
             */
            class WorkerThread : public concurrent::Thread
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param taskQueue Task queue.
                 */
                explicit WorkerThread(TaskQueue& taskQueue);

                /**
                 * Destructor.
                 */
                ~WorkerThread();

            private:
                /**
                 * Run thread.
                 */
                virtual void Run();

                /** Task queue. */
                TaskQueue& taskQueue;
            };

            /**
             * Handle an error that occurred during task execution.
             *
             * @param task Task.
             * @param err Error.
             */
            static void HandleTaskError(ThreadPoolTask &task, const IgniteError &err);

            /** Shared pointer to thread pool worker thread. */
            typedef concurrent::SharedPointer< WorkerThread > SP_WorkerThread;

            /** Started flag. */
            bool started;

            /** Stopped flag. */
            bool stopped;

            /** Task queue. */
            TaskQueue queue;

            /** Worker Threads. */
            std::vector<SP_WorkerThread> threads;

            /** Critical section. */
            concurrent::CriticalSection mutex;
        };
    }
}

#endif //_IGNITE_COMMON_THREAD_POOL
