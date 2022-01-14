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

#include <exception>
#include <iostream>

#include <ignite/common/thread_pool.h>

namespace ignite
{
    namespace common
    {
        ThreadPool::ThreadPool(uint32_t threadsNum):
            started(false),
            stopped(false),
            threads()
        {
            uint32_t threadToStart = threadsNum != 0 ? threadsNum : concurrent::GetNumberOfProcessors();
            if (!threadToStart)
                threadToStart = 2;

            threads.reserve(threadToStart);
            for (uint32_t i = 0; i < threadToStart; ++i)
            {
                SP_WorkerThread thread(new WorkerThread(queue));
                threads.push_back(thread);
            }
        }

        ThreadPool::~ThreadPool()
        {
            Stop();
        }

        void ThreadPool::Start()
        {
            concurrent::CsLockGuard guard(mutex);

            if (started)
                return;

            started = true;

            for (std::vector<SP_WorkerThread>::iterator it = threads.begin(); it != threads.end(); ++it)
                it->Get()->Start();
        }

        void ThreadPool::Stop()
        {
            concurrent::CsLockGuard guard(mutex);

            if (stopped || !started)
                return;

            stopped = true;

            queue.Unblock();

            for (std::vector<SP_WorkerThread>::iterator it = threads.begin(); it != threads.end(); ++it)
                it->Get()->Join();
        }

        void ThreadPool::Dispatch(const SP_ThreadPoolTask& task)
        {
            queue.Push(task);
        }

        ThreadPool::TaskQueue::TaskQueue() :
            unblocked(false)
        {
            // No-op.
        }

        ThreadPool::TaskQueue::~TaskQueue()
        {
            // No-op.
        }

        void ThreadPool::TaskQueue::Push(const SP_ThreadPoolTask &task)
        {
            if (!task.IsValid())
                return;

            concurrent::CsLockGuard guard(mutex);

            if (unblocked)
            {
                IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Execution thread pool is stopped");
                HandleTaskError(*task.Get(), err);

                return;
            }

            tasks.push_back(task);

            waitPoint.NotifyOne();
        }

        SP_ThreadPoolTask ThreadPool::TaskQueue::Pop()
        {
            concurrent::CsLockGuard guard(mutex);
            if (unblocked)
                return SP_ThreadPoolTask();

            while (tasks.empty())
            {
                waitPoint.Wait(mutex);

                if (unblocked)
                    return SP_ThreadPoolTask();
            }

            SP_ThreadPoolTask res = tasks.front();
            tasks.pop_front();
            return res;
        }

        void ThreadPool::TaskQueue::Unblock()
        {
            concurrent::CsLockGuard guard(mutex);
            unblocked = true;

            IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Execution thread pool is stopped");
            for (std::deque< SP_ThreadPoolTask >::iterator it = tasks.begin(); it != tasks.end(); ++it)
                HandleTaskError(*it->Get(), err);

            tasks.clear();

            waitPoint.NotifyAll();
        }

        ThreadPool::WorkerThread::WorkerThread(TaskQueue& taskQueue) :
            taskQueue(taskQueue)
        {
            // No-op.
        }

        ThreadPool::WorkerThread::~WorkerThread()
        {
            // No-op.
        }

        void ThreadPool::WorkerThread::Run()
        {
            while (true)
            {
                SP_ThreadPoolTask task = taskQueue.Pop();

                // Queue is unblocked and workers should stop.
                if (!task.IsValid())
                    break;

                ThreadPoolTask &task0 = *task.Get();

                try
                {
                    task0.Execute();
                }
                catch (const IgniteError& err)
                {
                    HandleTaskError(task0, err);
                }
                catch (const std::exception& err)
                {
                    IgniteError err0(IgniteError::IGNITE_ERR_STD, err.what());
                    HandleTaskError(task0, err0);
                }
                catch (...)
                {
                    IgniteError err(IgniteError::IGNITE_ERR_UNKNOWN, "Unknown error occurred when executing task");
                    HandleTaskError(task0, err);
                }
            }
        }

        void ThreadPool::HandleTaskError(ThreadPoolTask &task, const IgniteError &err)
        {
            try
            {
                task.OnError(err);

                return;
            }
            catch (const IgniteError& err0)
            {
                std::cerr << "Exception is thrown during handling of exception: "
                          << err0.what() << "Aborting execution" << std::endl;
            }
            catch (const std::exception& err0)
            {
                std::cerr << "Exception is thrown during handling of exception: "
                          << err0.what() << "Aborting execution" << std::endl;
            }
            catch (...)
            {
                std::cerr << "Unknown exception is thrown during handling of exception. Aborting execution" << std::endl;
            }

            std::terminate();
        }
    }
}
