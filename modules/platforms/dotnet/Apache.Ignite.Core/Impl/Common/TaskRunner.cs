/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extensions for <see cref="Task"/> classes.
    /// Fixes the issue with <see cref="TaskScheduler.Current"/> being used by defaut by system APIs.
    /// </summary>
    internal static class TaskRunner
    {
        /// <summary>
        /// ContinueWith using default scheduler.
        /// </summary>
        public static Task<TNewResult> ContWith<TResult, TNewResult>(this Task<TResult> task,
            Func<Task<TResult>, TNewResult> continuationFunction)
        {
            IgniteArgumentCheck.NotNull(task, "task");
            
            return task.ContinueWith(continuationFunction, TaskScheduler.Default);
        }
        
        /// <summary>
        /// ContinueWith using default scheduler.
        /// </summary>
        public static Task ContWith(this Task task,
            Action<Task> continuationFunction)
        {
            IgniteArgumentCheck.NotNull(task, "task");
            
            return task.ContinueWith(continuationFunction, TaskScheduler.Default);
        }

        /// <summary>
        /// Run new task using default scheduler.
        /// </summary>
        public static Task Run(Action action)
        {
            return Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.None, 
                TaskScheduler.Default);
        }
        
        /// <summary>
        /// Run new task using default scheduler.
        /// </summary>
        public static Task<TResult> Run<TResult>(Func<TResult> func)
        {
            return Task.Factory.StartNew(func, CancellationToken.None, TaskCreationOptions.None, 
                TaskScheduler.Default);
        }
    }
}