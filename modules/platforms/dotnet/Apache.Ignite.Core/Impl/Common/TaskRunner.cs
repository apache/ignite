﻿/*
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