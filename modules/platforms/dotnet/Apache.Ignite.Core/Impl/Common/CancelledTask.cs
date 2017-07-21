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

namespace Apache.Ignite.Core.Impl.Common
{
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides cancelled tasks of given type.
    /// </summary>
    internal static class CancelledTask<T>
    {
        /** Task source. */
        private static readonly TaskCompletionSource<T> TaskCompletionSource;

        /// <summary>
        /// Initializes the <see cref="CancelledTask{T}"/> class.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline",
            Justification = "Readability.")]
        static CancelledTask()
        {
            TaskCompletionSource = new TaskCompletionSource<T>();
            TaskCompletionSource.SetCanceled();
        }

        /// <summary>
        /// Gets the cancelled task.
        /// </summary>
        public static Task<T> Instance
        {
            get { return TaskCompletionSource.Task; }
        }
    }
}
