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
