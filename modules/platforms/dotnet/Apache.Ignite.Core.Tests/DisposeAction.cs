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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Wraps an action to be executed on Dispose call.
    /// </summary>
    public class DisposeAction : IDisposable
    {
        /** */
        private readonly Action _action;

        /// <summary>
        /// Initializes a new instance of <see cref="DisposeAction"/>.
        /// </summary>
        /// <param name="action">Action.</param>
        public DisposeAction(Action action)
        {
            IgniteArgumentCheck.NotNull(action, "action");
            _action = action;
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            _action();
        }
    }
}
