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

namespace Apache.Ignite.Core.Compute
{
    /// <summary>
    /// Defines function having a single argument.
    /// </summary>
    public interface IComputeFunc<in TArg, out TRes>
    {
        /// <summary>
        /// Invoke function.
        /// </summary>
        /// <param name="arg">Argument.</param>
        /// <returns>Result.</returns>
        TRes Invoke(TArg arg);
    }

    /// <summary>
    /// Defines function having no arguments.
    /// </summary>
    public interface IComputeFunc<out TRes>
    {
        /// <summary>
        /// Invoke function.
        /// </summary>
        /// <returns>Result.</returns>
        TRes Invoke();
    }

    /// <summary>
    /// Defines a void function having no arguments.
    /// </summary>
    public interface IComputeAction
    {
        /// <summary>
        /// Invokes action.
        /// </summary>
        void Invoke();
    }
}
