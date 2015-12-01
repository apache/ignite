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
