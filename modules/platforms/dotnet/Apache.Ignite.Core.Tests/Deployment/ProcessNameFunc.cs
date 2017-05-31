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

namespace Apache.Ignite.Core.Tests.Deployment
{
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Function that returns parent process name.
    /// </summary>
    public class ProcessNameFunc : IComputeFunc<string>
    {
        /// <summary>
        /// Gets or sets the argument.
        /// </summary>
        public object Arg { get; set; }

        /** <inheritdoc /> */
        public string Invoke()
        {
            return System.Diagnostics.Process.GetCurrentProcess().ProcessName + Arg;
        }
    }

    /// <summary>
    /// Function that returns parent process name.
    /// </summary>
    public class ProcessNameArgFunc : IComputeFunc<object, string>
    {
        /** <inheritdoc /> */
        public string Invoke(object arg)
        {
            return System.Diagnostics.Process.GetCurrentProcess().ProcessName + arg;
        }
    }
}