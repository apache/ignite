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

namespace Apache.Ignite.Core.Impl.Client.Compute
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Client compute flags.
    /// </summary>
    [Flags]
    [SuppressMessage("Design", "CA1028:Enum Storage should be Int32", 
        Justification = "Dictated by thin client protocol.")]
    internal enum ComputeClientFlags : byte
    {
        None = 0,
        NoFailover = 1,
        NoResultCache = 2,
        KeepBinary = 4
    }
}