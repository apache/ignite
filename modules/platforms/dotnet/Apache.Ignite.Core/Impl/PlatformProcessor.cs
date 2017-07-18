/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"), you may not use this file except in compliance with
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

namespace Apache.Ignite.Core.Impl
{
    /// <summary>
    /// Platform processor, interop entry point, delegates to PlatformProcessorImpl in Java.
    /// </summary>
    internal class PlatformProcessor
    {
        /// <summary>
        /// Operation codes.
        /// </summary>
        private enum Op
        {
            GetCache = 1,
            CreateCache = 2,
            GetOrCreateCache = 3,
            CreateCacheFromConfig = 4,
            GetOrCreateCacheFromConfig = 5,
            DestroyCache = 6,
            GetAffinity = 7,
            GetDataStreamer = 8,
            GetTransactions = 9,
            GetClusterGroup = 10,
            GetCompute = 11,
            GetMessaging = 12,
            GetEvents = 13,
            GetServices = 14,
            GetExtension = 15,
            GetAtomicLong = 16,
            GetAtomicReference = 17,
            GetAtomicSequence = 18,
            GetIgniteConfiguration = 19,
            GetCacheNames = 20,
            CreateNearCache = 21,
            GetOrCreateNearCache = 22,
            LoggerIsLevelEnabled = 23,
            LoggerLog = 24,
            GetBinaryProcessor = 25,
        }
    }
}