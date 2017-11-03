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

namespace Apache.Ignite.Core.Impl.Cache.Expiry
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Writer for <see cref="IExpiryPolicy"/>.
    /// </summary>
    internal static class ExpiryPolicySerializer
    {
        /** Duration: unchanged. */
        private const long DurUnchanged = -2;

        /** Duration: eternal. */
        private const long DurEternal = -1;

        /** Duration: zero. */
        private const long DurZero = 0;

        /// <summary>
        /// Writes the policy.
        /// </summary>
        public static void WritePolicy(IBinaryRawWriter writer, IExpiryPolicy plc)
        {
            Debug.Assert(plc != null);
            Debug.Assert(writer != null);

            writer.WriteLong(ConvertDuration(plc.GetExpiryForCreate()));
            writer.WriteLong(ConvertDuration(plc.GetExpiryForUpdate()));
            writer.WriteLong(ConvertDuration(plc.GetExpiryForAccess()));
        }

        /// <summary>
        /// Reads the policy.
        /// </summary>
        public static IExpiryPolicy ReadPolicy(IBinaryRawReader reader)
        {
            return new ExpiryPolicy(ConvertDuration(reader.ReadLong()), ConvertDuration(reader.ReadLong()),
                ConvertDuration(reader.ReadLong()));
        }

        /// <summary>
        /// Writes the policy factory.
        /// </summary>
        public static void WritePolicyFactory(IBinaryRawWriter writer, IFactory<IExpiryPolicy> factory)
        {
            Debug.Assert(writer != null);

            if (factory != null)
            {
                writer.WriteBoolean(true);
                var expiryPolicy = factory.CreateInstance();

                if (expiryPolicy == null)
                    throw new IgniteException("ExpiryPolicyFactory should return non-null result.");

                WritePolicy(writer, expiryPolicy);
            }
            else
                writer.WriteBoolean(false);
        }

        /// <summary>
        /// Reads the expiry policy factory.
        /// </summary>
        public static IFactory<IExpiryPolicy> ReadPolicyFactory(IBinaryRawReader reader)
        {
            return reader.ReadBoolean() ? new ExpiryPolicyFactory(ReadPolicy(reader)) : null;
        }

        /// <summary>
        /// Convert TimeSpan to duration recognizable by Java.
        /// </summary>
        /// <param name="dur">.NET duration.</param>
        /// <returns>Java duration in milliseconds.</returns>
        private static long ConvertDuration(TimeSpan? dur)
        {
            if (dur.HasValue)
            {
                if (dur.Value == TimeSpan.MaxValue)
                    return DurEternal;

                long dur0 = (long) dur.Value.TotalMilliseconds;

                return dur0 > 0 ? dur0 : DurZero;
            }

            return DurUnchanged;
        }

        /// <summary>
        /// Convert duration recognizable by Java to TimeSpan.
        /// </summary>
        /// <param name="dur">Java duration.</param>
        /// <returns>.NET duration.</returns>
        private static TimeSpan? ConvertDuration(long dur)
        {
            switch (dur)
            {
                case DurUnchanged:
                    return null;

                case DurEternal:
                    return TimeSpan.MaxValue;

                case DurZero:
                    return TimeSpan.Zero;

                default:
                    return TimeSpan.FromMilliseconds(dur);
            }
        }
    }
}
