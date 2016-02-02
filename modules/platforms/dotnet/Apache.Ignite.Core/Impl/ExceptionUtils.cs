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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Security;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Managed environment. Acts as a gateway for native code.
    /// </summary>
    internal static class ExceptionUtils
    {
        /** NoClassDefFoundError fully-qualified class name which is important during startup phase. */
        private const string ClsNoClsDefFoundErr = "java.lang.NoClassDefFoundError";

        /** NoSuchMethodError fully-qualified class name which is important during startup phase. */
        private const string ClsNoSuchMthdErr = "java.lang.NoSuchMethodError";

        /** InteropCachePartialUpdateException. */
        private const string ClsCachePartialUpdateErr = "org.apache.ignite.internal.processors.platform.cache.PlatformCachePartialUpdateException";
        
        /** Map with predefined exceptions. */
        private static readonly IDictionary<string, ExceptionFactoryDelegate> Exs = new Dictionary<string, ExceptionFactoryDelegate>();

        /** Exception factory delegate. */
        private delegate Exception ExceptionFactoryDelegate(string msg);
        
        /// <summary>
        /// Static initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", 
            Justification = "Readability")]
        static ExceptionUtils()
        {
            // Common Java exceptions mapped to common .Net exceptions.
            Exs["java.lang.IllegalArgumentException"] = m => new ArgumentException(m);
            Exs["java.lang.IllegalStateException"] = m => new InvalidOperationException(m);
            Exs["java.lang.UnsupportedOperationException"] = m => new NotImplementedException(m);
            Exs["java.lang.InterruptedException"] = m => new ThreadInterruptedException(m);
            
            // Generic Ignite exceptions.
            Exs["org.apache.ignite.IgniteException"] = m => new IgniteException(m);
            Exs["org.apache.ignite.IgniteCheckedException"] = m => new IgniteException(m);

            // Cluster exceptions.
            Exs["org.apache.ignite.cluster.ClusterGroupEmptyException"] = m => new ClusterGroupEmptyException(m);
            Exs["org.apache.ignite.cluster.ClusterTopologyException"] = m => new ClusterTopologyException(m);

            // Compute exceptions.
            Exs["org.apache.ignite.compute.ComputeExecutionRejectedException"] = m => new ComputeExecutionRejectedException(m);
            Exs["org.apache.ignite.compute.ComputeJobFailoverException"] = m => new ComputeJobFailoverException(m);
            Exs["org.apache.ignite.compute.ComputeTaskCancelledException"] = m => new ComputeTaskCancelledException(m);
            Exs["org.apache.ignite.compute.ComputeTaskTimeoutException"] = m => new ComputeTaskTimeoutException(m);
            Exs["org.apache.ignite.compute.ComputeUserUndeclaredException"] = m => new ComputeUserUndeclaredException(m);

            // Cache exceptions.
            Exs["javax.cache.CacheException"] = m => new CacheException(m);
            Exs["javax.cache.integration.CacheLoaderException"] = m => new CacheStoreException(m);
            Exs["javax.cache.integration.CacheWriterException"] = m => new CacheStoreException(m);
            Exs["javax.cache.processor.EntryProcessorException"] = m => new CacheEntryProcessorException(m);
            Exs["org.apache.ignite.cache.CacheAtomicUpdateTimeoutException"] = m => new CacheAtomicUpdateTimeoutException(m);
            
            // Transaction exceptions.
            Exs["org.apache.ignite.transactions.TransactionOptimisticException"] = m => new TransactionOptimisticException(m);
            Exs["org.apache.ignite.transactions.TransactionTimeoutException"] = m => new TransactionTimeoutException(m);
            Exs["org.apache.ignite.transactions.TransactionRollbackException"] = m => new TransactionRollbackException(m);
            Exs["org.apache.ignite.transactions.TransactionHeuristicException"] = m => new TransactionHeuristicException(m);

            // Security exceptions.
            Exs["org.apache.ignite.IgniteAuthenticationException"] = m => new SecurityException(m);
            Exs["org.apache.ignite.plugin.security.GridSecurityException"] = m => new SecurityException(m);

            // Future exceptions
            Exs["org.apache.ignite.lang.IgniteFutureCancelledException"] = m => new IgniteFutureCancelledException(m);
            Exs["org.apache.ignite.internal.IgniteFutureCancelledCheckedException"] = m => new IgniteFutureCancelledException(m);
        }

        /// <summary>
        /// Creates exception according to native code class and message.
        /// </summary>
        /// <param name="clsName">Exception class name.</param>
        /// <param name="msg">Exception message.</param>
        /// <param name="reader">Error data reader.</param>
        public static Exception GetException(string clsName, string msg, BinaryReader reader = null)
        {
            ExceptionFactoryDelegate ctor;

            if (Exs.TryGetValue(clsName, out ctor))
                return ctor(msg);

            if (ClsNoClsDefFoundErr.Equals(clsName))
                return new IgniteException("Java class is not found (did you set IGNITE_HOME environment " +
                    "variable?): " + msg);

            if (ClsNoSuchMthdErr.Equals(clsName))
                return new IgniteException("Java class method is not found (did you set IGNITE_HOME environment " +
                    "variable?): " + msg);

            if (ClsCachePartialUpdateErr.Equals(clsName))
                return ProcessCachePartialUpdateException(msg, reader);
            
            return new IgniteException("Java exception occurred [class=" + clsName + ", message=" + msg + ']');
        }

        /// <summary>
        /// Process cache partial update exception.
        /// </summary>
        /// <param name="msg">Message.</param>
        /// <param name="reader">Reader.</param>
        /// <returns></returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static Exception ProcessCachePartialUpdateException(string msg, BinaryReader reader)
        {
            if (reader == null)
                return new CachePartialUpdateException(msg, new IgniteException("Failed keys are not available."));
            
            bool dataExists = reader.ReadBoolean();

            Debug.Assert(dataExists);

            if (reader.ReadBoolean())
            {
                bool keepBinary = reader.ReadBoolean();

                BinaryReader keysReader = reader.Marshaller.StartUnmarshal(reader.Stream, keepBinary);

                try
                {
                    return new CachePartialUpdateException(msg, ReadNullableList(keysReader));
                }
                catch (Exception e)
                {
                    // Failed to deserialize data.
                    return new CachePartialUpdateException(msg, e);
                }
            }
            
            // Was not able to write keys.
            string innerErrCls = reader.ReadString();
            string innerErrMsg = reader.ReadString();

            Exception innerErr = GetException(innerErrCls, innerErrMsg);

            return new CachePartialUpdateException(msg, innerErr);
        }

        /// <summary>
        /// Create JVM initialization exception.
        /// </summary>
        /// <param name="clsName">Class name.</param>
        /// <param name="msg">Message.</param>
        /// <returns>Exception.</returns>
        public static Exception GetJvmInitializeException(string clsName, string msg)
        {
            if (clsName != null)
                return new IgniteException("Failed to initialize JVM.", GetException(clsName, msg));

            if (msg != null)
                return new IgniteException("Failed to initialize JVM: " + msg);

            return new IgniteException("Failed to initialize JVM.");
        }

        /// <summary>
        /// Reads nullable list.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>List.</returns>
        private static List<object> ReadNullableList(BinaryReader reader)
        {
            if (!reader.ReadBoolean()) 
                return null;

            var size = reader.ReadInt();

            var list = new List<object>(size);

            for (int i = 0; i < size; i++)
                list.Add(reader.ReadObject<object>());

            return list;
        }
    }
}
