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
        private static readonly IDictionary<string, ExceptionFactoryDelegate> EXS = new Dictionary<string, ExceptionFactoryDelegate>();

        /** Exception factory delegate. */
        private delegate Exception ExceptionFactoryDelegate(string msg, string javaStackTrace);
        
        /// <summary>
        /// Static initializer.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1810:InitializeReferenceTypeStaticFieldsInline", 
            Justification = "Readability")]
        static ExceptionUtils()
        {
            // Common Java exceptions mapped to common .Net exceptions.
            EXS["java.lang.IllegalArgumentException"] = (m, s) => new ArgumentException(m);
            EXS["java.lang.IllegalStateException"] = (m, s) => new InvalidOperationException(m);
            EXS["java.lang.UnsupportedOperationException"] = (m, s) => new NotImplementedException(m);
            EXS["java.lang.InterruptedException"] = (m, s) => new ThreadInterruptedException(m);
            
            // Generic Ignite exceptions.
            EXS["org.apache.ignite.IgniteException"] = (m, s) => new IgniteException(m, s);
            EXS["org.apache.ignite.IgniteCheckedException"] = (m, s) => new IgniteException(m, s);

            // Cluster exceptions.
            EXS["org.apache.ignite.cluster.ClusterGroupEmptyException"] = (m, s) => new ClusterGroupEmptyException(m, s);
            EXS["org.apache.ignite.cluster.ClusterTopologyException"] = (m, s) => new ClusterTopologyException(m, s);

            // Compute exceptions.
            EXS["org.apache.ignite.compute.ComputeExecutionRejectedException"] = (m, s) => new ComputeExecutionRejectedException(m, s);
            EXS["org.apache.ignite.compute.ComputeJobFailoverException"] = (m, s) => new ComputeJobFailoverException(m, s);
            EXS["org.apache.ignite.compute.ComputeTaskCancelledException"] = (m, s) => new ComputeTaskCancelledException(m, s);
            EXS["org.apache.ignite.compute.ComputeTaskTimeoutException"] = (m, s) => new ComputeTaskTimeoutException(m, s);
            EXS["org.apache.ignite.compute.ComputeUserUndeclaredException"] = (m, s) => new ComputeUserUndeclaredException(m, s);

            // Cache exceptions.
            EXS["javax.cache.CacheException"] = (m, s) => new CacheException(m);
            EXS["javax.cache.integration.CacheLoaderException"] = (m, s) => new CacheStoreException(m, s);
            EXS["javax.cache.integration.CacheWriterException"] = (m, s) => new CacheStoreException(m, s);
            EXS["javax.cache.processor.EntryProcessorException"] = (m, s) => new CacheEntryProcessorException(m, s);
            EXS["org.apache.ignite.cache.CacheAtomicUpdateTimeoutException"] = (m, s) => new CacheAtomicUpdateTimeoutException(m, s);
            
            // Transaction exceptions.
            EXS["org.apache.ignite.transactions.TransactionOptimisticException"] = (m, s) => new TransactionOptimisticException(m, s);
            EXS["org.apache.ignite.transactions.TransactionTimeoutException"] = (m, s) => new TransactionTimeoutException(m, s);
            EXS["org.apache.ignite.transactions.TransactionRollbackException"] = (m, s) => new TransactionRollbackException(m, s);
            EXS["org.apache.ignite.transactions.TransactionHeuristicException"] = (m, s) => new TransactionHeuristicException(m, s);

            // Security exceptions.
            EXS["org.apache.ignite.IgniteAuthenticationException"] = (m, s) => new SecurityException(m);
            EXS["org.apache.ignite.plugin.security.GridSecurityException"] = (m, s) => new SecurityException(m);
        }

        /// <summary>
        /// Creates exception according to native code class and message.
        /// </summary>
        /// <param name="clsName">Exception class name.</param>
        /// <param name="msg">Exception message.</param>
        /// <param name="stackTrace">Native stack trace.</param>
        /// <param name="reader">Error data reader.</param>
        /// <returns>Exception.</returns>
        public static Exception GetException(string clsName, string msg, string stackTrace, BinaryReader reader = null)
        {
            ExceptionFactoryDelegate ctor;

            if (EXS.TryGetValue(clsName, out ctor))
                return ctor(msg, stackTrace);

            if (ClsNoClsDefFoundErr.Equals(clsName))
                return new IgniteException("Java class is not found (did you set IGNITE_HOME environment " +
                    "variable?): " + msg);

            if (ClsNoSuchMthdErr.Equals(clsName))
                return new IgniteException("Java class method is not found (did you set IGNITE_HOME environment " +
                    "variable?): " + msg);

            if (ClsCachePartialUpdateErr.Equals(clsName))
                return ProcessCachePartialUpdateException(msg, stackTrace, reader);
            
            return new IgniteException("Java exception occurred [class=" + clsName + ", message=" + msg + ']');
        }

        /// <summary>
        /// Process cache partial update exception.
        /// </summary>
        /// <param name="msg">Message.</param>
        /// <param name="stackTrace">Stack trace.</param>
        /// <param name="reader">Reader.</param>
        /// <returns></returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static Exception ProcessCachePartialUpdateException(string msg, string stackTrace, BinaryReader reader)
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

            Exception innerErr = GetException(innerErrCls, innerErrMsg, stackTrace);

            return new CachePartialUpdateException(msg, innerErr);
        }

        /// <summary>
        /// Create JVM initialization exception.
        /// </summary>
        /// <param name="clsName">Class name.</param>
        /// <param name="msg">Message.</param>
        /// <param name="stackTrace">Stack trace.</param>
        /// <returns>Exception.</returns>
        public static Exception GetJvmInitializeException(string clsName, string msg, string stackTrace)
        {
            if (clsName != null)
                return new IgniteException("Failed to initialize JVM.", GetException(clsName, msg, stackTrace));

            if (msg != null)
                return new IgniteException("Failed to initialize JVM: " + msg + "\n" + stackTrace);

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
