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
#include <utility>

#include <ignite/ignite_error.h>
#include <ignite/common/utils.h>

using namespace ignite::common;
using namespace ignite::java;

namespace ignite
{
    void IgniteError::ThrowIfNeeded(const IgniteError& err)
    {
        if (err.code != IGNITE_SUCCESS)
            throw err;
    }

    IgniteError::IgniteError() :
        code(IGNITE_SUCCESS),
        msg(NULL)
    {
        // No-op.
    }

    IgniteError::IgniteError(int32_t code) :
        code(code),
        msg(NULL)
    {
    }

    IgniteError::IgniteError(int32_t code, const char* msg) :
        code(code),
        msg(CopyChars(msg))
    {
        // No-op.
    }

    IgniteError::IgniteError(const IgniteError& other) :
        code(other.code),
        msg(CopyChars(other.msg))
    {
        // No-op.
    }

    IgniteError& IgniteError::operator=(const IgniteError& other)
    {
        if (this != &other)
        {
            IgniteError tmp(other);

            std::swap(code, tmp.code);
            std::swap(msg, tmp.msg);
        }

        return *this;
    }

    IgniteError::~IgniteError() IGNITE_NO_THROW
    {
        ReleaseChars(msg);
    }

    int32_t IgniteError::GetCode() const
    {
        return code;
    }

    const char* IgniteError::GetText() const IGNITE_NO_THROW
    {
        if (code == IGNITE_SUCCESS)
            return "Operation completed successfully.";
        else if (msg)
            return msg;
        else
            return  "No additional information available.";
    }

    const char* IgniteError::what() const IGNITE_NO_THROW
    {
        return GetText();
    }

    void IgniteError::SetError(const int jniCode, const char* jniCls, const char* jniMsg, IgniteError& err)
    {
        if (jniCode == IGNITE_JNI_ERR_SUCCESS)
            err = IgniteError();
        else if (jniCode == IGNITE_JNI_ERR_GENERIC)
        {
            // The most common case when we have Java exception "in hands" and must map it to respective code.
            if (jniCls)
            {
                std::string jniCls0 = jniCls;

                if (jniCls0.compare("java.lang.NoClassDefFoundError") == 0)
                {
                    std::stringstream stream;

                    stream << "Java class is not found (did you set IGNITE_HOME environment variable?)";

                    if (jniMsg)
                        stream << ": " << jniMsg;

                    err = IgniteError(IGNITE_ERR_JVM_NO_CLASS_DEF_FOUND, stream.str().c_str());
                }
                else if (jniCls0.compare("java.lang.NoSuchMethodError") == 0)
                {
                    std::stringstream stream;

                    stream << "Java method is not found (did you set IGNITE_HOME environment variable?)";

                    if (jniMsg)
                        stream << ": " << jniMsg;

                    err = IgniteError(IGNITE_ERR_JVM_NO_SUCH_METHOD, stream.str().c_str());
                }
                else if (jniCls0.compare("java.lang.IllegalArgumentException") == 0)
                    err = IgniteError(IGNITE_ERR_ILLEGAL_ARGUMENT, jniMsg);
                else if (jniCls0.compare("java.lang.IllegalStateException") == 0)
                    err = IgniteError(IGNITE_ERR_ILLEGAL_STATE, jniMsg);
                else if (jniCls0.compare("java.lang.UnsupportedOperationException") == 0)
                    err = IgniteError(IGNITE_ERR_UNSUPPORTED_OPERATION, jniMsg);
                else if (jniCls0.compare("java.lang.InterruptedException") == 0)
                    err = IgniteError(IGNITE_ERR_INTERRUPTED, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.cluster.ClusterGroupEmptyException") == 0)
                    err = IgniteError(IGNITE_ERR_CLUSTER_GROUP_EMPTY, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.cluster.ClusterTopologyException") == 0)
                    err = IgniteError(IGNITE_ERR_CLUSTER_TOPOLOGY, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.compute.ComputeExecutionRejectedException") == 0)
                    err = IgniteError(IGNITE_ERR_COMPUTE_EXECUTION_REJECTED, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.compute.ComputeJobFailoverException") == 0)
                    err = IgniteError(IGNITE_ERR_COMPUTE_JOB_FAILOVER, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.compute.ComputeTaskCancelledException") == 0)
                    err = IgniteError(IGNITE_ERR_COMPUTE_TASK_CANCELLED, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.compute.ComputeTaskTimeoutException") == 0)
                    err = IgniteError(IGNITE_ERR_COMPUTE_TASK_TIMEOUT, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.compute.ComputeUserUndeclaredException") == 0)
                    err = IgniteError(IGNITE_ERR_COMPUTE_USER_UNDECLARED_EXCEPTION, jniMsg);
                else if (jniCls0.compare("javax.cache.CacheException") == 0)
                    err = IgniteError(IGNITE_ERR_CACHE, jniMsg);
                else if (jniCls0.compare("javax.cache.integration.CacheLoaderException") == 0)
                    err = IgniteError(IGNITE_ERR_CACHE_LOADER, jniMsg);
                else if (jniCls0.compare("javax.cache.integration.CacheWriterException") == 0)
                    err = IgniteError(IGNITE_ERR_CACHE_WRITER, jniMsg);
                else if (jniCls0.compare("javax.cache.processor.EntryProcessorException") == 0)
                    err = IgniteError(IGNITE_ERR_ENTRY_PROCESSOR, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.cache.CachePartialUpdateException") == 0)
                    err = IgniteError(IGNITE_ERR_CACHE_PARTIAL_UPDATE, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.transactions.TransactionOptimisticException") == 0)
                    err = IgniteError(IGNITE_ERR_TX_OPTIMISTIC, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.transactions.TransactionTimeoutException") == 0)
                    err = IgniteError(IGNITE_ERR_TX_TIMEOUT, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.transactions.TransactionRollbackException") == 0)
                    err = IgniteError(IGNITE_ERR_TX_ROLLBACK, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.transactions.TransactionHeuristicException") == 0)
                    err = IgniteError(IGNITE_ERR_TX_HEURISTIC, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.IgniteAuthenticationException") == 0)
                    err = IgniteError(IGNITE_ERR_AUTHENTICATION, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.plugin.security.GridSecurityException") == 0)
                    err = IgniteError(IGNITE_ERR_SECURITY, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.IgniteException") == 0)
                    err = IgniteError(IGNITE_ERR_GENERIC, jniMsg);
                else if (jniCls0.compare("org.apache.ignite.IgniteCheckedException") == 0)
                    err = IgniteError(IGNITE_ERR_GENERIC, jniMsg);
                else
                {
                    std::stringstream stream;

                    stream << "Java exception occurred [cls=" << jniCls0;

                    if (jniMsg)
                        stream << ", msg=" << jniMsg;

                    stream << "]";

                    err = IgniteError(IGNITE_ERR_UNKNOWN, stream.str().c_str());
                }
            }
            else
            {
                // JNI class name is not available. Something really weird.
                err = IgniteError(IGNITE_ERR_UNKNOWN);
            }
        }
        else if (jniCode == IGNITE_JNI_ERR_JVM_INIT)
        {
            std::stringstream stream;

            stream << "Failed to initialize JVM [errCls=";

            if (jniCls)
                stream << jniCls;
            else
                stream << "N/A";

            stream << ", errMsg=";

            if (jniMsg)
                stream << jniMsg;
            else
                stream << "N/A";

            stream << "]";

            err = IgniteError(IGNITE_ERR_JVM_INIT, stream.str().c_str());
        }
        else if (jniCode == IGNITE_JNI_ERR_JVM_ATTACH)
            err = IgniteError(IGNITE_ERR_JVM_ATTACH, "Failed to attach to JVM.");
    }
}
