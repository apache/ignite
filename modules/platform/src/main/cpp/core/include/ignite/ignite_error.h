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

#ifndef _IGNITE_ERROR
#define _IGNITE_ERROR

#include <sstream>
#include <stdint.h>

#include <ignite/common/common.h>

#define IGNITE_ERROR_1(code, part1) { \
    std::stringstream stream; \
    stream << (part1); \
    throw ignite::IgniteError(code, stream.str().c_str()); \
}

#define IGNITE_ERROR_2(code, part1, part2) { \
    std::stringstream stream; \
    stream << (part1) << (part2); \
    throw ignite::IgniteError(code, stream.str().c_str()); \
}

#define IGNITE_ERROR_3(code, part1, part2, part3) { \
    std::stringstream stream; \
    stream << (part1) << (part2) << (part3); \
    throw ignite::IgniteError(code, stream.str().c_str()); \
}

#define IGNITE_ERROR_FORMATTED_1(code, msg, key1, val1) { \
    std::stringstream stream; \
    stream << msg << " [" << key1 << "=" << (val1) << "]"; \
    throw ignite::IgniteError(code, stream.str().c_str()); \
}

#define IGNITE_ERROR_FORMATTED_2(code, msg, key1, val1, key2, val2) { \
    std::stringstream stream; \
    stream << msg << " [" << key1 << "=" << (val1) << ", " << key2 << "=" << (val2) << "]"; \
    throw ignite::IgniteError(code, stream.str().c_str()); \
}

#define IGNITE_ERROR_FORMATTED_3(code, msg, key1, val1, key2, val2, key3, val3) { \
    std::stringstream stream; \
    stream << msg << " [" << key1 << "=" << (val1) << ", " << key2 << "=" << (val2) << ", " << key3 << "=" << (val3) << "]"; \
    throw ignite::IgniteError(code, stream.str().c_str()); \
}

#define IGNITE_ERROR_FORMATTED_4(code, msg, key1, val1, key2, val2, key3, val3, key4, val4) { \
    std::stringstream stream; \
    stream << msg << " [" << key1 << "=" << (val1) << ", " << key2 << "=" << (val2) << ", " << key3 << "=" << (val3) << ", " << key4 << "=" << (val4) << "]"; \
    throw ignite::IgniteError(code, stream.str().c_str()); \
}

namespace ignite
{
    /**
     * Ignite error information.
     */
    class IGNITE_IMPORT_EXPORT IgniteError
    {
    public:
        /** Success. */
        static const int IGNITE_SUCCESS = 0;

        /** Failed to initialize JVM. */
        static const int IGNITE_ERR_JVM_INIT = 1;

        /** Failed to attach to JVM. */
        static const int IGNITE_ERR_JVM_ATTACH = 2;
        
        /** JVM library is not found. */
        static const int IGNITE_ERR_JVM_LIB_NOT_FOUND = 3;

        /** Failed to load JVM library. */
        static const int IGNITE_ERR_JVM_LIB_LOAD_FAILED = 4;
        
        /** JVM classpath is not provided. */
        static const int IGNITE_ERR_JVM_NO_CLASSPATH = 5;

        /** JVM error: no class definition found. */
        static const int IGNITE_ERR_JVM_NO_CLASS_DEF_FOUND = 6;

        /** JVM error: no such method. */
        static const int IGNITE_ERR_JVM_NO_SUCH_METHOD = 7;

        /** Memory operation error. */
        static const int IGNITE_ERR_MEMORY = 1001;

        /** Portable error. */
        static const int IGNITE_ERR_PORTABLE = 1002;

        /** Generic Ignite error. */
        static const int IGNITE_ERR_GENERIC = 2000;

        /** Illegal argument passed. */
        static const int IGNITE_ERR_ILLEGAL_ARGUMENT = 2001;

        /** Illegal state. */
        static const int IGNITE_ERR_ILLEGAL_STATE = 2002;

        /** Unsupported operation. */
        static const int IGNITE_ERR_UNSUPPORTED_OPERATION = 2003;

        /** Thread has been interrup. */
        static const int IGNITE_ERR_INTERRUPTED = 2004;

        /** Cluster group is empty. */
        static const int IGNITE_ERR_CLUSTER_GROUP_EMPTY = 2005;

        /** Cluster topology problem. */
        static const int IGNITE_ERR_CLUSTER_TOPOLOGY = 2006;

        /** Compute execution rejected. */
        static const int IGNITE_ERR_COMPUTE_EXECUTION_REJECTED = 2007;

        /** Compute job failover. */
        static const int IGNITE_ERR_COMPUTE_JOB_FAILOVER = 2008;

        /** Compute task cancelled. */
        static const int IGNITE_ERR_COMPUTE_TASK_CANCELLED = 2009;

        /** Compute task timeout. */
        static const int IGNITE_ERR_COMPUTE_TASK_TIMEOUT = 2010;

        /** Compute user undeclared exception. */
        static const int IGNITE_ERR_COMPUTE_USER_UNDECLARED_EXCEPTION = 2011;

        /** Generic cache error. */
        static const int IGNITE_ERR_CACHE = 2012;

        /** Generic cache loader error. */
        static const int IGNITE_ERR_CACHE_LOADER = 2013;

        /** Generic cache writer error. */
        static const int IGNITE_ERR_CACHE_WRITER = 2014;
        
        /** Generic cache entry processor error. */
        static const int IGNITE_ERR_ENTRY_PROCESSOR = 2015;

        /** Cache atomic update timeout. */
        static const int IGNITE_ERR_CACHE_ATOMIC_UPDATE_TIMEOUT = 2016;

        /** Cache partial update. */
        static const int IGNITE_ERR_CACHE_PARTIAL_UPDATE = 2017;
        
        /** Transaction optimisitc exception. */
        static const int IGNITE_ERR_TX_OPTIMISTIC = 2018;

        /** Transaction timeout. */
        static const int IGNITE_ERR_TX_TIMEOUT = 2019;

        /** Transaction rollback. */
        static const int IGNITE_ERR_TX_ROLLBACK = 2020;

        /** Transaction heuristic exception. */
        static const int IGNITE_ERR_TX_HEURISTIC = 2021;

        /** Authentication error. */
        static const int IGNITE_ERR_AUTHENTICATION = 2022;

        /** Security error. */
        static const int IGNITE_ERR_SECURITY = 2023;
        
        /** Unknown error. */
        static const int IGNITE_ERR_UNKNOWN = -1;

        /**
         * Throw an error if code is not IGNITE_SUCCESS.
         *
         * @param err Error.
         */
        static void ThrowIfNeeded(IgniteError& err);

        /**
         * Create empty error.
         */
        IgniteError();

        /**
         * Create error with specific code.
         *
         * @param code Error code.
         */
        IgniteError(const int32_t code);

        /**
         * Create error with specific code and message.
         *
         * @param code Error code.
         * @param msg Message.
         */
        IgniteError(const int32_t code, const char* msg);
        
        /**
         * Copy constructor.
         *
         * @param other Other instance.
         */
        IgniteError(const IgniteError& other);

        /**
         * Assignment operator.
         *
         * @param other Other instance.
         * @return Assignment result.
         */
        IgniteError& operator=(const IgniteError& other);

        /**
         * Destructor.
         */
        ~IgniteError();

        /**
         * Get error code.
         *
         * @return Error code.
         */
        int32_t GetCode();

        /**
         * Get error message.
         *
         * @return Error message.
         */
        const char* GetText();
        
        /**
         * Set error.
         *
         * @param jniCode Error code.
         * @param jniCls Error class.
         * @param jniMsg Error message.
         * @param err Error.
         */
        static void SetError(const int jniCode, const char* jniCls, const char* jniMsg, IgniteError* err);
    private:
        /** Error code. */
        int32_t code;    
        
        /** Error message. */
        char* msg;       
    };    
}

#endif