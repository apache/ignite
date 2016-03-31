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
#ifndef _IGNITE_COMMON_COMMON
#define _IGNITE_COMMON_COMMON

#define IGNITE_EXPORT __declspec(dllexport)
#define IGNITE_IMPORT __declspec(dllimport)
#define IGNITE_CALL __stdcall

#define IGNITE_IMPORT_EXPORT IGNITE_EXPORT

#include <iostream>

#define IGNITE_TRACE_ALLOC(addr) \
    std::cout << "ALLOC " << __FILE__ << "(" << __LINE__ << "): 0x" << (void*)addr << std::endl;

/**
 * Common construction to disable copy constructor and assignment for class.
 */
#define IGNITE_NO_COPY_ASSIGNMENT(cls) \
    cls(const cls& src); \
    cls& operator= (const cls& other); 

namespace ignite
{
    namespace common
    {
        /**
         * Helper class to manage attached threads.
         */
        class AttachHelper 
        {
        public:                       
            /**
             * Callback invoked on successful thread attach ot JVM.
             */
            static void OnThreadAttach();
        };   
    }
}

#endif