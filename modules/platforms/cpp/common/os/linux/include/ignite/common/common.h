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

#ifndef __has_attribute
#   define __has_attribute(x) 0
#endif

#if (defined(__GNUC__) && ((__GNUC__ > 4) || (__GNUC__ == 4) && (__GNUC_MINOR__ > 2))) || __has_attribute(visibility)
#   define IGNITE_EXPORT __attribute__((visibility("default")))
#   define IGNITE_IMPORT __attribute__((visibility("default")))
#else
#   define IGNITE_EXPORT
#   define IGNITE_IMPORT
#endif

#define IGNITE_CALL

#ifdef IGNITE_IMPL
#   define IGNITE_IMPORT_EXPORT IGNITE_EXPORT
#else
#   define IGNITE_IMPORT_EXPORT IGNITE_IMPORT
#endif

#if (__cplusplus >= 201103L)
#   define IGNITE_NO_THROW noexcept
#else
#   define IGNITE_NO_THROW throw()
#endif

#define IGNITE_UNUSED(x) ((void) x)

/**
 * Common construction to disable copy constructor and assignment for class.
 */
#define IGNITE_NO_COPY_ASSIGNMENT(cls) \
    cls(const cls& src); \
    cls& operator= (const cls& other)

#endif //_IGNITE_COMMON_COMMON
