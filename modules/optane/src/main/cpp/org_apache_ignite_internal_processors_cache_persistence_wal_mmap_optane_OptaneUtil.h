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

#ifndef _org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil
#define _org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jobject JNICALL Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_mmap(
        JNIEnv *, jclass, jstring, jlong);

JNIEXPORT void JNICALL Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_munmap(
        JNIEnv *, jclass, jobject);

JNIEXPORT void JNICALL Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_msync(
        JNIEnv *, jclass, jobject, jlong, jlong, jboolean);

JNIEXPORT jboolean JNICALL Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_isPmem(
        JNIEnv *, jclass, jstring);

#ifdef __cplusplus
}
#endif
#endif
