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

#ifndef _ORG_APACHE_IGNITE_INTERNAL_MEM_NUMAALLOCUTIL_H
#define _ORG_APACHE_IGNITE_INTERNAL_MEM_NUMAALLOCUTIL_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocate(JNIEnv *, jclass, jlong);
JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocateOnNode(JNIEnv *, jclass, jlong, jint);
JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocateLocal(JNIEnv *, jclass, jlong);
JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocateInterleaved(JNIEnv *, jclass, jlong, jintArray);
JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_chunkSize(JNIEnv *, jclass, jlong);
JNIEXPORT void JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_free(JNIEnv *, jclass, jlong);
JNIEXPORT jint JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_nodesCount(JNIEnv *, jclass);

#ifdef __cplusplus
}
#endif
#endif // _ORG_APACHE_IGNITE_INTERNAL_MEM_NUMAALLOCUTIL_H
