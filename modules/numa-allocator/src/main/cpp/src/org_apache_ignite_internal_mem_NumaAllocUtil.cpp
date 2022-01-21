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

#include <numa/numa_alloc.h>
#include "org_apache_ignite_internal_mem_NumaAllocUtil.h"

class JIntArrayGuard {
public:
    JIntArrayGuard(JNIEnv* env, jintArray arr): env_(env), java_arr_(arr) {
        arr_ = env->GetIntArrayElements(java_arr_, nullptr);
        length_ = static_cast<size_t>(env->GetArrayLength(java_arr_));
    }

    JIntArrayGuard() = delete;
    JIntArrayGuard(const JIntArrayGuard&) = delete;
    JIntArrayGuard& operator=(const JIntArrayGuard&) = delete;

    ~JIntArrayGuard() {
        env_->ReleaseIntArrayElements(java_arr_, arr_, 0);
    }

    const int& operator[](size_t pos) const {
        return arr_[pos];
    }

    int& operator[](size_t pos) {
        return arr_[pos];
    }

    size_t Size() const {
        return length_;
    }
private:
    JNIEnv* env_;
    jintArray java_arr_;
    jint* arr_;
    size_t length_;
};

JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocate(
        JNIEnv*,
        jclass,
        jlong size
) {
    void* ptr = numa::Alloc(static_cast<size_t>(size));
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocateOnNode(
        JNIEnv*,
        jclass,
        jlong size,
        jint node
) {
    void* ptr;
    auto size_ = static_cast<size_t>(size);
    if (node >= 0 && node < numa::NumaNodesCount()) {
        ptr = numa::Alloc(size_, static_cast<int>(node));
    }
    else {
        ptr = numa::Alloc(size_);
    }
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocateLocal(
        JNIEnv*,
        jclass,
        jlong size
) {
    void* ptr = numa::AllocLocal(static_cast<size_t>(size));
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_allocateInterleaved(
        JNIEnv *jniEnv,
        jclass,
        jlong size,
        jintArray arr
) {
    void* ptr;
    auto size_ = static_cast<size_t>(size);
    if (arr != nullptr) {
        JIntArrayGuard nodes(jniEnv, arr);

        if (nodes.Size() > 0) {
            numa::BitSet bs;
            for (size_t i = 0; i < nodes.Size(); ++i) {
                bs.Set(nodes[i]);
            }
            ptr = numa::AllocInterleaved(size_, bs);
        }
        else {
            ptr = numa::AllocInterleaved(size_);
        }
    }
    else {
        ptr = numa::AllocInterleaved(size_);
    }
    return reinterpret_cast<jlong>(ptr);
}

JNIEXPORT jlong JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_chunkSize(JNIEnv *, jclass, jlong addr) {
    void* ptr = reinterpret_cast<void*>(addr);
    return static_cast<jlong>(numa::Size(ptr));
}

JNIEXPORT void JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_free(JNIEnv *, jclass, jlong addr) {
    void* ptr = reinterpret_cast<void*>(addr);
    numa::Free(ptr);
}

JNIEXPORT jint JNICALL Java_org_apache_ignite_internal_mem_NumaAllocUtil_nodesCount(JNIEnv *, jclass) {
    return static_cast<jint>(numa::NumaNodesCount());
}
