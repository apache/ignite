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

#include <libpmem.h>
#include <unistd.h>
#include <algorithm>
#include "common.h"
#include "org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil.h"

static const char *CTX_CLS_NAME = "org/apache/ignite/internal/processors/cache/persistence/wal/mmap/optane/Context";
static const size_t PROBE_BUF_SZ = 4096;

JNIEXPORT jobject JNICALL Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_mmap(
        JNIEnv *env,
        jclass,
        jstring path,
        jlong sz
) {
    JStringGuard path_(env, path);

    PMemPtr mem_ptr(path_.data(), (size_t) sz);
    if (mem_ptr == nullptr) {
        throw_errno_fmt(env, "Failed to map %s", path_.data());
        return nullptr;
    }

    jclass ctxClazz = env->FindClass(CTX_CLS_NAME);
    if (!ctxClazz) {
        throw_err_fmt(env, "Could not load %s", CTX_CLS_NAME);
        return nullptr;
    }

    jmethodID constructor = env->GetMethodID(ctxClazz , "<init>", "(Ljava/nio/ByteBuffer;Z)V");
    if (!constructor) {
        throw_err_fmt(env, "Could not obtain %s constructor", CTX_CLS_NAME);
        return nullptr;
    }

    jobject bbuf = env->NewDirectByteBuffer(mem_ptr.get(), (jlong)mem_ptr.length());

    if (!bbuf) {
        throw_err_fmt(env, RUNTIME_ERROR_CLS_NAME,
                      "Could not create java.nio.DirectByteBuffer from address %p with Size %zu",
                      mem_ptr.get(), mem_ptr.length());
        return nullptr;
    }

    jobject res = env->NewObject(ctxClazz, constructor, bbuf , (jboolean)mem_ptr.is_pmem());
    mem_ptr.release();

    return res;
}

JNIEXPORT void JNICALL Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_munmap(
        JNIEnv *env,
        jclass,
        jobject buf
) {
    void *addr = env->GetDirectBufferAddress(buf);
    size_t len = env->GetDirectBufferCapacity(buf);
    errno = 0;

    if (pmem_unmap(addr, len) != 0) {
        throw_errno_fmt(env, RUNTIME_ERROR_CLS_NAME, "Failed to unmap buffer at %p", addr);
    }
}

inline bool check_buffer_length(jlong value, jlong length) {
    if (value < 0) {
        return false;
    }

    if (value > length) {
        return false;
    }

    return true;
}

JNIEXPORT void JNICALL Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_msync(
        JNIEnv *env,
        jclass,
        jobject buf,
        jlong offset,
        jlong length,
        jboolean isPmem
) {
    errno = 0;
    void *addr = env->GetDirectBufferAddress(buf);
    jlong capacity = env->GetDirectBufferCapacity(buf);

    if (!check_buffer_length(offset, capacity)) {
        throw_err_fmt(env, INDEX_OUT_OF_BOUNDS_ERROR_CLS_NAME,
                      "Offset %d is out of bounds of buffer [addr=%p, capacity=%zu]",
                      offset, addr, capacity);
        return;
    }

    if (!check_buffer_length(length, capacity)) {
        throw_err_fmt(env, INDEX_OUT_OF_BOUNDS_ERROR_CLS_NAME,
                      "Length %d is out of bounds of buffer [addr=%p, capacity=%zu]",
                      length, addr, capacity);
        return;
    }

    size_t length_ = static_cast<size_t>(std::min(length, capacity - offset));
    void* addr_ = static_cast<void*>(static_cast<char*>(addr) + static_cast<size_t>(offset));

    if (isPmem) {
        pmem_persist(addr_, length_);
        return;
    }

    if (pmem_msync(addr_, length_) != 0) {
        throw_errno_fmt(env, RUNTIME_ERROR_CLS_NAME,
                        "Failed to msync buffer [addr=%p, capacity=%zu] with offset %ll and Size %ll",
                        addr, capacity, offset, length);
    }
}

JNIEXPORT jboolean Java_org_apache_ignite_internal_processors_cache_persistence_wal_mmap_optane_OptaneUtil_isPmem(
        JNIEnv *env,
        jclass,
        jstring path
) {
    JStringGuard path_(env, path);
    PMemPtr mem_ptr(path_.data(), PROBE_BUF_SZ, PMEM_FILE_CREATE | PMEM_FILE_TMPFILE);
    return (jboolean)mem_ptr.is_pmem();
}
