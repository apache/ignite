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

#ifndef _common_h
#define _common_h

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <jni.h>
#include <libpmem.h>

static const size_t ERR_MSG_SZ = 128;
static const size_t ERRNO_MSG_SZ = 64;
static const char* RUNTIME_ERROR_CLS_NAME = "java/lang/RuntimeException";
static const char* INDEX_OUT_OF_BOUNDS_ERROR_CLS_NAME = "java/lang/IndexOutOfBoundsException";

class JStringGuard {
public:
    JStringGuard(JNIEnv* env, jstring str): env_(env), java_str_(str) {
        str_ = env->GetStringUTFChars(java_str_, nullptr);
        length_ = static_cast<size_t>(env->GetStringLength(java_str_));
    }

    JStringGuard() = delete;
    JStringGuard(const JStringGuard&) = delete;
    JStringGuard& operator=(const JStringGuard&) = delete;

    ~JStringGuard() {
        env_->ReleaseStringUTFChars(java_str_, str_);
    }

    const char* data() const {
        return str_;
    }

    size_t length() const {
        return length_;
    }
private:
    JNIEnv* env_;
    jstring java_str_;
    const char* str_;
    size_t length_;
};

class PMemPtr {
public:
    PMemPtr(const char* path, const size_t sz, const int flags = PMEM_FILE_CREATE,
            const mode_t mode = 0666) {
        errno = 0;
        addr_ = pmem_map_file(path, sz, flags, mode, &len_, &is_pmem_);
    }

    PMemPtr(PMemPtr&& other) noexcept {
        std::swap(is_pmem_, other.is_pmem_);
        std::swap(len_, other.len_);
        addr_ = other.release();
    }

    PMemPtr() = default;
    PMemPtr(const PMemPtr&) = delete;
    PMemPtr& operator=(const PMemPtr&) = delete;
    PMemPtr& operator=(PMemPtr) = delete;

    PMemPtr& operator=(PMemPtr &&rhs) noexcept {
        if (this != &rhs) {
            std::swap(is_pmem_, rhs.is_pmem_);
            std::swap(len_, rhs.len_);
            addr_ = rhs.release();
        }
        return *this;
    }

    friend bool operator==(const PMemPtr& lhs, const PMemPtr& rhs) {
        return lhs.addr_ == rhs.addr_;
    }

    friend bool operator!=(const PMemPtr& lhs, const PMemPtr& rhs) {
        return lhs.addr_ != rhs.addr_;
    }

    friend bool operator==(const PMemPtr& lhs, std::nullptr_t) {
        return !lhs.addr_;
    }

    friend bool operator!=(const PMemPtr& lhs, std::nullptr_t) {
        return static_cast<bool>(lhs.addr_);
    }

    friend bool operator==(std::nullptr_t, const PMemPtr& rhs) {
        return !rhs.addr_;
    }

    friend bool operator!=(std::nullptr_t, const PMemPtr& rhs){
        return static_cast<bool>(rhs.addr_);
    }

    ~PMemPtr() noexcept {
        if(addr_ != nullptr) {
            errno = 0;
            pmem_unmap(addr_, len_);
        }
    }

    size_t length() const noexcept{
        return len_;
    }

    bool is_pmem() const noexcept {
        return is_pmem_ != 0;
    }

    void* get() const noexcept {
        return addr_;
    }

    void* release() noexcept{
        void* res = addr_;
        is_pmem_ = 0; len_ = 0; addr_ = nullptr;
        return res;
    }
private:
    size_t len_ = 0;
    int is_pmem_ = 0;
    void* addr_ = nullptr;
};

inline void throw_err(JNIEnv *env, const char* cls_name, const char *msg) {
    jclass xklass = env->FindClass(cls_name);
    env->ThrowNew(xklass, msg);
}

inline void throw_err_fmt(JNIEnv *env, const char* cls_name, const char *fmt, ...) {
    char buf[ERR_MSG_SZ];

    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);

    throw_err(env, cls_name, buf);
}

inline void throw_errno_fmt(JNIEnv *env, const char* cls_name, const char *fmt, ...) {
    char buf[ERR_MSG_SZ];

    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);

    if (errno) {
        char errno_buf[ERRNO_MSG_SZ];
        snprintf(errno_buf, sizeof(errno_buf), ": errno=%d %s", errno, strerror(errno));
        strncat(buf, errno_buf, ERRNO_MSG_SZ);
    }

    throw_err(env, cls_name, buf);
}

#endif
