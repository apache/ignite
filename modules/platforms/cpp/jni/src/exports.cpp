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

#include "ignite/jni/exports.h"
#include "ignite/jni/java.h"

namespace gcj = ignite::jni::java;

/* --- Target methods. --- */
extern "C" {
    int IGNITE_CALL IgniteReallocate(long long memPtr, int cap) {
        return gcj::JniContext::Reallocate(memPtr, cap);
    }

    void IGNITE_CALL IgniteIgnitionStart(gcj::JniContext* ctx, char* cfgPath, char* name, int factoryId, long long dataPtr) {
        ctx->IgnitionStart(cfgPath, name, factoryId, dataPtr);
    }

    long long IGNITE_CALL IgniteIgnitionEnvironmentPointer(gcj::JniContext* ctx, char* name) {
        return ctx->IgnitionEnvironmentPointer(name);
    }

	bool IGNITE_CALL IgniteIgnitionStop(gcj::JniContext* ctx, char* name, bool cancel) {
        return ctx->IgnitionStop(name, cancel);
    }

	void IGNITE_CALL IgniteIgnitionStopAll(gcj::JniContext* ctx, bool cancel) {
        return ctx->IgnitionStopAll(cancel);
    }

    long long IGNITE_CALL IgniteTargetInLongOutLong(gcj::JniContext* ctx, void* obj, int opType, long long val) {
        return ctx->TargetInLongOutLong(static_cast<jobject>(obj), opType, val);
    }

    long long IGNITE_CALL IgniteTargetInStreamOutLong(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        return ctx->TargetInStreamOutLong(static_cast<jobject>(obj), opType, memPtr);
    }

    void IGNITE_CALL IgniteTargetInStreamOutStream(gcj::JniContext* ctx, void* obj, int opType, long long inMemPtr, long long outMemPtr) {
        ctx->TargetInStreamOutStream(static_cast<jobject>(obj), opType, inMemPtr, outMemPtr);
    }

    void* IGNITE_CALL IgniteTargetInStreamOutObject(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        return ctx->TargetInStreamOutObject(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteTargetInObjectStreamOutObjectStream(gcj::JniContext* ctx, void* obj, int opType, void* arg, long long inMemPtr, long long outMemPtr) {
        return ctx->TargetInObjectStreamOutObjectStream(static_cast<jobject>(obj), opType, arg, inMemPtr, outMemPtr);
    }

    void IGNITE_CALL IgniteTargetOutStream(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        ctx->TargetOutStream(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteTargetOutObject(gcj::JniContext* ctx, void* obj, int opType) {
        return ctx->TargetOutObject(static_cast<jobject>(obj), opType);
    }

    void IGNITE_CALL IgniteTargetInStreamAsync(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        ctx->TargetInStreamAsync(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteTargetInStreamOutObjectAsync(gcj::JniContext* ctx, void* obj, int opType, long long memPtr) {
        return ctx->TargetInStreamOutObjectAsync(static_cast<jobject>(obj), opType, memPtr);
    }

    void* IGNITE_CALL IgniteAcquire(gcj::JniContext* ctx, void* obj) {
        return ctx->Acquire(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteRelease(void* obj) {
        gcj::JniContext::Release(static_cast<jobject>(obj));
    }

    void IGNITE_CALL IgniteThrowToJava(gcj::JniContext* ctx, char* err) {
        ctx->ThrowToJava(err);
    }
    
    int IGNITE_CALL IgniteHandlersSize() {
        return sizeof(gcj::JniHandlers);
    }

    void* IGNITE_CALL IgniteCreateContext(char** opts, int optsLen, gcj::JniHandlers* cbs) {
        return gcj::JniContext::Create(opts, optsLen, *cbs);
    }

    void IGNITE_CALL IgniteDeleteContext(gcj::JniContext* ctx) {
        delete ctx;
    }

    void IGNITE_CALL IgniteDestroyJvm(gcj::JniContext* ctx) {
        ctx->DestroyJvm();
    }

    void IGNITE_CALL IgniteSetConsoleHandler(gcj::ConsoleWriteHandler consoleHandler) {
        gcj::JniContext::SetConsoleHandler(consoleHandler);
    }

    void IGNITE_CALL IgniteRemoveConsoleHandler(gcj::ConsoleWriteHandler consoleHandler) {
        gcj::JniContext::RemoveConsoleHandler(consoleHandler);
    }
}