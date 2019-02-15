/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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