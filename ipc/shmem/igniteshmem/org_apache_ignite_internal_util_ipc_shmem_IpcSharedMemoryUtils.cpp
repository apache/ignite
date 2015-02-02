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

#include "org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/sem.h>
#include <sys/stat.h>
#include <string>
#include <iostream>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <signal.h>

using namespace std;

/** IgniteCheckedException JNI class name. */
const char* IGNITE_EXCEPTION = "org/apache/ignite/IgniteCheckedException";

/** IpcSharedMemoryOperationTimedoutException JNI class name. */
const char* OP_TIMEDOUT_EXCEPTION = "org/apache/ignite/internal/util/ipc/shmem/IpcSharedMemoryOperationTimedoutException";

/** IpcOutOfSystemResourcesException JNI class name. */
const char* OUT_OF_RSRCS_EXCEPTION = "org/apache/ignite/internal/util/ipc/shmem/IpcOutOfSystemResourcesException";

/** Global flag for enabling debug logging. */
static bool __GG_DEBUG = false;

/** Read semaphore ID. */
#define SEM_READ 0

/** Write semaphore ID. */
#define SEM_WRITE 1

/**
 * Logging macro.
 *
 * @param m Logging message with optional formatting symbols.
 * @param varagrs Formatting arguments.
 */
#define GG_LOG_DEBUG(m, ...) {\
    if(__GG_DEBUG)\
        log(__FILE__, __LINE__, __FUNCTION__, m, __VA_ARGS__);\
}

/** Buffer size for current time string. */
#define TIME_NOW_BUF_SIZE 1024

/** Buffer size for debug message. */
#define FORMAT_LOG_BUF_SIZE 4096

/**
 * @return Current time string in format: year-month-day hour:minute:second.
 */
static string timeNow() {
    timeval tv;
    tm lt;

    char timebuf[TIME_NOW_BUF_SIZE];

    gettimeofday(&tv, 0);

    time_t now = tv.tv_sec;
    localtime_r(&now, &lt);

    // Clone the format used by log4j ISO8601DateFormat,
    // specifically: "yyyy-MM-dd HH:mm:ss.SSS"
    size_t len = strftime(timebuf, TIME_NOW_BUF_SIZE, "%Y-%m-%d %H:%M:%S", &lt);

    snprintf(timebuf + len, TIME_NOW_BUF_SIZE - len, ".%03d", (int) (tv.tv_usec / 1000));

    return string(timebuf);
}

/**
 * Writes debug message to standard output, if global debug flag is enabled.
 *
 * @param file Source file, from which the message originates.
 * @param line Source line, from which the message originates.
 * @param funcName Name of the function, from which the message originates.
 * @param format Message string with optional formatting symbols.
 * @param varargs Formatting arguments.
 */
static void log(const char* file, int line, const char* funcName, const char* format, ...) {
    static pid_t pid = getpid();

    char msgbuf[FORMAT_LOG_BUF_SIZE];
    va_list va;

    va_start(va, format);
    vsnprintf(msgbuf, FORMAT_LOG_BUF_SIZE - 1, format, va);
    va_end(va);

    cout << timeNow() << " pid:" << pid << " " << file << ":" << funcName << ":" << line << ": " << msgbuf << endl;

    flush(cout);
}

/** Lock operation on semaphore #0. */
struct sembuf op_lock0[2] = {
    0, 0, 0, // Wait until semaphore #0 becomes 0.
    0, 1, 0  // Then increment semaphore #0 by 1.
};

/** Lock operation on semaphore #1. */
struct sembuf op_lock1[2] = {
    1, 0, 0, // Wait until semaphore #1 becomes 0.
    1, 1, 0  // Then increment semaphore #1 by 1.
};

/**
 * Data offset in shared memory buffer (the memory segment preceding data
 * is used for IPC data).
 */
#define BUF_OFFSET 64

/**
 * IPC data, that is used for inter-process communication.
 */
typedef struct {
    /** Number of parties that have closed the connection (0, 1, or 2). */
    int closedCnt;

    /** Shared memory segment ID. */
    int shmId;

    /** Semaphore set ID. */
    int semId;

    /** Shared memory segment size. */
    int size;

    /** Closed flag. */
    volatile bool closed;

    /** Read position. */
    volatile unsigned int readCnt;

    /** Write position (should be always >= readCnt). */
    volatile unsigned int writeCnt;

    /** Flag, indicating that reader is waiting on semaphore. */
    volatile bool readBlocked;

    /** Flag, indicating that writer is waiting on semaphore. */
    volatile bool writeBlocked;
} T_IpcData;

/**
 * Calculates unread bytes count, given IPC data pointer.
 *
 * @param ipcData IPC data pointer.
 * @param fetchWriteCnt True to fetch write count or read it normally.
 * @param fetchReadCnt True to fetch read count or read it normally..
 * @return Unread bytes count.
 */
static unsigned int getUnreadCount(T_IpcData *ipcData, bool fetchWriteCnt, bool fetchReadCnt) {
    unsigned int writeCnt = fetchWriteCnt ? __sync_fetch_and_add(&ipcData->writeCnt, 0) : ipcData->writeCnt;
    unsigned int readCnt = fetchReadCnt ? __sync_fetch_and_add(&ipcData->readCnt, 0) : ipcData->readCnt;

    unsigned int unreadCnt = writeCnt - readCnt;

    if (unreadCnt < 0) {
        GG_LOG_DEBUG("Unread count failed [writeCnt=%u, readCnt=%u]", writeCnt, readCnt);

        *((char *) 0) = 5;
    }

    return unreadCnt;
}

/**
 * Throws exception in Java code.
 *
 * @param env JNI environment.
 * @param clsName Exception class full name (slashed notation).
 */
static void throwException(JNIEnv* env, const char* clsName) {
    // We assume that 512 bytes will be enough.
    char msg[512];

    ::sprintf(msg, "%s (error code: %d).", ::strerror(errno), errno);

    env->ThrowNew(env->FindClass(clsName), msg);
}

/**
 * Throws exception in Java code according to current
 * errno value.
 *
 * @param env JNI environment.
 */
static void throwExceptionByErrno(JNIEnv* env) {
    switch (errno) {
    case ENOMEM:
    case EMFILE:
    case ENOSPC:
        throwException(env, OUT_OF_RSRCS_EXCEPTION);
        break;

    default:
        throwException(env, IGNITE_EXCEPTION);
        break;
    }
}

/**
 * Initializes semaphore.
 *
 * @param env JNI environment.
 * @param semId Semaphore set ID.
 * @param semNum Semaphore number in semaphore set.
 */
static bool semInit(JNIEnv* env, int semId, int semNum) {
    struct sembuf sb;
    memset(&sb, 0, sizeof(sb));

    // Initialize the semaphore.
    sb.sem_op = 1;
    sb.sem_num = semNum;

    if (::semop(semId, &sb, 1) == -1) {
        GG_LOG_DEBUG("Semaphore init failed [semId=%d, semNum=%s, errno=%d]", semId,
                semNum == SEM_READ ? "SEM_READ" : "SEM_WRITE", errno);

        throwException(env, IGNITE_EXCEPTION);

        return false;
    }

    return true;
}

/**
 * Waits on semaphore until another process has signaled or the semaphore
 * has been removed.
 *
 * @param env JNI environment.
 * @param semId Semaphore set ID.
 * @param semNum Semaphore number in semaphore set.
 * @param timeout Timeout for wait operation, if supported on current platform.
 * @param ipcData IPC data pointer.
 * @see semNotify()
 */
static void semWait(JNIEnv * env, int semId, int semNum, int timeout, T_IpcData *ipcData) {
    while (1) {
        int ret;
#ifdef HAVE_SEMTIMEDOP
        _STRUCT_TIMESPEC timeout0 = {
            0, timeout * 1000
        };
        ret = semtimedop(semId, semNum == 0 ? op_lock0 : op_lock1, 2, timeout > 0 ? &timeout0 : NULL);
#else
        ret = semop(semId, semNum == 0 ? op_lock0 : op_lock1, 2);
#endif
        if (ret == 0)
            return;

        if (errno == EIDRM || errno == EINVAL) { // Semaphore was removed while waiting.
            if (!ipcData->closed) {
                GG_LOG_DEBUG("Semaphore removed, but the space is not closed [semId=%d]", semId);

                ipcData->closed = true;
            }

            return;
        }

        GG_LOG_DEBUG("Semaphore wait failed [semId=%d, semNum=%s, errno=%d]", semId,
                semNum == SEM_READ ? "SEM_READ" : "SEM_WRITE", errno);

        if (errno == EINTR) {
            // spin again
        }
        else if (errno == EAGAIN) {
            throwException(env, OP_TIMEDOUT_EXCEPTION);

            return;
        }
        else {
            throwException(env, IGNITE_EXCEPTION);
        }
    }
}

/**
 * Notifies the semaphore to signal other process waiting on this semaphore
 * to resume execution.
 *
 * @param env JNI environment.
 * @param semId Semaphore set ID.
 * @param semNum Semaphore number in semaphore set.
 * @param ipcData IPC data pointer.
 * @see semWait()
 */
static void semNotify(JNIEnv * env, int semId, int semNum, T_IpcData *ipcData) {
    if (::semctl(semId, semNum, SETVAL, 0) == -1) {
        if (errno == EIDRM || errno == EINVAL) {
            if (!ipcData->closed) {
                GG_LOG_DEBUG("Semaphore removed, but the space is not closed [semId=%d]", semId);

                ipcData->closed = true;
            }

            return;
        }

        GG_LOG_DEBUG("Semaphore wait failed [semId=%d, semNum=%s, errno=%d]", semId,
                semNum == SEM_READ ? "SEM_READ" : "SEM_WRITE", errno);

        throwException(env, IGNITE_EXCEPTION);
    }
}

/**
 * Allocates shared memory segment and semaphores for inter-process communication (JNI method).
 *
 * @param env JNI environment.
 * @param jTokFileName Token file name for allocating resources.
 * @param size Shared memory segment size in bytes.
 * @param debug Debug flag, which modifies the global debug flag. This parameter is expected
 *              to be always the same during application lifetime.
 */
jlong Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_allocateSystemResources(
        JNIEnv * env, jclass, jstring jTokFileName, jint size, jboolean debug) {
    int key = -1;
    int shmId = -1;
    int semId = -1;
    jboolean isCopy = false;

    // Copy to STL string and release.
    const char* tokFileName0 = env->GetStringUTFChars(jTokFileName, &isCopy);

    string tokFileName(tokFileName0);

    env->ReleaseStringUTFChars(jTokFileName, tokFileName0);

    // Set global debug flag.
    __GG_DEBUG = debug;

    // Get system token, used in IPC.
    if ((key = ::ftok(tokFileName.c_str(), 'G')) == -1) {
        throwException(env, IGNITE_EXCEPTION);

        return 0;
    }

    // Get shared memory descriptor (create shared memory segment if absent).
    if ((shmId = ::shmget(key, size + BUF_OFFSET, 0666 | IPC_CREAT)) == -1) {
        throwExceptionByErrno(env);

        return 0;
    }

    void* data = ::shmat(shmId, (void *) 0, 0);

#ifndef __APPLE__
    // Shared memory segment will be deleted upon last process detach.
    ::shmctl(shmId, IPC_RMID, NULL);
#else
    GG_LOG_DEBUG("Will not mark shared memory region for deletion (will be removed on close): %d", shmId);
#endif

    if ((ptrdiff_t) data == -1) {
        // Exception will be thrown on return.
        throwExceptionByErrno(env);

        return 0;
    }

    T_IpcData *ipcData = (T_IpcData*) data;

    // Allocate semaphores for native synchronization.
    if ((semId = ::semget(key, 2, 0666 | IPC_CREAT)) == -1) {
        // Exception will be thrown on return.
        throwExceptionByErrno(env);

        // Cleanup ignoring possible errors.
        ::shmdt(ipcData);

        return 0;
    }

    // Initialize SEM_READ and SEM_WRITE to 1.
    if (!semInit(env, semId, SEM_READ) || !semInit(env, semId, SEM_WRITE)) {
        // Exception will be thrown on return.
        throwException(env, IGNITE_EXCEPTION);

        // Cleanup ignoring possible errors.
        ::semctl(semId, 0, IPC_RMID);
        ::shmdt(ipcData);

        return 0;
    }

    // Initialize data structure.
    memset(ipcData, 0, sizeof(*ipcData));
    ipcData->shmId = shmId;
    ipcData->semId = semId;
    ipcData->size = size;

    return (jlong) (((char*) data) + BUF_OFFSET);
}

/**
 * Attaches to an existing shared memory segment (JNI method).
 *
 * @param env JNI environment.
 * @param shmId Shared memory segment ID.
 * @param debug Debug flag, which modifies the global debug flag. This parameter is expected
 *              to be always the same during application lifetime.
 */
jlong Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_attach(JNIEnv * env, jclass,
        jint shmId, jboolean debug) {
    // Set global debug flag.
    __GG_DEBUG = debug;

    void* data = ::shmat(shmId, (void *) 0, 0);

    GG_LOG_DEBUG("Attaching to shmId: %d", shmId);

    if ((ptrdiff_t) data == -1) {
        // Exception will be thrown on return.
        throwExceptionByErrno(env);

        return 0;
    }

    T_IpcData *ipcData = (T_IpcData*) data;

    return (jlong) (((char*) data) + BUF_OFFSET);
}

/**
 * Shuts down inter-process communication (JNI method).
 *
 * @param env JNI environment.
 * @param buf Data buffer pointer in shared memory segment.
 */
void Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_ipcClose(JNIEnv *env, jclass, jlong buf) {
    T_IpcData *ipcData = (T_IpcData*) (((char *) buf) - BUF_OFFSET);

    // Set closed flag to true using memory barrier.
    // This is to ensure the flag is set BEFORE we notify
    // the semaphores (i.e. no reordering will happen).
    __sync_fetch_and_add(&ipcData->closed, 1);

    // Remove semaphore.
    if (::semctl(ipcData->semId, 0, IPC_RMID) == -1) {
        if (__GG_DEBUG)
            cerr << "Failed to remove semaphore: " << errno << ": " << strerror(errno) << endl << flush;

        if (errno == EPERM) { // Operation not permitted (no rights).
            // Signal both reader and writer (because we don't know who we are).
            // The other side will remove the semaphore.
            semNotify(env, ipcData->semId, SEM_READ, ipcData);
            semNotify(env, ipcData->semId, SEM_WRITE, ipcData);
        }
    }
}

/**
 * Detaches from shared memory segment and removes the token file.
 *
 * @param env JNI environment.
 * @param jTokFileName Token file name for allocating resources.
 * @param buf Data buffer pointer in shared memory segment.
 * @param force Force flag for forcing resources removal.
 *
 */
void Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_freeSystemResources__Ljava_lang_String_2JZ(
        JNIEnv* env, jclass, jstring jTokFileName, jlong buf, jboolean force) {
    T_IpcData *ipcData = (T_IpcData*) (((char *) buf) - BUF_OFFSET);

    if (__sync_bool_compare_and_swap(&ipcData->closedCnt, 0, 1) && !force) {
        ::shmdt(ipcData);

        return;
    }

#ifdef __APPLE__
    int shmId = ipcData->shmId;
#endif

    // Detach from shared memory (shared memory segment will be deleted upon last detach).
    if (::shmdt(ipcData) == -1) {
        // If error occurred, then return.
        return;
    }

#ifdef __APPLE__
    GG_LOG_DEBUG("Deleting shared memory region: %d", shmId);

    ::shmctl(shmId, IPC_RMID, NULL);
#endif

    jboolean isCopy = false;

    // Copy to STL string and release.
    const char* tokFileName0 = env->GetStringUTFChars(jTokFileName, &isCopy);

    string tokFileName(tokFileName0);

    env->ReleaseStringUTFChars(jTokFileName, tokFileName0);

    ::remove(tokFileName.c_str());
}

/**
 * Removes semaphores and shared memory segment.
 *
 * @param env JNI environment.
 * @param jTokFileName Token file name for allocating resources.
 * @param size Shared memory segment size in bytes.
 */
void Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_freeSystemResources__Ljava_lang_String_2I(
        JNIEnv* env, jclass, jstring jTokFileName, jint size) {
    int key = -1;
    int shmId = -1;
    int semId = -1;
    jboolean isCopy = false;

    // Copy to STL string and release.
    const char* tokFileName0 = env->GetStringUTFChars(jTokFileName, &isCopy);

    string tokFileName(tokFileName0);

    env->ReleaseStringUTFChars(jTokFileName, tokFileName0);

    // Get system token, used in IPC.
    if ((key = ::ftok(tokFileName.c_str(), 'G')) == -1) {
        return;
    }

    // Get semaphores for native synchronization (no create).
    if ((semId = ::semget(key, 2, 0666)) > 0) {
        // Remove semaphores if present.
        ::semctl((int) semId, 0, IPC_RMID);
    }

    // Get shared memory descriptor (no create).
    if ((shmId = ::shmget(key, size, 0666)) == -1) {
        // This means that shared memory segment was not created or was removed.
        // No point to continue, as semaphores do not exist as well.
        return;
    }

    // Remove shared memory segment (ignoring possible errors).
    ::shmctl((int) shmId, IPC_RMID, NULL);
}

/**
 * Read-write operations for shared memory segments.
 */
class RW {
public:
    /**
     * Copies data from shared memory to the destination buffer.
     *
     * @param env JNI environment.
     * @param dest Destination buffer.
     * @param dOffset Destination buffer offset.
     * @param len Number of bytes to copy.
     * @param src Source pointer in shared memory segment.
     */
    static void FromShMem(JNIEnv *env, jbyteArray dest, jlong dOffset, jlong len, void *src) {
        env->SetByteArrayRegion(dest, dOffset, len, (jbyte*) src);
    }

    /**
     * Copies data from shared memory to the destination Java object.
     *
     * @param env JNI environment.
     * @param dest Destination Java object.
     * @param dOffset Destination object offset.
     * @param len Number of bytes to copy.
     * @param src Source pointer in shared memory segment.
     */
    static void FromShMem(JNIEnv *env, jobject dest, jlong dOffset, jlong len, void *src) {
        char *destAddr = ((char *) env->GetDirectBufferAddress(dest)) + dOffset;
        memcpy((void*) destAddr, src, len);
    }

    /**
     * Copies data from buffer to shared memory.
     *
     * @param env JNI environment.
     * @param src Source buffer.
     * @param sOffset Source buffer offset.
     * @param len Number of bytes to copy.
     * @param dest Destination pointer.
     */
    static void ToShMem(JNIEnv *env, jbyteArray src, jlong sOffset, jlong len, void *dest) {
        env->GetByteArrayRegion(src, sOffset, len, (jbyte*) dest);
    }

    /**
     * Copies data from Java object to shared memory.
     *
     * @param env JNI environment.
     * @param src Source Java object.
     * @param sOffset Source Java object offset.
     * @param len Number of bytes to copy.
     * @param dest Destination pointer.
     */
    static void ToShMem(JNIEnv *env, jobject src, jlong sOffset, jlong len, void *dest) {
        char *srcAddr = ((char *) env->GetDirectBufferAddress(src)) + sOffset;
        memcpy(dest, (void*) srcAddr, len);
    }
};

/**
 * Helper method for copying data from shared memory.
 *
 * @param env JNI environment.
 * @param shMemPtr Data pointer in shared memory segment.
 * @param dest Destination object to copy data to.
 * @param dOffset Destination object write offset.
 * @param len Number of bytes to copy.
 * @param timeout Operation timeout in milliseconds.
 * @param <T> Destination object type.
 */
template<class T>
jlong Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_ReadShMem(JNIEnv *env, jclass, jlong shMemPtr,
        T dest, jlong dOffset, jlong len, jlong timeout) {
    T_IpcData *ipcData = (T_IpcData*) (((char *) shMemPtr) - BUF_OFFSET);

    unsigned int unreadCnt = getUnreadCount(ipcData, true, false);

    while (unreadCnt == 0) {
        if (unreadCnt == 0 && ipcData->closed) {
            return -1;
        }

        // signal the other party, if it's blocked
        if (ipcData->writeBlocked) {
            GG_LOG_DEBUG("Before write semaphore notification [semId=%d]", ipcData->semId);

            semNotify(env, ipcData->semId, SEM_WRITE, ipcData);
        }

        GG_LOG_DEBUG("Before read semaphore wait [semId=%d]", ipcData->semId);

        ipcData->readBlocked = 1;
        semWait(env, ipcData->semId, SEM_READ, timeout, ipcData);
        ipcData->readBlocked = 0;

        unreadCnt = getUnreadCount(ipcData, true, false);
    }

    int bytesRead = 0;

    while (unreadCnt > 0 && bytesRead < len) {
        int pos = ipcData->readCnt % ipcData->size;
        int len0 = (ipcData->size - pos < unreadCnt) ? ipcData->size - pos : unreadCnt;

        if (len0 > len - bytesRead) {
            len0 = len - bytesRead;
        }

        RW::FromShMem(env, dest, dOffset + bytesRead, len0, (void*) (shMemPtr + pos));

        __sync_add_and_fetch(&ipcData->readCnt, len0);

        GG_LOG_DEBUG("Updated read count [readCnt=%d]", ipcData->readCnt);

        bytesRead += len0;

        GG_LOG_DEBUG("Before write semaphore notification [semId=%d]", ipcData->semId);

        semNotify(env, ipcData->semId, SEM_WRITE, ipcData);

        unreadCnt = getUnreadCount(ipcData, true, false);
    }

    return bytesRead;
}

/**
 * Helper method for copying data to shared memory.
 *
 * @param env JNI environment.
 * @param shMemPtr Data pointer in shared memory segment.
 * @param src Source object to copy data from.
 * @param dOffset Destination object read offset.
 * @param len Number of bytes to copy.
 * @param timeout Operation timeout in milliseconds.
 * @param <T> Source object type.
 */
template<class T>
void Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_WriteShMem(JNIEnv *env, jclass,
        jlong shMemPtr, T src, jlong sOffset, jlong len, jlong timeout) {
    T_IpcData *ipcData = (T_IpcData*) (((char *) shMemPtr) - BUF_OFFSET);

    int bytesWritten = 0;

    while (bytesWritten < len) {
        // Wait for reader.
        unsigned int unreadCnt = getUnreadCount(ipcData, false, true);
        int pos = ipcData->writeCnt % ipcData->size;

        while (unreadCnt == ipcData->size) {
            if (ipcData->closed) {
                env->ThrowNew(env->FindClass(IGNITE_EXCEPTION), "Shared memory segment has been closed.");

                return;
            }

            // signal the other party, if it's blocked
            if (ipcData->readBlocked) {
                semNotify(env, ipcData->semId, SEM_READ, ipcData);
            }

            ipcData->writeBlocked = 1;
            semWait(env, ipcData->semId, SEM_WRITE, timeout, ipcData);
            ipcData->writeBlocked = 0;

            unreadCnt = getUnreadCount(ipcData, false, true);
        }

        int len0 = ipcData->size - ((pos > unreadCnt) ? pos : unreadCnt);

        if (len0 > len - bytesWritten) {
            len0 = len - bytesWritten;
        }

        if (ipcData->closed) {
            env->ThrowNew(env->FindClass(IGNITE_EXCEPTION), "Shared memory segment has been closed");

            return;
        }

        RW::ToShMem(env, src, sOffset + bytesWritten, len0, (void*) (shMemPtr + pos));

        __sync_add_and_fetch(&ipcData->writeCnt, len0);

        GG_LOG_DEBUG("Updated write count [readCnt=%d]", ipcData->readCnt);

        bytesWritten += len0;

        semNotify(env, ipcData->semId, SEM_READ, ipcData);
    }
}

/**
 * Copies data from Java byte array to shared memory (JNI method).
 *
 * @param env JNI environment.
 * @param clsName Java class name.
 * @param shMemPtr Data pointer in shared memory segment.
 * @param src Source Java byte array.
 * @param dOffset Source Java byte array offset.
 * @param len Number of bytes to copy.
 * @param timeout Operation timeout in milliseconds.
 */
void Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_writeSharedMemory(JNIEnv *env, jclass clsName,
        jlong shMemPtr, jbyteArray src, jlong sOffset, jlong len, jlong timeout) {
    Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_WriteShMem<jbyteArray>(env, clsName, shMemPtr,
            src, sOffset, len, timeout);
}

/**
 * Copies data from Java byte array to shared memory (JNI method).
 *
 * @param env JNI environment.
 * @param clsName Java class name.
 * @param shMemPtr Data pointer in shared memory segment.
 * @param src Source Java object.
 * @param dOffset Source Java object offset.
 * @param len Number of bytes to copy.
 * @param timeout Operation timeout in milliseconds.
 */
void Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_writeSharedMemoryByteBuffer(JNIEnv *env,
        jclass clsName, jlong shMemPtr, jobject src, jlong sOffset, jlong len, jlong timeout) {
    Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_WriteShMem<jobject>(env, clsName, shMemPtr,
            src, sOffset, len, timeout);
}

/**
 * Copies data from shared memory to Java byte array (JNI method).
 *
 * @param env JNI environment.
 * @param clsName Java class name.
 * @param shMemPtr Data pointer in shared memory segment.
 * @param dest Destination Java byte array.
 * @param dOffset Destination Java byte array offset.
 * @param size Number of bytes to copy.
 * @param timeout Operation timeout in milliseconds.
 */
jlong Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_readSharedMemory(JNIEnv *env, jclass clsName,
        jlong shMemPtr, jbyteArray dest, jlong dOffset, jlong size, jlong timeout) {
    return Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_ReadShMem<jbyteArray>(env, clsName, shMemPtr,
            dest, dOffset, size, timeout);
}

/**
 * Copies data from shared memory to Java object (JNI method).
 *
 * @param env JNI environment.
 * @param clsName Java class name.
 * @param shMemPtr Data pointer in shared memory segment.
 * @param dest Destination Java object.
 * @param dOffset Destination Java object offset.
 * @param size Number of bytes to copy.
 * @param timeout Operation timeout in milliseconds.
 */
jlong Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_readSharedMemoryByteBuffer(JNIEnv *env,
        jclass clsName, jlong shMemPtr, jobject dest, jlong dOffset, jlong size, jlong timeout) {
    return Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_ReadShMem<jobject>(env, clsName, shMemPtr,
            dest, dOffset, size, timeout);
}

/**
 * Gets the number of unread bytes in shared memory segment (JNI method).
 *
 * @param shMemPtr Data pointer in shared memory segment.
 * @return Number of uneread bytes.
 */
jint Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_unreadCount(JNIEnv*, jclass, jlong shMemPtr) {
    T_IpcData *ipcData = (T_IpcData*) (((char *) shMemPtr) - BUF_OFFSET);

    return getUnreadCount(ipcData, true, true);
}

/**
 * Checks if the counterpart is alive (JNI method).
 *
 * @param pid Process ID of the counterpart.
 * @return true if couterpart is alive, false otherwise.
 */
jboolean Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_alive(JNIEnv*, jclass, jint pid) {
    int res = kill((int) pid, 0);

    // Return true if signal was sent or there is no permission to send signal to this process.
    // If kill failed with error and errno is ESRCH or EINVAL, process is considered to be dead.
    return res == 0 || errno == EPERM;
}

/**
 * Gets the shared memory segment ID for a given shared memory data pointer (JNI method).
 *
 * @param shMemPtr Shared memory data pointer.
 * @return Shared memory ID.
 */
jint Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_sharedMemoryId(JNIEnv*, jclass, jlong shMemPtr) {
    T_IpcData *ipcData = (T_IpcData*) (((char *) shMemPtr) - BUF_OFFSET);

    return ipcData->shmId;
}

/**
 * Gets the semaphore set ID for a given shared memory data pointer (JNI method).
 *
 * @param shMemPtr Shared memory data pointer.
 * @return Semaphore set ID.
 */
jint Java_org_apache_ignite_internal_util_ipc_shmem_IpcSharedMemoryUtils_semaphoreId(JNIEnv*, jclass, jlong shMemPtr) {
    T_IpcData *ipcData = (T_IpcData*) (((char *) shMemPtr) - BUF_OFFSET);

    return ipcData->semId;
}
