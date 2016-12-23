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

#include <sstream>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
#include <ignite/jni/exports.h>
#include <ignite/jni/java.h>
#include <ignite/jni/utils.h>
#include <ignite/common/utils.h>

#include "ignite/ignition.h"
#include "ignite/impl/ignite_environment.h"
#include "ignite/impl/ignite_impl.h"

using namespace ignite::common;
using namespace ignite::common::concurrent;

using namespace ignite::jni;
using namespace ignite::jni::java;

using namespace ignite::impl;

namespace ignite
{
    /** Default configuration. */
    const char* DFLT_CFG = "config/default-config.xml";

    /** Whether JVM library is loaded to the process. */
    bool JVM_LIB_LOADED;

    /** Critical section for factory methods. */
    CriticalSection factoryLock;

    /** Flag indicating that at least one Ignite instance has started. */
    bool started = false;

    /**
     * Convert integer value to string.
     */
    std::string JvmMemoryString(const std::string& prefix, int32_t val)
    {
        std::ostringstream ss;
        ss << val;

        std::string valStr = ss.str();

        std::string res = std::string(prefix);
        res.append(valStr);
        res.append("m");

        return res;
    }

    /**
     * Create JVM options.
     *
     * @param cfg Configuration.
     * @param home Optional GG home.
     * @param cp Classpath.
     * @param opts Options.
     * @param optsLen Options length.
     * @return Options.
     */
    char** CreateJvmOptions(const IgniteConfiguration& cfg, const std::string* home, const std::string& cp, int* optsLen)
    {
        *optsLen = 3 + (home ? 1 : 0) + static_cast<int>(cfg.jvmOpts.size());
        char** opts = new char*[*optsLen];

        int idx = 0;

        // 1. Set classpath.
        std::string cpFull = std::string("-Djava.class.path=") + cp;

        *(opts + idx++) = CopyChars(cpFull.c_str());

        // 2. Set home.
        if (home) {
            std::string homeFull = std::string("-DIGNITE_HOME=") + *home;

            *(opts + idx++) = CopyChars(homeFull.c_str());
        }

        // 3. Set Xms, Xmx.
        std::string xmsStr = JvmMemoryString(std::string("-Xms"), cfg.jvmInitMem);
        std::string xmxStr = JvmMemoryString(std::string("-Xmx"), cfg.jvmMaxMem);

        *(opts + idx++) = CopyChars(xmsStr.c_str());
        *(opts + idx++) = CopyChars(xmxStr.c_str());

        // 4. Set the rest options.
        for (std::list<std::string>::const_iterator i = cfg.jvmOpts.begin(); i != cfg.jvmOpts.end(); ++i) {
            char* optCopy = CopyChars(i->c_str());

            opts[idx++] = optCopy;
        }

        return opts;
    }

    Ignite Ignition::Start(const IgniteConfiguration& cfg)
    {
        return Start(cfg, static_cast<const char*>(NULL));
    }

    Ignite Ignition::Start(const IgniteConfiguration& cfg, IgniteError* err)
    {
        return Start(cfg, NULL, err);
    }

    Ignite Ignition::Start(const IgniteConfiguration& cfg, const char* name)
    {
        IgniteError err;

        Ignite res = Start(cfg, name, &err);

        IgniteError::ThrowIfNeeded(err);

        return res;
    }

    Ignite Ignition::Start(const IgniteConfiguration& cfg, const char* name, IgniteError* err)
    {
        bool failed = false;

        SharedPointer<IgniteEnvironment> env;
        SharedPointer<IgniteEnvironment>* envTarget = NULL;

        jobject javaRef = NULL;

        factoryLock.Enter();

        // 1. Load JVM library if needed.
        if (!JVM_LIB_LOADED)
        {
            bool jvmLibFound;
            std::string jvmLib;

            if (!cfg.jvmLibPath.empty())
            {
                std::string jvmLibPath = std::string(cfg.jvmLibPath);

                jvmLib = FindJvmLibrary(&jvmLibPath, &jvmLibFound);
            }
            else
                jvmLib = FindJvmLibrary(NULL, &jvmLibFound);

            if (!jvmLibFound)
            {
                *err = IgniteError(IgniteError::IGNITE_ERR_JVM_LIB_NOT_FOUND,
                    "JVM library is not found (did you set JAVA_HOME environment variable?)");

                failed = true;
            }

            if (!failed) {
                if (!LoadJvmLibrary(jvmLib))
                {
                    *err = IgniteError(IgniteError::IGNITE_ERR_JVM_LIB_LOAD_FAILED, "Failed to load JVM library.");

                    failed = true;
                }
            }

            JVM_LIB_LOADED = true;
        }

        if (!failed)
        {
            // 2. Resolve IGNITE_HOME.
            bool homeFound;
            std::string home;

            if (!cfg.igniteHome.empty())
            {
                std::string homePath = std::string(cfg.igniteHome);

                home = ResolveIgniteHome(&homePath, &homeFound);
            }
            else
                home = ResolveIgniteHome(NULL, &homeFound);

            // 3. Create classpath.
            std::string cp;

            if (!cfg.jvmClassPath.empty())
            {
                std::string usrCp = cfg.jvmClassPath;

                cp = CreateIgniteClasspath(&usrCp, homeFound ? &home : NULL);
            }
            else
                cp = CreateIgniteClasspath(NULL, homeFound ? &home : NULL);

            if (!cp.empty())
            {
                // 4. Start JVM if needed.
                JniErrorInfo jniErr;

                env = SharedPointer<IgniteEnvironment>(new IgniteEnvironment());

                int optsLen;
                char** opts = CreateJvmOptions(cfg, homeFound ? &home : NULL, cp, &optsLen);

                envTarget = new SharedPointer<IgniteEnvironment>(env);

                SharedPointer<JniContext> ctx(
                    JniContext::Create(opts, optsLen, env.Get()->GetJniHandlers(envTarget), &jniErr));

                for (int i = 0; i < optsLen; i++)
                    ReleaseChars(*(opts + i));

                delete[] opts;

                if (!ctx.Get())
                {
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    failed = true;
                }

                env.Get()->SetContext(ctx);

                // 5. Start Ignite.
                if (!failed)
                {
                    char* springCfgPath0 = NULL;

                    if (!cfg.springCfgPath.empty())
                        springCfgPath0 = CopyChars(cfg.springCfgPath.c_str());
                    else
                        springCfgPath0 = CopyChars(DFLT_CFG);

                    char* name0 = CopyChars(name);

                    interop::InteropUnpooledMemory mem(16);
                    interop::InteropOutputStream stream(&mem);
                    stream.WriteBool(false);
                    stream.Synchronize();

                    javaRef = ctx.Get()->IgnitionStart(springCfgPath0, name0, 2, mem.PointerLong(), &jniErr);

                    ReleaseChars(springCfgPath0);
                    ReleaseChars(name0);

                    if (!javaRef) {
                        IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                        failed = true;
                    }
                    else {
                        // 6. Ignite is started at this point.
                        env.Get()->Initialize();

                        started = true;
                    }
                }
            }
            else {
                *err = IgniteError(IgniteError::IGNITE_ERR_JVM_NO_CLASSPATH,
                    "Java classpath is empty (did you set IGNITE_HOME environment variable?)");

                failed = true;
            }
        }

        factoryLock.Leave();

        if (failed) 
            return Ignite();

        env.Get()->ProcessorReleaseStart();

        IgniteImpl* impl = new IgniteImpl(env, javaRef);

        return Ignite(impl);
    }

    Ignite Ignition::Get()
    {
        return Get(static_cast<const char*>(NULL));
    }

    Ignite Ignition::Get(IgniteError* err)
    {
        return Get(NULL, err);
    }

    Ignite Ignition::Get(const char* name)
    {
        IgniteError err;

        Ignite res = Get(name, &err);

        IgniteError::ThrowIfNeeded(err);

        return res;
    }

    Ignite Ignition::Get(const char* name, IgniteError* err)
    {
        Ignite res;

        factoryLock.Enter();

        if (started)
        {
            char* name0 = CopyChars(name);

            // 1. Create context for this operation.
            JniErrorInfo jniErr;

            SharedPointer<JniContext> ctx(JniContext::Create(NULL, 0, JniHandlers(), &jniErr));

            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            if (err->GetCode() == IgniteError::IGNITE_SUCCESS)
            {
                // 2. Get environment pointer.
                long long ptr = ctx.Get()->IgnitionEnvironmentPointer(name0, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (err->GetCode() == IgniteError::IGNITE_SUCCESS)
                {
                    if (ptr != 0)
                    {
                        // 3. Obtain real environment for this instance.
                        JniHandlers* hnds = reinterpret_cast<JniHandlers*>(ptr);

                        SharedPointer<IgniteEnvironment>* env =
                            static_cast<SharedPointer<IgniteEnvironment>*>(hnds->target);

                        // 4. Get fresh node reference.
                        jobject ref = ctx.Get()->IgnitionInstance(name0, &jniErr);

                        if (err->GetCode() == IgniteError::IGNITE_SUCCESS) {
                            if (ref)
                            {
                                IgniteImpl* impl = new IgniteImpl(*env, ref);

                                res = Ignite(impl);
                            }
                            else
                                // Error: concurrent node stop.
                                *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                    "Failed to get Ignite instance because it was stopped concurrently.");

                        }
                    }
                    else
                        // Error: no node with the given name.
                        *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Failed to get Ignite instance because it is either not started yet or already stopped.");
                }
            }

            ReleaseChars(name0);
        }
        else
            // Error: no node with the given name.
            *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                "Failed to get Ignite instance because it is either not started yet or already stopped.");

        factoryLock.Leave();

        return res;
    }

    bool Ignition::Stop(bool cancel)
    {
        return Stop(NULL, cancel);
    }

    bool Ignition::Stop(bool cancel, IgniteError* err)
    {
        return Stop(NULL, cancel, err);
    }

    bool Ignition::Stop(const char* name, bool cancel)
    {
        IgniteError err;

        bool res = Stop(name, cancel, &err);

        IgniteError::ThrowIfNeeded(err);

        return res;
    }

    bool Ignition::Stop(const char* name, bool cancel, IgniteError* err)
    {
        bool res = false;

        factoryLock.Enter();

        if (started)
        {
            JniErrorInfo jniErr;

            SharedPointer<JniContext> ctx(JniContext::Create(NULL, 0, JniHandlers(), &jniErr));

            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            if (err->GetCode() == IgniteError::IGNITE_SUCCESS)
            {
                char* name0 = CopyChars(name);

                bool res0 = ctx.Get()->IgnitionStop(name0, cancel, &jniErr);

                ReleaseChars(name0);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (err->GetCode() == IgniteError::IGNITE_SUCCESS)
                    res = res0;
            }
        }

        factoryLock.Leave();

        return res;
    }

    void Ignition::StopAll(bool cancel)
    {
        IgniteError err;

        StopAll(cancel, &err);

        IgniteError::ThrowIfNeeded(err);
    }

    void Ignition::StopAll(bool cancel, IgniteError* err)
    {
        factoryLock.Enter();

        if (started)
        {
            JniErrorInfo jniErr;

            SharedPointer<JniContext> ctx(JniContext::Create(NULL, 0, JniHandlers(), &jniErr));

            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            if (err->GetCode() == IgniteError::IGNITE_SUCCESS)
            {
                ctx.Get()->IgnitionStopAll(cancel, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);
            }
        }

        factoryLock.Leave();
    }
}
