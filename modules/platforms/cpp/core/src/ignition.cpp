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
#include <memory>

#include <ignite/common/concurrent.h>
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

        ss << prefix << val << 'm';

        return ss.str();
    }

    /**
     * JVM options
     *
     * Used to build JVM options in a exception-safe way.
     */
    class JvmOptions
    {
    public:
        /**
         * Constructor.
         */
        JvmOptions() :
            opts()
        {
            // No-op.
        }

        /**
         * Destructor.
         */
        ~JvmOptions()
        {
            Deinit();
        }

        /**
         * Create JVM options from configuration.
         *
         * @param cfg Configuration.
         * @param home Optional GG home.
         * @param cp Classpath.
         */
        void FromConfiguration(const IgniteConfiguration& cfg, const std::string& home, const std::string& cp)
        {
            Deinit();

            const size_t REQ_OPTS_CNT = 4;
            const size_t JAVA9_OPTS_CNT = 6;

            opts.reserve(cfg.jvmOpts.size() + REQ_OPTS_CNT + JAVA9_OPTS_CNT);

            std::string fileEncParam = "-Dfile.encoding=";

            bool hadFileEnc = false;

            // 1. Set classpath.
            std::string cpFull = "-Djava.class.path=" + cp;

            opts.push_back(CopyChars(cpFull.c_str()));

            // 2. Set home.
            if (!home.empty()) {
                std::string homeFull = "-DIGNITE_HOME=" + home;

                opts.push_back(CopyChars(homeFull.c_str()));
            }

            // 3. Set Xms, Xmx.
            std::string xmsStr = JvmMemoryString("-Xms", cfg.jvmInitMem);
            std::string xmxStr = JvmMemoryString("-Xmx", cfg.jvmMaxMem);

            opts.push_back(CopyChars(xmsStr.c_str()));
            opts.push_back(CopyChars(xmxStr.c_str()));

            // 4. Set the rest options.
            for (std::list<std::string>::const_iterator i = cfg.jvmOpts.begin(); i != cfg.jvmOpts.end(); ++i) {
                if (i->find(fileEncParam) != std::string::npos)
                    hadFileEnc = true;

                opts.push_back(CopyChars(i->c_str()));
            }

            // 5. Set file.encoding.
            if (!hadFileEnc) {
                std::string fileEncFull = fileEncParam + "UTF-8";

                opts.push_back(CopyChars(fileEncFull.c_str()));
            }

            // Adding options for Java 9 or later
            if (IsJava9OrLater()) {
                opts.push_back(CopyChars("--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED"));
                opts.push_back(CopyChars("--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"));
                opts.push_back(CopyChars("--add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED"));
                opts.push_back(CopyChars("--add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED"));
                opts.push_back(CopyChars("--add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED"));
                opts.push_back(CopyChars("--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED"));
                opts.push_back(CopyChars("--illegal-access=permit"));
            }
        }

        /**
         * Deallocates all allocated data.
         */
        void Deinit()
        {
            for (size_t i = 0; i < opts.size(); ++i)
                ReleaseChars(opts[i]);

            opts.clear();
        }

        /**
         * Get built options.
         *
         * @return Built options
         */
        char** GetOpts()
        {
            return &opts[0];
        }

        /**
         * Get options number.
         *
         * @return Options number.
         */
        int GetSize() const
        {
            return static_cast<int>(opts.size());
        }

    private:
        /** Options array. */
        std::vector<char*> opts;
    };

    Ignite Ignition::Start(const IgniteConfiguration& cfg)
    {
        return Start(cfg, static_cast<const char*>(0));
    }

    Ignite Ignition::Start(const IgniteConfiguration& cfg, IgniteError& err)
    {
        return Start(cfg, 0, err);
    }

    Ignite Ignition::Start(const IgniteConfiguration& cfg, const char* name)
    {
        IgniteError err;

        Ignite res = Start(cfg, name, err);

        IgniteError::ThrowIfNeeded(err);

        return res;
    }

    Ignite Ignition::Start(const IgniteConfiguration& cfg, const char* name, IgniteError& err)
    {
        CsLockGuard guard(factoryLock);

        std::string jvmLib = FindJvmLibrary(cfg.jvmLibPath);

        // 1. Load JVM library if needed.
        if (!JVM_LIB_LOADED)
        {
            if (jvmLib.empty())
            {
                err = IgniteError(IgniteError::IGNITE_ERR_JVM_LIB_NOT_FOUND,
                    "JVM library is not found (did you set JAVA_HOME environment variable?)");

                return Ignite();
            }

            if (!LoadJvmLibrary(jvmLib))
            {
                err = IgniteError(IgniteError::IGNITE_ERR_JVM_LIB_LOAD_FAILED, "Failed to load JVM library.");

                return Ignite();
            }

            JVM_LIB_LOADED = true;
        }

        // 2. Resolve IGNITE_HOME.
        std::string home = ResolveIgniteHome(cfg.igniteHome);

        // 3. Create classpath.
        std::string cp = CreateIgniteClasspath(cfg.jvmClassPath, home);

        if (cp.empty())
        {
            err = IgniteError(IgniteError::IGNITE_ERR_JVM_NO_CLASSPATH,
                "Java classpath is empty (did you set IGNITE_HOME environment variable?)");

            return Ignite();
        }

        // 4. Resolving spring config path
        std::string springCfgPath0;

        if (!cfg.springCfgPath.empty())
            springCfgPath0 = cfg.springCfgPath;
        else
            springCfgPath0 = DFLT_CFG;

        // 5. Start JVM if needed.
        IgniteConfiguration cfg0(cfg);

        cfg0.jvmLibPath = jvmLib;
        cfg0.igniteHome = home;
        cfg0.jvmClassPath = cp;
        cfg0.springCfgPath = springCfgPath0;

        JniErrorInfo jniErr;

        JvmOptions opts;
        opts.FromConfiguration(cfg, home, cp);

        // This is the instance that allows us keep IgniteEnvironment alive
        // till the end of the method call
        SharedPointer<IgniteEnvironment> env = SharedPointer<IgniteEnvironment>(new IgniteEnvironment(cfg0));

        // This is the instance with manual control over lifetime which is we
        // going to pass to Java if Java object is constructed and initialized
        // successfully.
        std::auto_ptr< SharedPointer<IgniteEnvironment> > envGuard(new SharedPointer<IgniteEnvironment>(env));

        SharedPointer<JniContext> ctx(
            JniContext::Create(opts.GetOpts(), opts.GetSize(), env.Get()->GetJniHandlers(envGuard.get()), &jniErr));

        if (!ctx.Get())
        {
            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            return Ignite();
        }

        env.Get()->SetContext(ctx);

        // 6. Start Ignite.

        // Workaround for nullable strings as we can't use unique_ptr nor
        // can we construct std::string from the null-pointer.
        char* namep = 0;

        std::string name0;
        if (name)
        {
            name0 = name;

            namep = &name0[0];
        }

        interop::InteropUnpooledMemory mem(16);
        interop::InteropOutputStream stream(&mem);
        stream.WriteBool(false);
        stream.Synchronize();

        ctx.Get()->IgnitionStart(&springCfgPath0[0], namep, 2, mem.PointerLong(), &jniErr);

        if (!env.Get()->GetProcessor() || jniErr.code != java::IGNITE_JNI_ERR_SUCCESS)
        {
            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            return Ignite();
        }

        // 7. Ignite is started at this point.
        env.Get()->Initialize();

        started = true;

        guard.Reset();

        // We successfully created Java Ignite instance by this point, so we
        // give control over C++ instance to Java. Still, we keep holding env
        // instance so it will keep living till the end of the scope.
        envGuard.release();

        env.Get()->ProcessorReleaseStart();

        IgniteImpl* impl = new IgniteImpl(env);

        return Ignite(impl);
    }

    Ignite Ignition::Get()
    {
        return Get(static_cast<const char*>(0));
    }

    Ignite Ignition::Get(IgniteError& err)
    {
        return Get(0, err);
    }

    Ignite Ignition::Get(const char* name)
    {
        IgniteError err;

        Ignite res = Get(name, err);

        IgniteError::ThrowIfNeeded(err);

        return res;
    }

    Ignite Ignition::Get(const char* name, IgniteError& err)
    {
        Ignite res;

        factoryLock.Enter();

        if (started)
        {
            char* name0 = CopyChars(name);

            // 1. Create context for this operation.
            JniErrorInfo jniErr;

            SharedPointer<JniContext> ctx(JniContext::Create(0, 0, JniHandlers(), &jniErr));

            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
            {
                // 2. Get environment pointer.
                int64_t ptr = ctx.Get()->IgnitionEnvironmentPointer(name0, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                {
                    if (ptr != 0)
                    {
                        // 3. Obtain real environment for this instance.
                        JniHandlers* hnds = reinterpret_cast<JniHandlers*>(ptr);

                        SharedPointer<IgniteEnvironment>* env =
                            static_cast<SharedPointer<IgniteEnvironment>*>(hnds->target);

                        IgniteImpl* impl = new IgniteImpl(*env);

                        res = Ignite(impl);
                    }
                    else
                        // Error: no node with the given name.
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Failed to get Ignite instance because it is either not started yet or already stopped.");
                }
            }

            ReleaseChars(name0);
        }
        else
            // Error: no node with the given name.
            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                "Failed to get Ignite instance because it is either not started yet or already stopped.");

        factoryLock.Leave();

        return res;
    }

    bool Ignition::Stop(bool cancel)
    {
        return Stop(0, cancel);
    }

    bool Ignition::Stop(bool cancel, IgniteError& err)
    {
        return Stop(0, cancel, err);
    }

    bool Ignition::Stop(const char* name, bool cancel)
    {
        IgniteError err;

        bool res = Stop(name, cancel, err);

        IgniteError::ThrowIfNeeded(err);

        return res;
    }

    bool Ignition::Stop(const char* name, bool cancel, IgniteError& err)
    {
        bool res = false;

        factoryLock.Enter();

        if (started)
        {
            JniErrorInfo jniErr;

            SharedPointer<JniContext> ctx(JniContext::Create(0, 0, JniHandlers(), &jniErr));

            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
            {
                char* name0 = CopyChars(name);

                bool res0 = ctx.Get()->IgnitionStop(name0, cancel, &jniErr);

                ReleaseChars(name0);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                    res = res0;
            }
        }

        factoryLock.Leave();

        return res;
    }

    void Ignition::StopAll(bool cancel)
    {
        IgniteError err;

        StopAll(cancel, err);

        IgniteError::ThrowIfNeeded(err);
    }

    void Ignition::StopAll(bool cancel, IgniteError& err)
    {
        factoryLock.Enter();

        if (started)
        {
            JniErrorInfo jniErr;

            SharedPointer<JniContext> ctx(JniContext::Create(0, 0, JniHandlers(), &jniErr));

            IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

            if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
            {
                ctx.Get()->IgnitionStopAll(cancel, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);
            }
        }

        factoryLock.Leave();
    }
}
