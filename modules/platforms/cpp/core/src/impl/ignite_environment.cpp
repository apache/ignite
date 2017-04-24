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

#include <ignite/impl/interop/interop_external_memory.h>
#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_type_updater_impl.h>
#include <ignite/impl/module_manager.h>
#include <ignite/impl/ignite_binding_impl.h>

#include <ignite/binary/binary.h>
#include <ignite/cache/query/continuous/continuous_query.h>
#include <ignite/ignite_binding.h>
#include <ignite/ignite_binding_context.h>

#include <ignite/impl/ignite_environment.h>

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::binary;
using namespace ignite::impl::cache::query::continuous;

namespace ignite
{
    namespace impl
    {
        /**
         * Callback codes.
         */
        struct OperationCallback
        {
            enum Type
            {
                CACHE_INVOKE = 8,
                CONTINUOUS_QUERY_LISTENER_APPLY = 18,
                CONTINUOUS_QUERY_FILTER_CREATE = 19,
                CONTINUOUS_QUERY_FILTER_APPLY = 20,
                CONTINUOUS_QUERY_FILTER_RELEASE = 21,
                REALLOC = 36,
                ON_START = 49,
                ON_STOP = 50 
            };
        };

        /**
         * InLongOutLong callback.
         * 
         * @param target Target environment.
         * @param type Operation type.
         * @param val Value.
         */
        long long IGNITE_CALL InLongOutLong(void* target, int type, long long val)
        {
            int64_t res = 0;
            SharedPointer<IgniteEnvironment>* env = static_cast<SharedPointer<IgniteEnvironment>*>(target);

            switch (type)
            {
                case OperationCallback::ON_STOP:
                {
                    delete env;

                    break;
                }

                case OperationCallback::CONTINUOUS_QUERY_LISTENER_APPLY:
                {
                    SharedPointer<InteropMemory> mem = env->Get()->GetMemory(val);

                    env->Get()->OnContinuousQueryListenerApply(mem);

                    break;
                }

                case OperationCallback::CONTINUOUS_QUERY_FILTER_CREATE:
                {
                    SharedPointer<InteropMemory> mem = env->Get()->GetMemory(val);

                    res = env->Get()->OnContinuousQueryFilterCreate(mem);

                    break;
                }

                case OperationCallback::CONTINUOUS_QUERY_FILTER_APPLY:
                {
                    SharedPointer<InteropMemory> mem = env->Get()->GetMemory(val);

                    res = env->Get()->OnContinuousQueryFilterApply(mem);

                    break;
                }

                case OperationCallback::CONTINUOUS_QUERY_FILTER_RELEASE:
                {
                    // No-op.
                    break;
                }

                case OperationCallback::CACHE_INVOKE:
                {
                    SharedPointer<InteropMemory> mem = env->Get()->GetMemory(val);

                    env->Get()->CacheInvokeCallback(mem);

                    break;
                }

                default:
                {
                    break;
                }
            }

            return res;
        }

        /**
         * InLongOutLong callback.
         * 
         * @param target Target environment.
         * @param type Operation type.
         * @param val1 Value1.
         * @param val2 Value2.
         * @param val3 Value3.
         * @param arg Object arg.
         */
        long long IGNITE_CALL InLongLongLongObjectOutLong(void* target, int type, long long val1, long long val2, 
            long long val3, void* arg)
        {
            SharedPointer<IgniteEnvironment>* env = static_cast<SharedPointer<IgniteEnvironment>*>(target);

            switch (type)
            {
                case OperationCallback::ON_START:
                {
                    env->Get()->OnStartCallback(val1, reinterpret_cast<jobject>(arg));

                    break;
                }

                case OperationCallback::REALLOC:
                {
                    SharedPointer<InteropMemory> mem = env->Get()->GetMemory(val1);

                    mem.Get()->Reallocate(static_cast<int32_t>(val2));

                    break;
                }

                default:
                {
                    break;
                }
            }

            return 0;
        }

        IgniteEnvironment::IgniteEnvironment(const IgniteConfiguration& cfg) :
            cfg(new IgniteConfiguration(cfg)),
            ctx(SharedPointer<JniContext>()),
            latch(),
            name(0),
            proc(),
            registry(DEFAULT_FAST_PATH_CONTAINERS_CAP, DEFAULT_SLOW_PATH_CONTAINERS_CAP),
            metaMgr(new BinaryTypeManager()),
            metaUpdater(0),
            binding(),
            moduleMgr()
        {
            binding = SharedPointer<IgniteBindingImpl>(new IgniteBindingImpl(*this));

            IgniteBindingContext bindingContext(cfg, GetBinding());

            moduleMgr = SharedPointer<ModuleManager>(new ModuleManager(bindingContext));
        }

        IgniteEnvironment::~IgniteEnvironment()
        {
            delete[] name;

            delete metaUpdater;
            delete metaMgr;
            delete cfg;
        }

        const IgniteConfiguration& IgniteEnvironment::GetConfiguration() const
        {
            return *cfg;
        }

        JniHandlers IgniteEnvironment::GetJniHandlers(SharedPointer<IgniteEnvironment>* target)
        {
            JniHandlers hnds;

            hnds.target = target;

            hnds.inLongOutLong = InLongOutLong;
            hnds.inLongLongLongObjectOutLong = InLongLongLongObjectOutLong;

            hnds.error = 0;

            return hnds;
        }

        void IgniteEnvironment::SetContext(SharedPointer<JniContext> ctx)
        {
            this->ctx = ctx;
        }

        void IgniteEnvironment::Initialize()
        {
            latch.CountDown();

            jobject binaryProc = Context()->ProcessorBinaryProcessor(proc.Get());
            metaUpdater = new BinaryTypeUpdaterImpl(*this, binaryProc);

            metaMgr->SetUpdater(metaUpdater);

            common::dynamic::Module currentModule = common::dynamic::GetCurrent();
            moduleMgr.Get()->RegisterModule(currentModule);
        }

        const char* IgniteEnvironment::InstanceName() const
        {
            return name;
        }

        void* IgniteEnvironment::GetProcessor()
        {
            return (void*)proc.Get();
        }

        JniContext* IgniteEnvironment::Context()
        {
            return ctx.Get();
        }

        SharedPointer<InteropMemory> IgniteEnvironment::AllocateMemory()
        {
            SharedPointer<InteropMemory> ptr(new InteropUnpooledMemory(DEFAULT_ALLOCATION_SIZE));

            return ptr;
        }

        SharedPointer<InteropMemory> IgniteEnvironment::AllocateMemory(int32_t cap)
        {
            SharedPointer<InteropMemory> ptr(new InteropUnpooledMemory(cap));

            return ptr;
        }

        SharedPointer<InteropMemory> IgniteEnvironment::GetMemory(int64_t memPtr)
        {
            int8_t* memPtr0 = reinterpret_cast<int8_t*>(memPtr);

            int32_t flags = InteropMemory::Flags(memPtr0);

            if (InteropMemory::IsExternal(flags))
            {
                SharedPointer<InteropMemory> ptr(new InteropExternalMemory(memPtr0));

                return ptr;
            }
            else
            {
                SharedPointer<InteropMemory> ptr(new InteropUnpooledMemory(memPtr0));

                return ptr;
            }
        }

        BinaryTypeManager* IgniteEnvironment::GetTypeManager()
        {
            return metaMgr;
        }

        BinaryTypeUpdater* IgniteEnvironment::GetTypeUpdater()
        {
            return metaUpdater;
        }

        SharedPointer<IgniteBindingImpl> IgniteEnvironment::GetBinding() const
        {
            return binding;
        }

        void IgniteEnvironment::ProcessorReleaseStart()
        {
            if (proc.Get())
                ctx.Get()->ProcessorReleaseStart(proc.Get());
        }

        HandleRegistry& IgniteEnvironment::GetHandleRegistry()
        {
            return registry;
        }

        void IgniteEnvironment::OnStartCallback(long long memPtr, jobject proc)
        {
            this->proc = jni::JavaGlobalRef(*ctx.Get(), proc);

            InteropExternalMemory mem(reinterpret_cast<int8_t*>(memPtr));
            InteropInputStream stream(&mem);

            BinaryReaderImpl reader(&stream);

            int32_t nameLen = reader.ReadString(0, 0);

            if (nameLen >= 0)
            {
                name = new char[nameLen + 1];
                reader.ReadString(name, nameLen + 1);
            }
            else
                name = 0;
        }

        void IgniteEnvironment::OnContinuousQueryListenerApply(SharedPointer<InteropMemory>& mem)
        {
            InteropInputStream stream(mem.Get());
            BinaryReaderImpl reader(&stream);

            int64_t qryHandle = reader.ReadInt64();

            ContinuousQueryImplBase* contQry = reinterpret_cast<ContinuousQueryImplBase*>(registry.Get(qryHandle).Get());

            if (contQry)
            {
                BinaryRawReader rawReader(&reader);

                contQry->ReadAndProcessEvents(rawReader);
            }
        }

        int64_t IgniteEnvironment::OnContinuousQueryFilterCreate(SharedPointer<InteropMemory>& mem)
        {
            if (!binding.Get())
                throw IgniteError(IgniteError::IGNITE_ERR_UNKNOWN, "IgniteBinding is not initialized.");

            InteropInputStream inStream(mem.Get());
            BinaryReaderImpl reader(&inStream);

            InteropOutputStream outStream(mem.Get());
            BinaryWriterImpl writer(&outStream, GetTypeManager());

            BinaryObjectImpl binFilter = BinaryObjectImpl::FromMemory(*mem.Get(), inStream.Position(), metaMgr);

            int32_t filterId = binFilter.GetTypeId();

            bool invoked = false;

            int64_t res = binding.Get()->InvokeCallback(invoked,
                IgniteBindingImpl::CallbackType::CACHE_ENTRY_FILTER_CREATE, filterId, reader, writer);

            if (!invoked)
            {
                IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_COMPUTE_USER_UNDECLARED_EXCEPTION,
                    "C++ remote filter is not registered on the node (did you compile your program without -rdynamic?).",
                    "filterId", filterId);
            }

            outStream.Synchronize();

            return res;
        }

        int64_t IgniteEnvironment::OnContinuousQueryFilterApply(SharedPointer<InteropMemory>& mem)
        {
            InteropInputStream inStream(mem.Get());
            BinaryReaderImpl reader(&inStream);
            BinaryRawReader rawReader(&reader);

            int64_t handle = rawReader.ReadInt64();

            SharedPointer<ContinuousQueryImplBase> qry =
                StaticPointerCast<ContinuousQueryImplBase>(registry.Get(handle));

            if (!qry.Get())
                IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_GENERIC, "Null query for handle.", "handle", handle);

            cache::event::CacheEntryEventFilterBase* filter = qry.Get()->GetFilterHolder().GetFilter();

            if (!filter)
                IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_GENERIC, "Null filter for handle.", "handle", handle);

            bool res = filter->ReadAndProcessEvent(rawReader);

            return res ? 1 : 0;
        }

        void IgniteEnvironment::CacheInvokeCallback(SharedPointer<InteropMemory>& mem)
        {
            if (!binding.Get())
                throw IgniteError(IgniteError::IGNITE_ERR_UNKNOWN, "IgniteBinding is not initialized.");

            InteropInputStream inStream(mem.Get());
            BinaryReaderImpl reader(&inStream);

            InteropOutputStream outStream(mem.Get());
            BinaryWriterImpl writer(&outStream, GetTypeManager());

            bool local = reader.ReadBool();

            if (local)
                throw IgniteError(IgniteError::IGNITE_ERR_UNSUPPORTED_OPERATION, "Local invokation is not supported.");

            BinaryObjectImpl binProcHolder = BinaryObjectImpl::FromMemory(*mem.Get(), inStream.Position(), 0);
            BinaryObjectImpl binProc = binProcHolder.GetField(0);

            int32_t procId = binProc.GetTypeId();

            bool invoked = false;

            binding.Get()->InvokeCallback(invoked,
                IgniteBindingImpl::CallbackType::CACHE_ENTRY_PROCESSOR_APPLY, procId, reader, writer);

            if (!invoked)
            {
                IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_COMPUTE_USER_UNDECLARED_EXCEPTION,
                    "C++ entry processor is not registered on the node (did you compile your program without -rdynamic?).",
                    "procId", procId);
            }

            outStream.Synchronize();
        }
    }
}
