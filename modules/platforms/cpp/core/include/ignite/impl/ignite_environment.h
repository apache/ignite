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

#ifndef _IGNITE_IMPL_IGNITE_ENVIRONMENT
#define _IGNITE_IMPL_IGNITE_ENVIRONMENT

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>
#include <ignite/jni/utils.h>
#include <ignite/ignite_configuration.h>
#include <ignite/guid.h>

#include <ignite/impl/interop/interop_memory.h>
#include <ignite/impl/binary/binary_type_manager.h>
#include <ignite/impl/handle_registry.h>

namespace ignite
{
    /* Forward declaration. */
    class Ignite;

    namespace impl
    {
        /* Forward declarations. */
        class IgniteEnvironment;
        class IgniteBindingImpl;
        class ModuleManager;
        class ClusterNodesHolder;
        namespace cluster {
            class ClusterNodeImpl;
        }

        typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;

        /**
         * Defines environment in which Ignite operates.
         */
        class IGNITE_IMPORT_EXPORT IgniteEnvironment
        {
            typedef common::concurrent::SharedPointer<cluster::ClusterNodeImpl> SP_ClusterNodeImpl;
        public:
            /**
             * Default memory block allocation size.
             */
            enum { DEFAULT_ALLOCATION_SIZE = 1024 };

            /**
             * Default fast path handle registry containers capasity.
             */
            enum { DEFAULT_FAST_PATH_CONTAINERS_CAP = 1024 };

            /**
            * Default slow path handle registry containers capasity.
            */
            enum { DEFAULT_SLOW_PATH_CONTAINERS_CAP = 16 };

            /**
             * Constructor.
             *
             * @param cfg Node configuration.
             */
            IgniteEnvironment(const IgniteConfiguration& cfg);

            /**
             * Destructor.
             */
            ~IgniteEnvironment();

            /**
             * Get node configuration.
             *
             * @return Node configuration.
             */
            const IgniteConfiguration& GetConfiguration() const;

            /**
             * Populate callback handlers.
             *
             * @param target (current env wrapped into a shared pointer).
             * @return JNI handlers.
             */
            jni::java::JniHandlers GetJniHandlers(common::concurrent::SharedPointer<IgniteEnvironment>* target);

            /**
             * Set context.
             *
             * @param ctx Context.
             */
            void SetContext(common::concurrent::SharedPointer<jni::java::JniContext> ctx);

            /**
             * Perform initialization on successful start.
             */
            void Initialize();

            /**
             * Start callback.
             *
             * @param memPtr Memory pointer.
             * @param proc Processor instance.
             */
            void OnStartCallback(int64_t memPtr, jobject proc);

            /**
             * Continuous query listener apply callback.
             *
             * @param mem Memory with data.
             */
            void OnContinuousQueryListenerApply(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Continuous query filter create callback.
             *
             * @param mem Memory with data.
             * @return Filter handle.
             */
            int64_t OnContinuousQueryFilterCreate(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Continuous query filter apply callback.
             *
             * @param mem Memory with data.
             */
            int64_t OnContinuousQueryFilterApply(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Callback on future result received.
             *
             * @param handle Task handle.
             * @param value Value.
             */
            int64_t OnFuturePrimitiveResult(int64_t handle, int64_t value);

            /**
             * Callback on future result received.
             *
             * @param handle Task handle.
             * @param mem Memory with data.
             */
            int64_t OnFutureObjectResult(int64_t handle, common::concurrent::SharedPointer<interop::InteropMemory> &mem);

            /**
             * Callback on future null result received.
             *
             * @param handle Task handle.
             */
            int64_t OnFutureNullResult(int64_t handle);

            /**
             * Callback on future error received.
             *
             * @param handle Task handle.
             * @param mem Memory with data.
             */
            int64_t OnFutureError(int64_t handle, common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Callback on compute function execute request.
             *
             * @param mem Memory with data.
             */
            int64_t OnComputeFuncExecute(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Cache Invoke callback.
             *
             * @param mem Input-output memory.
             */
            void CacheInvokeCallback(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Get name of Ignite instance.
             *
             * @return Name.
             */
            const char* InstanceName() const;

            /**
             * Get processor associated with the instance.
             *
             * @return Processor.
             */
            void* GetProcessor();

            /**
             * Get JNI context.
             *
             * @return Context.
             */
            jni::java::JniContext* Context();

            /**
             * Get memory for interop operations.
             *
             * @return Memory.
             */
            common::concurrent::SharedPointer<interop::InteropMemory> AllocateMemory();

            /**
             * Get memory chunk for interop operations with desired capacity.
             *
             * @param cap Capacity.
             * @return Memory.
             */
            common::concurrent::SharedPointer<interop::InteropMemory> AllocateMemory(int32_t cap);

            /**
             * Get memory chunk located at the given pointer.
             *
             * @param memPtr Memory pointer.
             * @retrun Memory.
             */
            common::concurrent::SharedPointer<interop::InteropMemory> GetMemory(int64_t memPtr);

            /**
             * Get type manager.
             *
             * @return Type manager.
             */
            binary::BinaryTypeManager* GetTypeManager();

            /**
             * Get type updater.
             *
             * @return Type updater.
             */
            binary::BinaryTypeUpdater* GetTypeUpdater();

            /**
             * Get local cluster node implementation.
             *
             * @return Cluster node implementation or NULL if does not exist.
             */
            SP_ClusterNodeImpl GetLocalNode();

            /**
             * Get cluster node implementation by id.
             *
             * @return Cluster node implementation or NULL if does not exist.
             */
            SP_ClusterNodeImpl GetNode(Guid Id);

            /**
             * Notify processor that Ignite instance has started.
             */
            void ProcessorReleaseStart();

            /**
             * Get handle registry.
             *
             * @return Handle registry.
             */
            HandleRegistry& GetHandleRegistry();

            /**
             * Get binding.
             *
             * @return IgniteBinding instance.
             */
            common::concurrent::SharedPointer<IgniteBindingImpl> GetBinding() const;

            /**
             * Get processor compute.
             *
             * @param proj Projection.
             * @return Processor compute.
             */
            jobject GetProcessorCompute(jobject proj);

            /**
             * Locally execute compute job.
             *
             * @param jobHandle Job handle.
             */
            void ComputeJobExecuteLocal(int64_t jobHandle);

            /**
             * Locally commit job execution result for the task.
             *
             * @param taskHandle Task handle.
             * @param jobHandle Job handle.
             * @return Reduce politics.
             */
            int32_t ComputeTaskLocalJobResult(int64_t taskHandle, int64_t jobHandle);

            /**
             * Reduce compute task.
             *
             * @param taskHandle Task handle.
             */
            void ComputeTaskReduce(int64_t taskHandle);

            /**
             * Complete compute task.
             *
             * @param taskHandle Task handle.
             */
            void ComputeTaskComplete(int64_t taskHandle);

            /**
             * Create compute job.
             *
             * @param mem Memory.
             * @return Job handle.
             */
            int64_t ComputeJobCreate(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Execute compute job.
             *
             * @param mem Memory.
             * @return Job handle.
             */
            void ComputeJobExecute(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Destroy compute job.
             *
             * @param jobHandle Job handle to destroy.
             */
            void ComputeJobDestroy(int64_t jobHandle);

            /**
             * Consume result of remote job execution.
             *
             * @param mem Memory containing result.
             * @return Reduce policy.
             */
            int32_t ComputeTaskJobResult(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Get pointer to ignite node.
             *
             * @return Pointer to ignite node.
             */
            ignite::Ignite* GetIgnite();

            /**
             * InLongOutLong callback.
             * Allow access to private nodes member.
             *
             * @param target Target environment.
             * @param type Operation type.
             * @param val Value.
             */
            friend int64_t IGNITE_CALL InLongOutLong(void* target, int type, int64_t val);

        private:
            /** Node configuration. */
            IgniteConfiguration* cfg;

            /** Context to access Java. */
            common::concurrent::SharedPointer<jni::java::JniContext> ctx;

            /** Startup latch. */
            common::concurrent::SingleLatch latch;

            /** Ignite name. */
            char* name;

            /** Processor instance. */
            jni::JavaGlobalRef proc;

            /** Handle registry. */
            HandleRegistry registry;

            /** Type manager. */
            binary::BinaryTypeManager* metaMgr;

            /** Type updater. */
            binary::BinaryTypeUpdater* metaUpdater;

            /** Ignite binding */
            common::concurrent::SharedPointer<IgniteBindingImpl> binding;

            /** Module manager. */
            common::concurrent::SharedPointer<ModuleManager> moduleMgr;

            /** Cluster nodes. */
            common::concurrent::SharedPointer<ClusterNodesHolder> nodes;

            /** Ignite node. */
            ignite::Ignite* ignite;

            IGNITE_NO_COPY_ASSIGNMENT(IgniteEnvironment);
        };
    }
}

#endif //_IGNITE_IMPL_IGNITE_ENVIRONMENT
