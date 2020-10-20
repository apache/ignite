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

#ifndef _IGNITE_IMPL_INTEROP_INTEROP_TARGET
#define _IGNITE_IMPL_INTEROP_INTEROP_TARGET

#include <ignite/impl/ignite_environment.h>
#include <ignite/impl/operations.h>

namespace ignite
{    
    namespace impl 
    {
        namespace interop
        {
            /**
             * Interop target.
             */
            class IGNITE_IMPORT_EXPORT InteropTarget
            {
            public:
                /**
                 * Operation result.
                 */
                struct OperationResult
                {
                    enum Type
                    {
                        /** Null. */
                        AI_NULL = 0,

                        /** Success. */
                        AI_SUCCESS = 1,

                        /** Error. */
                        AI_ERROR = -1
                    };
                };

                /**
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                InteropTarget(ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);

                /**
                * Constructor used to create new instance.
                *
                * @param env Environment.
                * @param javaRef Reference to java object.
                * @param javaRef Whether javaRef release in destructor should be skipped.
                */
                InteropTarget(ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef, 
                    bool skipJavaRefRelease);

                /**
                 * Destructor.
                 */
                virtual ~InteropTarget();

                /**
                 * Internal out operation.
                 *
                 * @param opType Operation type.
                 * @param inMem Input memory.
                 * @param err Error.
                 * @return Result.
                 */
                bool OutOp(int32_t opType, InteropMemory& inMem, IgniteError& err);

                /**
                 * Internal out operation.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result.
                 */
                bool OutOp(int32_t opType, InputOperation& inOp, IgniteError& err);

                /**
                 * Internal out operation.
                 *
                 * @param opType Operation type.
                 * @param err Error.
                 * @return Result.
                 */
                bool OutOp(int32_t opType, IgniteError& err);

                /**
                 * Internal in operation.
                 *
                 * @param opType Operation type.
                 * @param outOp Output.
                 * @param err Error.
                 * @return Result.
                 */
                bool InOp(int32_t opType, OutputOperation& outOp, IgniteError& err);

                /**
                 * Internal in Object operation.
                 *
                 * @param opType Operation type.
                 * @param err Error.
                 * @return Object.
                 */
                jobject InOpObject(int32_t opType, IgniteError& err);

                /**
                 * Internal out-in operation.
                 * Uses two independent memory pieces to write and read data.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void OutInOp(int32_t opType, InputOperation& inOp, OutputOperation& outOp, IgniteError& err);

                /**
                 * Internal out-in operation.
                 * Uses single memory piece to write and read data.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void OutInOpX(int32_t opType, InputOperation& inOp, OutputOperation& outOp, IgniteError& err);

                /**
                 * In stream out long operation.
                 *
                 * @param opType Type of operation.
                 * @param outInMem Input and output memory.
                 * @param err Error.
                 * @return Operation result.
                 */
                OperationResult::Type InStreamOutLong(int32_t opType, InteropMemory& outInMem, IgniteError& err);

                /**
                 * In stream out long operation.
                 *
                 * @param opType Type of operation.
                 * @param inOp Input opeartion.
                 * @param err Error.
                 * @return Operation result or long value.
                 */
                int64_t InStreamOutLong(int32_t opType, InputOperation& inOp, IgniteError& err);

                /**
                 * In stream out object operation.
                 *
                 * @param opType Type of operation.
                 * @param outInMem Input and output memory.
                 * @param err Error.
                 * @return Java object references.
                 */
                jobject InStreamOutObject(int32_t opType, InteropMemory& outInMem, IgniteError& err);

                /**
                 * In stream out stream operation.
                 *
                 * @param opType Type of operation.
                 * @param inMem Input memory.
                 * @param outMem Output memory.
                 * @param err Error.
                 */
                 void InStreamOutStream(int32_t opType, InteropMemory& inMem, InteropMemory& outMem, IgniteError& err);

                /**
                * Internal out-in operation.
                *
                * @param opType Operation type.
                * @param val Value.
                * @param err Error.
                */
                int64_t OutInOpLong(int32_t opType, int64_t val, IgniteError& err);

                /**
                 * Get environment shared pointer.
                 *
                 * @return Environment shared pointer.
                 */
                ignite::common::concurrent::SharedPointer<IgniteEnvironment> GetEnvironmentPointer()
                {
                    return env;
                }

            protected:
                /**
                 * Get raw target.
                 *
                 * @return Underlying java object reference.
                 */
                jobject GetTarget()
                {
                    return javaRef;
                }

                /**
                 * Get environment reference.
                 *
                 * @return Environment reference.
                 */
                IgniteEnvironment& GetEnvironment()
                {
                    return *env.Get();
                }

            private:
                /** Environment. */
                ignite::common::concurrent::SharedPointer<IgniteEnvironment> env;

                /** Handle to Java object. */
                jobject javaRef;

                /** javaRef release flag. */
                bool skipJavaRefRelease;

                IGNITE_NO_COPY_ASSIGNMENT(InteropTarget);

                /**
                 * Write data to memory.
                 *
                 * @param mem Memory.
                 * @param inOp Input opeartion.
                 * @param err Error.
                 * @return Memory pointer.
                 */
                int64_t WriteTo(interop::InteropMemory* mem, InputOperation& inOp, IgniteError& err);

                /**
                 * Read data from memory.
                 *
                 * @param mem Memory.
                 * @param outOp Output operation.
                 */
                void ReadFrom(interop::InteropMemory* mem, OutputOperation& outOp);

                /**
                 * Read error data from memory.
                 *
                 * @param mem Memory.
                 * @param err Error.
                 */
                void ReadError(interop::InteropMemory* mem, IgniteError& err);
            };
        }
    }    
}

#endif //_IGNITE_IMPL_INTEROP_INTEROP_TARGET
