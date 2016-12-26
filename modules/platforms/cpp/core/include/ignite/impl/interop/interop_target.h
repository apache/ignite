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
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                InteropTarget(ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);

                /**
                 * Destructor.
                 */
                ~InteropTarget();

                /**
                 * Internal out operation.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result.
                 */
                bool OutOp(int32_t opType, InputOperation& inOp, IgniteError* err);

                /**
                 * Internal out operation.
                 *
                 * @param opType Operation type.
                 * @param err Error.
                 * @return Result.
                 */
                bool OutOp(int32_t opType, IgniteError* err);

                /**
                 * Internal out operation.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result.
                 */
                bool InOp(int32_t opType, OutputOperation& outOp, IgniteError* err);

                /**
                 * Internal out-in operation.
                 * Uses two independent memory pieces to write and read data.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void OutInOp(int32_t opType, InputOperation& inOp, OutputOperation& outOp, IgniteError* err);

                /**
                 * Internal out-in operation.
                 * Uses single memory piece to write and read data.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void OutInOpX(int32_t opType, InputOperation& inOp, OutputOperation& outOp, IgniteError* err);

                /**
                * Internal out-in operation.
                *
                * @param opType Operation type.
                * @param val Value.
                * @param err Error.
                */
                int64_t OutInOpLong(int32_t opType, int64_t val, IgniteError* err);

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

                IGNITE_NO_COPY_ASSIGNMENT(InteropTarget)

                /**
                 * Write data to memory.
                 *
                 * @param mem Memory.
                 * @param inOp Input opeartion.
                 * @param err Error.
                 * @return Memory pointer.
                 */
                int64_t WriteTo(interop::InteropMemory* mem, InputOperation& inOp, IgniteError* err);

                /**
                 * Read data from memory.
                 *
                 * @param mem Memory.
                 * @param outOp Output operation.
                 */
                void ReadFrom(interop::InteropMemory* mem, OutputOperation& outOp);
            };
        }
    }    
}

#endif //_IGNITE_IMPL_INTEROP_INTEROP_TARGET