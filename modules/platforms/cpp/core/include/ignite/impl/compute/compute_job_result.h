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

/**
 * @file
 * Declares ignite::impl::compute::ComputeJobResult class template.
 */

#ifndef _IGNITE_IMPL_COMPUTE_COMPUTE_JOB_RESULT
#define _IGNITE_IMPL_COMPUTE_COMPUTE_JOB_RESULT

#include <memory>
#include <sstream>

#include <ignite/common/promise.h>
#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_writer_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            struct ComputeJobResultPolicy
            {
                enum Type
                {
                    /**
                    * Wait for results if any are still expected. If all results have been received -
                    * it will start reducing results.
                    */
                    WAIT = 0,

                    /**
                    * Ignore all not yet received results and start reducing results.
                    */
                    REDUCE = 1,

                    /**
                    * Fail-over job to execute on another node.
                    */
                    FAILOVER = 2
                };
            };

            /**
             * Used to hold compute job result.
             */
            template<typename R>
            class ComputeJobResult
            {
            public:
                typedef R ResultType;
                /**
                 * Default constructor.
                 */
                ComputeJobResult() :
                    res(),
                    err()
                {
                    // No-op.
                }

                /**
                 * Set result value.
                 *
                 * @param val Value to set as a result.
                 */
                void SetResult(const ResultType& val)
                {
                    res = val;
                }

                /**
                 * Get result value.
                 *
                 * @return Result.
                 */
                const ResultType& GetResult() const
                {
                    return res;
                }

                /**
                 * Set error.
                 *
                 * @param error Error to set.
                 */
                void SetError(const IgniteError& error)
                {
                    err = error;
                }

                /**
                 * Get error.
                 *
                 * @return Error.
                 */
                const IgniteError& GetError() const
                {
                    return err;
                }

                /**
                 * Set promise to a state which corresponds to result.
                 *
                 * @param promise Promise, which state to set.
                 */
                void SetPromise(common::Promise<ResultType>& promise)
                {
                    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                        promise.SetError(err);
                    else
                        promise.SetValue(std::auto_ptr<ResultType>(new ResultType(res)));
                }

                /**
                 * Write using writer.
                 *
                 * @param writer Writer.
                 */
                void Write(binary::BinaryWriterImpl& writer)
                {
                    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                    {
                        // Fail
                        writer.WriteBool(false);

                        // Native Exception
                        writer.WriteBool(true);

                        writer.WriteObject<IgniteError>(err);
                    }
                    else
                    {
                        // Success
                        writer.WriteBool(true);

                        writer.WriteObject<ResultType>(res);
                    }
                }

                /**
                 * Read using reader.
                 *
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl& reader)
                {
                    bool success = reader.ReadBool();

                    if (success)
                    {
                        res = reader.ReadObject<ResultType>();

                        err = IgniteError();
                    }
                    else
                    {
                        bool native = reader.ReadBool();

                        if (native)
                            err = reader.ReadObject<IgniteError>();
                        else
                        {
                            std::stringstream buf;

                            buf << reader.ReadObject<std::string>() << " : ";
                            buf << reader.ReadObject<std::string>() << ", ";
                            buf << reader.ReadObject<std::string>();

                            std::string msg = buf.str();

                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, msg.c_str());
                        }
                    }
                }

            private:
                /** Result. */
                ResultType res;

                /** Erorr. */
                IgniteError err;
            };

            /**
             * Used to hold compute job result.
             */
            template<>
            class ComputeJobResult<void>
            {
            public:
                /**
                 * Default constructor.
                 */
                ComputeJobResult() :
                    err()
                {
                    // No-op.
                }

                /**
                 * Mark as complete.
                 */
                void SetResult()
                {
                    err = IgniteError();
                }

                /**
                 * Set error.
                 *
                 * @param error Error to set.
                 */
                void SetError(const IgniteError error)
                {
                    err = error;
                }

                /**
                 * Get error.
                 *
                 * @return Error.
                 */
                const IgniteError& GetError() const
                {
                    return err;
                }

                /**
                 * Set promise to a state which corresponds to result.
                 *
                 * @param promise Promise, which state to set.
                 */
                void SetPromise(common::Promise<void>& promise)
                {
                    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                        promise.SetError(err);
                    else
                        promise.SetValue();
                }

                /**
                 * Write using writer.
                 *
                 * @param writer Writer.
                 */
                void Write(binary::BinaryWriterImpl& writer)
                {
                    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                    {
                        // Fail
                        writer.WriteBool(false);

                        // Native Exception
                        writer.WriteBool(true);

                        writer.WriteObject<IgniteError>(err);
                    }
                    else
                    {
                        // Success
                        writer.WriteBool(true);

                        writer.WriteNull();
                    }
                }

                /**
                 * Read using reader.
                 *
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl& reader)
                {
                    bool success = reader.ReadBool();

                    if (success)
                        err = IgniteError();
                    else
                    {
                        bool native = reader.ReadBool();

                        if (native)
                            err = reader.ReadObject<IgniteError>();
                        else
                        {
                            std::stringstream buf;

                            buf << reader.ReadObject<std::string>() << " : ";
                            buf << reader.ReadObject<std::string>() << ", ";
                            buf << reader.ReadObject<std::string>();

                            std::string msg = buf.str();

                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, msg.c_str());
                        }
                    }
                }

            private:
                /** Erorr. */
                IgniteError err;
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_JOB_RESULT
