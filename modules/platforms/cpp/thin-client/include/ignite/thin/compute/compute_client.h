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

/**
 * @file
 * Declares ignite::thin::compute::ComputeClient class.
 */

#ifndef _IGNITE_THIN_COMPUTE_COMPUTE_CLIENT
#define _IGNITE_THIN_COMPUTE_COMPUTE_CLIENT

#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace thin
    {
        namespace compute
        {
            /**
             * Client Compute API.
             *
             * @see IgniteClient::GetCompute()
             *
             * This class is implemented as a reference to an implementation so copying of this class instance will only
             * create another reference to the same underlying object. Underlying object will be released automatically
             * once all the instances are destructed.
             */
            class IGNITE_IMPORT_EXPORT ComputeClient
            {
                typedef common::concurrent::SharedPointer<void> SP_Void;
            public:
                /**
                 * Default constructor.
                 */
                ComputeClient()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                ComputeClient(SP_Void impl) :
                    impl(impl),
                    timeout(0)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ComputeClient()
                {
                    // No-op.
                }

                /**
                 * Returns a new instance of ComputeClient with a timeout for all task executions.
                 *
                 * @param timeoutMs Timeout in milliseconds.
                 * @return New ComputeClient instance with timeout.
                 */
                ComputeClient WithTimeout(int64_t timeoutMs)
                {
                    return ComputeClient(impl, timemoutMs);
                }

            private:
                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 * @param timeout Timeout in milliseconds.
                 */
                ComputeClient(SP_Void impl, int64_t timeout) :
                    impl(impl),
                    timeout(timeout)
                {
                    // No-op.
                }

                /** Implementation. */
                SP_Void impl;

                /** Timeout. */
                int64_t timeout;
            };
        }
    }
}

#endif // _IGNITE_THIN_COMPUTE_COMPUTE_CLIENT
