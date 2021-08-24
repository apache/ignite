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

#ifndef _IGNITE_IMPL_THIN_COMPUTE_COMPUTE_CLIENT
#define _IGNITE_IMPL_THIN_COMPUTE_COMPUTE_CLIENT

#include "impl/data_router.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace compute
            {
                /**
                 * Thin client Compute implementation.
                 */
                class ComputeClientImpl
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param router Data router instance.
                     */
                    ComputeClientImpl(const SP_DataRouter& router);

                    /**
                     * Destructor.
                     */
                    ~ComputeClientImpl() {}

                private:
                    /** Data router. */
                    SP_DataRouter router;

                    IGNITE_NO_COPY_ASSIGNMENT(ComputeClientImpl);
                };

                typedef ignite::common::concurrent::SharedPointer<ComputeClientImpl> SP_ComputeClientImpl;
            }
        }
    }
}

#endif // _IGNITE_IMPL_THIN_COMPUTE_COMPUTE_CLIENT
