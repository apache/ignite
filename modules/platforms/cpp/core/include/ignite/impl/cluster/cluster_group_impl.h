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

#ifndef _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL
#define _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

#include <ignite/impl/interop/interop_target.h>

namespace ignite 
{
    namespace impl
    {
        namespace cluster
        {
            /**
             * Cluster group implementation.
             */
            class IGNITE_FRIEND_EXPORT ClusterGroupImpl : private interop::InteropTarget
            {
                typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                ClusterGroupImpl(SP_IgniteEnvironment env, jobject javaRef);

                /**
                 * Destructor.
                 */
                ~ClusterGroupImpl();

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ClusterGroupImpl)
            };
        }
    }
}

#endif //_IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL