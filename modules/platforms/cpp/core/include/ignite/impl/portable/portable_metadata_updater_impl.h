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

#ifndef _IGNITE_IMPL_PORTABLE_METADATA_UPDATER_IMPL
#define _IGNITE_IMPL_PORTABLE_METADATA_UPDATER_IMPL

#include <ignite/common/exports.h>

#include "ignite/impl/ignite_environment.h"
#include "ignite/impl/portable/portable_metadata_updater.h"

namespace ignite
{    
    namespace impl
    {
        namespace portable
        {
            /**
             * Metadata updater implementation.
             */
            class IGNITE_IMPORT_EXPORT PortableMetadataUpdaterImpl : public PortableMetadataUpdater
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to Java object which is able to process metadata request.
                 */
                PortableMetadataUpdaterImpl(ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);

                /**
                 * Destructor.
                 */
                ~PortableMetadataUpdaterImpl();

                bool Update(Snap* snapshot, IgniteError* err);
            private:
                /** Environment. */
                ignite::common::concurrent::SharedPointer<IgniteEnvironment> env;
                
                /** Handle to Java object. */
                jobject javaRef;                 

                IGNITE_NO_COPY_ASSIGNMENT(PortableMetadataUpdaterImpl)
            };
        }
    }    
}

#endif