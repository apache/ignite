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

#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

#include <ignite/impl/thin/ignite_client_impl.h>

namespace ignite
{
    namespace thin
    {
        IgniteClient::IgniteClient(common::concurrent::SharedPointer<impl::thin::IgniteClientImpl>& impl)
        {
            this->impl.Swap(impl);
        }

        IgniteClient::~IgniteClient()
        {
            // No-op.
        }

        IgniteClient IgniteClient::Start(const IgniteClientConfiguration& cfg)
        {
            common::concurrent::SharedPointer<impl::thin::IgniteClientImpl> res(new impl::thin::IgniteClientImpl(cfg));

            res.Get()->Start();

            return IgniteClient(res);
        }
    }
}
