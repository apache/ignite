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

using namespace ignite::thin;

void main()
{
    //tag::thin-client-ssl[]
    IgniteClientConfiguration cfg;

    // Sets SSL mode.
    cfg.SetSslMode(SslMode::Type::REQUIRE);

    // Sets file path to SSL certificate authority to authenticate server certificate during connection establishment.
    cfg.SetSslCaFile("path/to/SSL/certificate/authority");

    // Sets file path to SSL certificate to use during connection establishment.
    cfg.SetSslCertFile("path/to/SSL/certificate");

    // Sets file path to SSL private key to use during connection establishment.
    cfg.SetSslKeyFile("path/to/SSL/private/key");
    //end::thin-client-ssl[]
}
