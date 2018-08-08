<?php
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

namespace Apache\Ignite;

/**
 * Class representing Ignite client configuration.
 *
 * The configuration includes:
 *   - (mandatory) Ignite node endpoint(s)
 *   - (optional) user credentials for authentication
 *   - (optional) TLS enabling
 *   - (optional) connection options
 */
class ClientConfiguration
{
    private $endpoints;
    private $userName;
    private $password;
    
    /**
     * Creates an instance of Ignite client configuration
     * with the provided mandatory settings and default optional settings.
     *
     * By default, the client does not use authentication and secure connection.
     *
     * @param string ...$endpoints Ignite node endpoint(s). The client randomly connects/reconnects 
     * to one of the specified node.
     *
     * @return ClientConfiguration new client configuration instance.
     *
     * @throws Exception::ClientException if error.
     */
    public function __construct(string ...$endpoints)
    {
        $this->endpoints = $endpoints;
        $this->userName = null;
        $this->password = null;
    }
    
    /**
     * Returns Ignite node endpoints specified in the constructor.
     * 
     * @return string[] endpoints
     */
    public function getEndpoints(): array
    {
        return $this->endpoints;
    }

    /**
     * Sets username which will be used for authentication during the client's connection.
     *
     * If username is not set, the client does not use authentication during connection.
     * 
     * @param string|null $userName username. If null, authentication is disabled.
     * 
     * @return ClientConfiguration the same instance of the ClientConfiguration.
     */
    public function setUserName(?string $userName): ClientConfiguration
    {
        $this->userName = $userName;
        return $this;
    }
    
    /**
     * Returns the username specified in the setUserName() method.
     * 
     * @return string|null username or null (if authentication is disabled).
     */
    public function getUserName(): ?string
    {
        return $this->userName;
    }
    
    /**
     * Sets password which will be used for authentication during the client's connection.
     *
     * Password is ignored, if username is not set.
     * If password is not set, it is considered empty.
     * 
     * @param string|null $password password. If null, password is empty.
     * 
     * @return ClientConfiguration the same instance of the ClientConfiguration.
     */
    public function setPassword(?string $password): ClientConfiguration
    {
        $this->password = $password;
        return $this;
    }
    
    /**
     * Returns the password specified in the setPassword() method.
     * 
     * @return string|null password or null (if password is empty).
     */
    public function getPassword(): ?string
    {
        return $this->password;
    }
}
