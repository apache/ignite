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

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Utils\ArgumentChecker;

/**
 * Class representing Ignite client configuration.
 *
 * The configuration includes:
 *   - (mandatory) Ignite node endpoint(s)
 *   - (optional) user credentials for authentication
 *   - (optional) TLS options for secure connection
 *   - (optional) connection options
 */
class ClientConfiguration
{
    private $endpoints;
    private $userName;
    private $password;
    private $tlsOptions;
    private $timeout;
    private $sendChunkSize;
    private $receiveChunkSize;
    private $tcpNoDelay;

    const ENDPOINT_PORT_DEFAULT = 10800;

    /**
     * Creates an instance of Ignite client configuration
     * with the provided mandatory settings and default optional settings.
     *
     * By default, the client does not use authentication and secure connection.
     *
     * @param string ...$endpoints Ignite node endpoint(s). The client randomly connects/reconnects
     * to one of the specified node.
     *
     * @throws ClientException if error.
     */
    public function __construct(string ...$endpoints)
    {
        ArgumentChecker::notEmpty($endpoints, 'endpoints');
        $this->endpoints = array_map(array($this, 'parseEndpoint'), $endpoints);
        $this->userName = null;
        $this->password = null;
        $this->tlsOptions = null;
        $this->timeout = 0;
        $this->sendChunkSize = 0;
        $this->receiveChunkSize = 0;
        $this->tcpNoDelay = true;
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
     * Returns the current username.
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
     * Returns the current password.
     * 
     * @return string|null password or null (if password is empty).
     */
    public function getPassword(): ?string
    {
        return $this->password;
    }
    
    /**
     * Enables and setup TLS connection.
     *
     * If not enabled (by default), the client does not use secure connection.
     *
     * @param array|null $tlsOptions TLS connection options in the format defined here: http://php.net/manual/en/context.ssl.php
     * If null, secure connection is not used.
     *
     * @return ClientConfiguration the same instance of the ClientConfiguration.
     */
    public function setTLSOptions(?array $tlsOptions): ClientConfiguration
    {
        $this->tlsOptions = $tlsOptions;
        return $this;
    }
    
    /**
     * Returns the current TLS connection options.
     * 
     * @return array|null TLS connection options or null (if secure connection is not used).
     */
    public function getTLSOptions(): ?array
    {
        return $this->tlsOptions;
    }

    /**
     * Sets send/receive timeout.
     *
     * Default value is defined by the PHP settings.
     *
     * @param int $timeout send/receive timeout (in milliseconds).
     *
     * @return ClientConfiguration the same instance of the ClientConfiguration.
     */
    public function setTimeout(int $timeout): ClientConfiguration
    {
        $this->timeout = $timeout;
        return $this;
    }

    /**
     * Returns the current send/receive timeout.
     *
     * @return int send/receive timeout (in milliseconds).
     */
    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * Sets the size of the send chunk.
     *
     * 8192 bytes by default.
     *
     * @param int $size size of the send chunk (in bytes).
     *
     * @return ClientConfiguration the same instance of the ClientConfiguration.
     */
    public function setSendChunkSize(int $size): ClientConfiguration
    {
        $this->sendChunkSize = $size;
        return $this;
    }

    /**
     * Returns the current size of the send chunk.
     *
     * @return int size of the send chunk (in bytes).
     */
    public function getSendChunkSize(): int
    {
        return $this->sendChunkSize;
    }

    /**
     * Sets the size of the receive chunk.
     *
     * 8192 bytes by default.
     *
     * @param int $size size of the receive chunk (in bytes).
     *
     * @return ClientConfiguration the same instance of the ClientConfiguration.
     */
    public function setReceiveChunkSize(int $size): ClientConfiguration
    {
        $this->receiveChunkSize = $size;
        return $this;
    }

    /**
     * Returns the current size of the receive chunk.
     *
     * @return int size of the receive chunk (in bytes).
     */
    public function getReceiveChunkSize(): int
    {
        return $this->receiveChunkSize;
    }

    /**
     * Disables/enables the TCP Nagle algorithm.
     *
     * Enabled by default.
     *
     * @param bool $tcpNoDelay true to enable, false to disable.
     *
     * @return ClientConfiguration the same instance of the ClientConfiguration.
     */
    public function setTcpNoDelay(bool $tcpNoDelay): ClientConfiguration
    {
        $this->tcpNoDelay = $tcpNoDelay;
        return $this;
    }

    /**
     * Returns the current status of the TCP Nagle algorithm.
     *
     * @return bool true if enabled, false if disabled.
     */
    public function getTcpNoDelay(): bool
    {
        return $this->tcpNoDelay;
    }

    private function parseEndpoint(string $endpoint): string
    {
        $endpoint = trim($endpoint);
        $host = $endpoint;
        $port = null;
        $parsed = explode(':', $endpoint);
        if (count($parsed) > 2) {
            // IPv6 address
            $index = strrpos($endpoint, ']:');
            if ($index !== false) {
                $host = substr($endpoint, 0, $index + 1);
                $port = substr($endpoint, $index + 2);
            }
            $first = $host[0];
            $last = $host[strlen($host) - 1];
            if ($first === '[' || $last === ']') {
                if (!($first === '[' && $last === ']')) {
                    ArgumentChecker::illegalArgument('Incorrect endpoint format: ' . $endpoint);
                }
            } else {
                $host = sprintf('[%s]', $host);
            }
        }
        else {
            // IPv4 address
            $host = $parsed[0];
            if (count($parsed) === 2) {
                $port = $parsed[1];
            }
        }
        if ($port === null) {
            $port = self::ENDPOINT_PORT_DEFAULT;
        } elseif (!ctype_digit($port)) {
            ArgumentChecker::illegalArgument('Incorrect endpoint format: ' . $endpoint);
        }
        return sprintf('%s:%s', $host, $port);
    }
}
