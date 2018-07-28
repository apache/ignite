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

namespace Apache\Ignite\Impl\Connection;

use Apache\Ignite\IgniteClientConfiguration;
use Apache\Ignite\Exception\ConnectionException;
use Apache\Ignite\Impl\Utils\Logger;

class ClientFailoverSocket
{
    const STATE_DISCONNECTED = 0;
    const STATE_CONNECTING = 1;
    const STATE_CONNECTED = 2;

    private $socket;
    private $state;
            
    public function __construct()
    {
        $this->socket = null;
        $this->state = ClientFailoverSocket::STATE_DISCONNECTED;
    }

    public function connect(IgniteClientConfiguration $config): void
    {
        if ($this->state !== ClientFailoverSocket::STATE_DISCONNECTED) {
            $this->disconnect();
        }
        $this->config = $config;
        $this->socket = new ClientSocket($this->config->getEndpoints()[0], $this->config);
        $this->changeState(ClientFailoverSocket::STATE_CONNECTING);
        $this->socket->connect();
        $this->changeState(ClientFailoverSocket::STATE_CONNECTED);
    }

    public function send(int $opCode, callable $payloadWriter, callable $payloadReader = null): void
    {
        if ($this->state !== ClientFailoverSocket::STATE_CONNECTED) {
            throw new ConnectionException();
        }
        $this->socket->sendRequest($opCode, $payloadWriter, $payloadReader);
    }

    public function disconnect(): void
    {
        if ($this->state !== ClientFailoverSocket::STATE_DISCONNECTED) {
            $this->changeState(ClientFailoverSocket::STATE_DISCONNECTED);
            if ($this->socket) {
                $this->socket->disconnect();
                $this->socket = null;
            }
        }
    }

    private function changeState(int $state): void
    {
        if (Logger::isDebug() && $this->socket) {
            Logger::logDebug(sprintf('Socket %s: %s -> %s',
                $this->socket->getEndpoint(), $this->getState($this->state), $this->getState($state)));
        }
        $this->state = $state;
    }

    private function getState(int $state)
    {
        switch ($state) {
            case ClientFailoverSocket::STATE_DISCONNECTED:
                return 'DISCONNECTED';
            case ClientFailoverSocket::STATE_CONNECTING:
                return 'CONNECTING';
            case ClientFailoverSocket::STATE_CONNECTED:
                return 'CONNECTED';
            default:
                return 'UNKNOWN';
        }
    }
}
