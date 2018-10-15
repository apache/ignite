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

namespace Apache\Ignite\Internal\Connection;

use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Exception\NoConnectionException;
use Apache\Ignite\Exception\OperationStatusUnknownException;
use Apache\Ignite\Internal\Utils\Logger;

class ClientFailoverSocket
{
    const STATE_DISCONNECTED = 0;
    const STATE_CONNECTING = 1;
    const STATE_CONNECTED = 2;

    private $socket;
    private $state;
    private $config;
    private $endpointsNumber;
    private $endpointIndex;
    private $reconnectRequired;
            
    public function __construct()
    {
        $this->socket = null;
        $this->state = ClientFailoverSocket::STATE_DISCONNECTED;
        $this->reconnectRequired = false;
    }

    public function connect(ClientConfiguration $config): void
    {
        if ($this->state !== ClientFailoverSocket::STATE_DISCONNECTED) {
            $this->disconnect();
        }
        $this->config = $config;
        $this->endpointsNumber = count($this->config->getEndpoints());
        $this->endpointIndex = rand(0, $this->endpointsNumber - 1);
        $this->failoverConnect();
    }

    public function send(int $opCode, ?callable $payloadWriter, callable $payloadReader = null): void
    {
        if ($this->reconnectRequired) {
            $this->failoverConnect();
            $this->reconnectRequired = false;
        }
        if ($this->state !== ClientFailoverSocket::STATE_CONNECTED) {
            throw new NoConnectionException();
        }
        try {
            $this->socket->sendRequest($opCode, $payloadWriter, $payloadReader);
        } catch (OperationStatusUnknownException $e) {
            $this->disconnect();
            $this->endpointIndex++;
            $this->reconnectRequired = true;
            throw $e;
        }
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

    private function failoverConnect(): void
    {
        $errors = [];
        for ($i = 0; $i < $this->endpointsNumber; $i++) {
            $index = ($this->endpointIndex + $i) % $this->endpointsNumber;
            $endpoint = $this->config->getEndpoints()[$index];
            try {
                $this->changeState(ClientFailoverSocket::STATE_CONNECTING, $endpoint);
                $this->socket = new ClientSocket($endpoint, $this->config);
                $this->socket->connect();
                $this->changeState(ClientFailoverSocket::STATE_CONNECTED, $endpoint);
                $this->endpointIndex = $index;
                return;
            } catch (NoConnectionException $e) {
                Logger::logError($e->getMessage());
                array_push($errors, sprintf('[%s] %s', $endpoint, $e->getMessage()));
                $this->changeState(ClientFailoverSocket::STATE_DISCONNECTED, $endpoint);
            }
        }
        $this->socket = null;
        throw new NoConnectionException(implode(';', $errors));
    }

    private function changeState(int $state, ?string $endpoint = null): void
    {
        if (Logger::isDebug()) {
            Logger::logDebug(sprintf('Socket %s: %s -> %s',
                $endpoint ? $endpoint : ($this->socket ? $this->socket->getEndpoint() : ''),
                $this->getState($this->state),
                $this->getState($state)));
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
