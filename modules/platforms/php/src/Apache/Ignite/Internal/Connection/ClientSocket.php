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
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Internal\Utils\Logger;
use Apache\Ignite\Internal\Binary\BinaryUtils;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Binary\Request;
use Apache\Ignite\Exception\NoConnectionException;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Exception\OperationStatusUnknownException;

class ClientSocket
{
    const HANDSHAKE_SUCCESS_STATUS_CODE = 1;
    const REQUEST_SUCCESS_STATUS_CODE = 0;
    const PORT_DEFAULT = 10800;
    const SOCKET_CHUNK_SIZE_DEFAULT = 8192;
    const HANDSHAKE_CODE = 1;
    const CLIENT_CODE = 2;

    private static $currentVersion;
    private static $supportedVersions;

    private $endpoint;
    private $config;
    private $socket;
    private $sendChunkSize;
    private $receiveChunkSize;
    private $protocolVersion;

    public function __construct(string $endpoint, ClientConfiguration $config)
    {
        $this->endpoint = $endpoint;
        $this->config = $config;
        $this->socket = null;
        $this->sendChunkSize = $config->getSendChunkSize() > 0 ?
            $config->getSendChunkSize() :
            self::SOCKET_CHUNK_SIZE_DEFAULT;
        $this->receiveChunkSize = $config->getReceiveChunkSize() > 0 ?
            $config->getReceiveChunkSize() :
            self::SOCKET_CHUNK_SIZE_DEFAULT;
        $this->protocolVersion = null;
    }

    public function __destruct()
    {
        $this->disconnect();
    }
    
    public static function init(): void
    {
        ClientSocket::$currentVersion = ProtocolVersion::$V_1_2_0;
        ClientSocket::$supportedVersions = [
            ProtocolVersion::$V_1_2_0
        ];
    }
    
    public function getEndpoint(): string
    {
        return $this->endpoint;
    }

    public function connect(): void
    {
        $tlsOptions = $this->config->getTLSOptions();
        $options = ['socket' => ['tcp_nodelay' => $this->config->getTcpNoDelay()]];
        if ($tlsOptions) {
            $options['ssl'] = $tlsOptions;
        }
        $context = stream_context_create($options);
        $errno = 0;
        $errstr = null;
        if (!($this->socket = stream_socket_client(
                ($tlsOptions ? 'ssl://' : 'tcp://') . $this->endpoint,
                $errno,
                $errstr,
                ini_get('default_socket_timeout'),
                STREAM_CLIENT_CONNECT,
                $context))) {
            throw new NoConnectionException($errstr);
        }
        if ($this->config->getTimeout() > 0) {
            $timeout = $this->config->getTimeout();
            stream_set_timeout($this->socket, intdiv($timeout, 1000), $timeout % 1000);
        }
        // send handshake
        $this->processRequest($this->getHandshakeRequest(ClientSocket::$currentVersion));
    }

    public function disconnect(): void
    {
        if ($this->socket !== false && $this->socket !== null) {
            fclose($this->socket);
            $this->socket = null;
        }
    }
    
    private function getHandshakeRequest($version): Request
    {
        $this->protocolVersion = $version;
        return new Request(-1, array($this, 'handshakePayloadWriter'), null, true);
    }

    public function handshakePayloadWriter(MessageBuffer $buffer): void
    {
        // Handshake code
        $buffer->writeByte(ClientSocket::HANDSHAKE_CODE);
        // Protocol version
        $this->protocolVersion->write($buffer);
        // Client code
        $buffer->writeByte(ClientSocket::CLIENT_CODE);
        if ($this->config->getUserName()) {
            BinaryCommunicator::writeString($buffer, $this->config->getUserName());
            BinaryCommunicator::writeString($buffer, $this->config->getPassword());
        }
    }

    public function sendRequest(int $opCode, ?callable $payloadWriter, callable $payloadReader = null): void
    {
        $request = new Request($opCode, $payloadWriter, $payloadReader);
        $this->processRequest($request);
    }

    private function processRequest(Request $request): void
    {
        $buffer = $request->getMessage();
        $this->logMessage($request->getId(), true, $buffer);
        $data = $buffer->getBuffer();
        while (($length = strlen($data)) > 0) {
            $written = fwrite($this->socket, $data, $this->sendChunkSize);
            if ($length === $written) {
                break;
            }
            if ($written === false || $written === 0) {
                throw new OperationStatusUnknownException('Error while writing data to the server');
            }
            $data = substr($data, $written);
        }
        $this->processResponse($request);
    }

    private function receive(MessageBuffer $buffer, int $minSize): void
    {
        while ($buffer->getLength() < $minSize)
        {
            $chunk = fread($this->socket, $this->receiveChunkSize);
            if ($chunk === false || $chunk === '') {
                throw new OperationStatusUnknownException('Error while reading data from the server');
            } else {
                $buffer->append($chunk);
            }
        }
    }

    private function processResponse(Request $request): void
    {
        $buffer = new MessageBuffer(0);
        $this->receive($buffer, BinaryUtils::getSize(ObjectType::INTEGER));
        // Response length
        $length = $buffer->readInteger();
        $this->receive($buffer, $length + BinaryUtils::getSize(ObjectType::INTEGER));
        if ($request->isHandshake()) {
            $this->processHandshake($buffer);
        } else {
            // Request id
            $requestId = $buffer->readLong();
            if (!BinaryUtils::floatEquals($requestId, $request->getId())) {
                BinaryUtils::internalError('Invalid response id: ' . $requestId);
            }
            // Status code
            $isSuccess = ($buffer->readInteger() === ClientSocket::REQUEST_SUCCESS_STATUS_CODE);
            if (!$isSuccess) {
                // Error message
                $errMessage = BinaryCommunicator::readString($buffer);
                throw new OperationException($errMessage);
            } else {
                $payloadReader = $request->getPayloadReader();
                if ($payloadReader) {
                    call_user_func($payloadReader, $buffer);
                }
            }
        }
        $this->logMessage($request->getId(), false, $buffer);
    }

    private function processHandshake(MessageBuffer $buffer): void
    {
        // Handshake status
        if ($buffer->readByte() === ClientSocket::HANDSHAKE_SUCCESS_STATUS_CODE) {
            return;
        }
        // Server protocol version
        $serverVersion = new ProtocolVersion();
        $serverVersion->read($buffer);
        // Error message
        $errMessage = BinaryCommunicator::readString($buffer);

        if (!$this->isSupportedVersion($serverVersion)) {
            throw new OperationException(
                sprintf('Protocol version mismatch: client %s / server %s. Server details: %s',
                    $this->protocolVersion->toString(), $serverVersion->toString(), $errMessage));
        } else {
            $this->disconnect();
            throw new OperationException($errMessage);
        }
    }

    private function isSupportedVersion(ProtocolVersion $version): bool
    {
        foreach (ClientSocket::$supportedVersions as $supportedVersion) {
            if ($supportedVersion->equals($version)) {
                return true;
            }
        }
        return false;
    }
    
    private function logMessage(int $requestId, bool $isRequest, MessageBuffer $buffer): void
    {
        if (Logger::isDebug()) {
            Logger::logDebug(($isRequest ? 'Request: ' : 'Response: ') . $requestId);
            Logger::logBuffer($buffer);
        }
    }
}

ClientSocket::init();
