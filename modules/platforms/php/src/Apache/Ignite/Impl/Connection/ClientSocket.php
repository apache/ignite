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

use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Impl\Utils\Logger;
use Apache\Ignite\Impl\Binary\BinaryUtils;
use Apache\Ignite\Impl\Binary\BinaryReader;
use Apache\Ignite\Impl\Binary\BinaryWriter;
use Apache\Ignite\Impl\Binary\MessageBuffer;
use Apache\Ignite\Impl\Binary\Request;
use Apache\Ignite\Exception\ConnectionException;
use Apache\Ignite\Exception\OperationException;

class ClientSocket
{
    const HANDSHAKE_SUCCESS_STATUS_CODE = 1;
    const REQUEST_SUCCESS_STATUS_CODE = 0;
    const PORT_DEFAULT = 10800;
    const SOCKET_READ_SIZE = 1024;
    
    private static $supportedVersions;

    private $endpoint;
    private $config;
    private $socket;
    private $protocolVersion;

    public function __construct(string $endpoint, ClientConfiguration $config)
    {
        $this->endpoint = $endpoint;
        $this->config = $config;
        $this->socket = null;
        $this->protocolVersion = null;
    }
    
    public static function init(): void
    {
        ClientSocket::$supportedVersions = [
            ProtocolVersion::$V_1_1_0
        ];
    }
    
    public function getEndpoint(): string
    {
        return $this->endpoint;
    }
    
    public function connect(): void
    {
        $options = $this->config->getConnectionOptions();
        $isSsl = $options && array_key_exists('ssl', $options); 
        $context = stream_context_create($options);
        $errno = 0;
        $errstr = null;
        if (!($this->socket = stream_socket_client(
                ($isSsl ? 'ssl://' : 'tcp://') . $this->endpoint,
                $errno,
                $errstr,
                ini_get("default_socket_timeout"), 
                STREAM_CLIENT_CONNECT | STREAM_CLIENT_PERSISTENT,
                $context))) {
            throw new ConnectionException($errstr);
        }
        // send handshake
        $this->processRequest($this->getHandshakeRequest(ProtocolVersion::$V_1_1_0));
    }

    public function disconnect(): void
    {
        if ($this->socket !== false) {
            fclose($this->socket);
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
        $buffer->writeByte(1);
        // Protocol version
        $this->protocolVersion->write($buffer);
        // Client code
        $buffer->writeByte(2);
        if ($this->config->getUserName()) {
            BinaryWriter::writeString($buffer, $this->config->getUserName());
            BinaryWriter::writeString($buffer, $this->config->getPassword());
        }
    }
    
    public function sendRequest(int $opCode, callable $payloadWriter, callable $payloadReader = null): void
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
            $written = fwrite($this->socket, $data);
            if ($length === $written) {
                break;
            }
            if ($written === false || $written === 0) {
                throw new ConnectionException('Error while writing data to the server');
            }
            $data = substr($data, $written);
        }
        $this->processResponse($request);
    }
    
    private function receive(MessageBuffer $buffer, int $minSize): void
    {
        while ($buffer->getLength() < $minSize) 
        {
            $chunk = fread($this->socket, ClientSocket::SOCKET_READ_SIZE);
            if ($chunk === false || $chunk === '') {
                throw new ConnectionException('Error while reading data from the server');
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
                $errMessage = BinaryReader::readObject($buffer);
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
        $errMessage = BinaryReader::readObject($buffer);

        if (!$this->protocolVersion->equals($serverVersion)) {
            throw new OperationException(
                sprintf('Protocol version mismatch: client %s / server %s. Server details: %s',
                    $this->protocolVersion.toString(), $serverVersion.toString(), $errMessage));
        } else {
            $this->disconnect();
            throw new OperationException($errMessage);
        }
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
