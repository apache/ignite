/// <reference types="node" />
export declare class ClientSocket {
    private _endpoint;
    private _config;
    private _state;
    private _socket;
    private _requestId;
    private _handshakeRequestId;
    private _protocolVersion;
    private _requests;
    private _onSocketDisconnect;
    private _error;
    private _wasConnected;
    private _buffer;
    private _offset;
    private _host;
    private _port;
    private _version;
    constructor(endpoint: any, config: any, onSocketDisconnect: any);
    connect(): Promise<{}>;
    disconnect(): void;
    readonly requestId: any;
    sendRequest(opCode: any, payloadWriter: any, payloadReader?: any): Promise<{}>;
    _connectSocket(handshakeRequest: any): void;
    _addRequest(request: any): void;
    _sendRequest(request: any): Promise<void>;
    _processResponse(message: any): Promise<void>;
    _finalizeHandshake(buffer: any, request: any, isSuccess: any): Promise<void>;
    _finalizeResponse(buffer: any, request: any, isSuccess: any): Promise<void>;
    _handshakePayloadWriter(payload: any): Promise<void>;
    _getHandshake(version: any, resolve: any, reject: any): Request;
    _isSupportedVersion(protocolVersion: any): boolean;
    _disconnect(close?: boolean, callOnDisconnect?: boolean): void;
    _parseEndpoint(endpoint: any): void;
    _logMessage(requestId: any, isRequest: any, message: any): void;
}
declare class Request {
    private _id;
    private _opCode;
    private _payloadWriter;
    private _payloadReader;
    private _resolve;
    private _reject;
    constructor(id: any, opCode: any, payloadWriter: any, payloadReader: any, resolve: any, reject: any);
    readonly id: any;
    readonly payloadReader: any;
    readonly resolve: any;
    readonly reject: any;
    getMessage(): Promise<Buffer>;
}
export {};
