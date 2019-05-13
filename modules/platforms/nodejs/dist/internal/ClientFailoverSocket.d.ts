/** Socket wrapper with failover functionality: reconnects on failure. */
export declare class ClientFailoverSocket {
    private _socket;
    private _state;
    private _onStateChanged;
    private _config;
    private _endpointsNumber;
    private _endpointIndex;
    constructor(onStateChanged: any);
    connect(config: any): Promise<void>;
    send(opCode: any, payloadWriter: any, payloadReader?: any): Promise<void>;
    disconnect(): void;
    _onSocketDisconnect(error?: any): Promise<void>;
    _connect(): Promise<void>;
    _changeState(state: any, endpoint?: any, reason?: any): void;
    _getState(state: any): "DISCONNECTED" | "CONNECTING" | "CONNECTED" | "UNKNOWN";
    _getRandomInt(max: any): number;
}
