package org.apache.ignite.internal.benchmarks.jmh.binary;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

public class PooledMessagePacker extends MessagePacker {
    public PooledMessagePacker(PooledMessageBufferOutput messageBufferOutput, MessagePack.PackerConfig packerConfig) {
        super(messageBufferOutput, packerConfig);
    }

    public byte[] toByteArray() {
        return ((PooledMessageBufferOutput) out).getPooledArray();
    }
}
