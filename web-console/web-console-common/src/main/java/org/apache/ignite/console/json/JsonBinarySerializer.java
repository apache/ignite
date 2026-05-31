package org.apache.ignite.console.json;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryWriter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.ClusterSerializable;

/**
 * vertx.JsonObject vertx.JsonArray
 */
public class JsonBinarySerializer implements BinarySerializer {
    /** {@inheritDoc} */
    @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
    	ClusterSerializable o = (ClusterSerializable)obj;
    	
    	Buffer buf = Buffer.buffer();
    	o.writeToBuffer(buf);
        writer.writeByteArray("_clusterSerialize", buf.getBytes());
    }

    /** {@inheritDoc} */
    @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
    	ClusterSerializable o = (ClusterSerializable)obj;
    	
    	byte[] data = reader.readByteArray("_clusterSerialize");
    	Buffer buf = BufferImpl.buffer(Unpooled.wrappedBuffer(data));
    	o.readFromBuffer(0, buf);
        
    }
}