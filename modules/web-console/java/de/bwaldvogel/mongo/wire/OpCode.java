package de.bwaldvogel.mongo.wire;

import java.util.HashMap;
import java.util.Map;

public enum OpCode {
    OP_REPLY(1), // Reply to a client request. responseTo is set
    OP_MSG(1000), // generic msg command followed by a string
    OP_UPDATE(2001), // update document
    OP_INSERT(2002), // insert new document
    RESERVED(2003), // formerly used for OP_GET_BY_OID
    OP_QUERY(2004), // query a collection
    OP_GET_MORE(2005), // Get more data from a query. See Cursors
    OP_DELETE(2006), // Delete documents
    OP_KILL_CURSORS(2007); // Tell database client is done with a cursor

    private final int id;

    private static Map<Integer, OpCode> byIdMap = new HashMap<>();

    static {
        for (final OpCode opCode : values()) {
            byIdMap.put(Integer.valueOf(opCode.id), opCode);
        }
    }

    OpCode(final int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static OpCode getById(int id) {
        return byIdMap.get(Integer.valueOf(id));
    }

}
