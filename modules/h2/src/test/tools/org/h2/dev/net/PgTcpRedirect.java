/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.net;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class helps debug the PostgreSQL network protocol.
 * It listens on one port, and sends the exact same data to another port.
 */
public class PgTcpRedirect {

    private static final boolean DEBUG = false;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new PgTcpRedirect().loop(args);
    }

    private void loop(String... args) throws Exception {
        // MySQL protocol:
        // http://www.redferni.uklinux.net/mysql/MySQL-Protocol.html
        // PostgreSQL protocol:
        // http://developer.postgresql.org/pgdocs/postgres/protocol.html
        // int portServer = 9083, portClient = 9084;
        // int portServer = 3306, portClient = 3307;
        // H2 PgServer
        // int portServer = 5435, portClient = 5433;
        // PostgreSQL
        int portServer = 5432, portClient = 5433;

        for (int i = 0; i < args.length; i++) {
            if ("-client".equals(args[i])) {
                portClient = Integer.parseInt(args[++i]);
            } else if ("-server".equals(args[i])) {
                portServer = Integer.parseInt(args[++i]);
            }
        }
        ServerSocket listener = new ServerSocket(portClient);
        while (true) {
            Socket client = listener.accept();
            Socket server = new Socket("localhost", portServer);
            TcpRedirectThread c = new TcpRedirectThread(client, server, true);
            TcpRedirectThread s = new TcpRedirectThread(server, client, false);
            new Thread(c).start();
            new Thread(s).start();
        }
    }

    /**
     * This is the working thread of the TCP redirector.
     */
    private class TcpRedirectThread implements Runnable {

        private static final int STATE_INIT_CLIENT = 0, STATE_REGULAR = 1;
        private final Socket read, write;
        private int state;
        private final boolean client;

        TcpRedirectThread(Socket read, Socket write, boolean client) {
            this.read = read;
            this.write = write;
            this.client = client;
            state = client ? STATE_INIT_CLIENT : STATE_REGULAR;
        }

        String readStringNull(InputStream in) throws IOException {
            StringBuilder buff = new StringBuilder();
            while (true) {
                int x = in.read();
                if (x <= 0) {
                    break;
                }
                buff.append((char) x);
            }
            return buff.toString();
        }

        private void println(String s) {
            if (DEBUG) {
                System.out.println(s);
            }
        }

        private boolean processClient(InputStream inStream,
                OutputStream outStream) throws IOException {
            DataInputStream dataIn = new DataInputStream(inStream);
            ByteArrayOutputStream buff = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(buff);
            if (state == STATE_INIT_CLIENT) {
                state = STATE_REGULAR;
                int len = dataIn.readInt();
                dataOut.writeInt(len);
                len -= 4;
                byte[] data = new byte[len];
                dataIn.readFully(data, 0, len);
                dataOut.write(data);
                dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));
                int version = dataIn.readInt();
                if (version == 80877102) {
                    println("CancelRequest");
                    println(" pid: " + dataIn.readInt());
                    println(" key: " + dataIn.readInt());
                } else if (version == 80877103) {
                    println("SSLRequest");
                } else {
                    println("StartupMessage");
                    println(" version " + version + " (" + (version >> 16)
                            + "." + (version & 0xff) + ")");
                    while (true) {
                        String param = readStringNull(dataIn);
                        if (param.length() == 0) {
                            break;
                        }
                        String value = readStringNull(dataIn);
                        println(" param " + param + "=" + value);
                    }
                }
            } else {
                int x = dataIn.read();
                if (x < 0) {
                    println("end");
                    return false;
                }
                // System.out.println(" x=" + (char)x+" " +x);
                dataOut.write(x);
                int len = dataIn.readInt();
                dataOut.writeInt(len);
                len -= 4;
                byte[] data = new byte[len];
                dataIn.readFully(data, 0, len);
                dataOut.write(data);
                dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));
                switch (x) {
                case 'B': {
                    println("Bind");
                    println(" destPortal: " + readStringNull(dataIn));
                    println(" prepName: " + readStringNull(dataIn));
                    int formatCodesCount = dataIn.readShort();
                    for (int i = 0; i < formatCodesCount; i++) {
                        println(" formatCode[" + i + "]=" + dataIn.readShort());
                    }
                    int paramCount = dataIn.readShort();
                    for (int i = 0; i < paramCount; i++) {
                        int paramLen = dataIn.readInt();
                        println(" length[" + i + "]=" + paramLen);
                        byte[] d2 = new byte[paramLen];
                        dataIn.readFully(d2);
                    }
                    int resultCodeCount = dataIn.readShort();
                    for (int i = 0; i < resultCodeCount; i++) {
                        println(" resultCodeCount[" + i + "]=" + dataIn.readShort());
                    }
                    break;
                }
                case 'C': {
                    println("Close");
                    println(" type: (S:prepared statement, P:portal): " + dataIn.read());
                    break;
                }
                case 'd': {
                    println("CopyData");
                    break;
                }
                case 'c': {
                    println("CopyDone");
                    break;
                }
                case 'f': {
                    println("CopyFail");
                    println(" message: " + readStringNull(dataIn));
                    break;
                }
                case 'D': {
                    println("Describe");
                    println(" type (S=prepared statement, P=portal): " + (char) dataIn.readByte());
                    println(" name: " + readStringNull(dataIn));
                    break;
                }
                case 'E': {
                    println("Execute");
                    println(" name: " + readStringNull(dataIn));
                    println(" maxRows: " + dataIn.readShort());
                    break;
                }
                case 'H': {
                    println("Flush");
                    break;
                }
                case 'F': {
                    println("FunctionCall");
                    println(" objectId:" + dataIn.readInt());
                    int columns = dataIn.readShort();
                    for (int i = 0; i < columns; i++) {
                        println(" formatCode[" + i + "]: " + dataIn.readShort());
                    }
                    int count = dataIn.readShort();
                    for (int i = 0; i < count; i++) {
                        int l = dataIn.readInt();
                        println(" len[" + i + "]: " + l);
                        if (l >= 0) {
                            for (int j = 0; j < l; j++) {
                                dataIn.readByte();
                            }
                        }
                    }
                    println(" resultFormat: " + dataIn.readShort());
                    break;
                }
                case 'P': {
                    println("Parse");
                    println(" name:" + readStringNull(dataIn));
                    println(" query:" + readStringNull(dataIn));
                    int count = dataIn.readShort();
                    for (int i = 0; i < count; i++) {
                        println(" [" + i + "]: " + dataIn.readInt());
                    }
                    break;
                }
                case 'p': {
                    println("PasswordMessage");
                    println(" password: " + readStringNull(dataIn));
                    break;
                }
                case 'Q': {
                    println("Query");
                    println(" sql : " + readStringNull(dataIn));
                    break;
                }
                case 'S': {
                    println("Sync");
                    break;
                }
                case 'X': {
                    println("Terminate");
                    break;
                }
                default:
                    println("############## UNSUPPORTED: " + (char) x);
                }
            }
            dataOut.flush();
            byte[] buffer = buff.toByteArray();
            printData(buffer, buffer.length);
            try {
                outStream.write(buffer, 0, buffer.length);
                outStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }

        private boolean processServer(InputStream inStream,
                OutputStream outStream) throws IOException {
            DataInputStream dataIn = new DataInputStream(inStream);
            ByteArrayOutputStream buff = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(buff);
            int x = dataIn.read();
            if (x < 0) {
                println("end");
                return false;
            }
            // System.out.println(" x=" + (char)x+" " +x);
            dataOut.write(x);
            int len = dataIn.readInt();
            dataOut.writeInt(len);
            len -= 4;
            byte[] data = new byte[len];
            dataIn.readFully(data, 0, len);
            dataOut.write(data);
            dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));
            switch (x) {
            case 'R': {
                println("Authentication");
                int value = dataIn.readInt();
                if (value == 0) {
                    println(" Ok");
                } else if (value == 2) {
                    println(" KerberosV5");
                } else if (value == 3) {
                    println(" CleartextPassword");
                } else if (value == 4) {
                    println(" CryptPassword");
                    byte b1 = dataIn.readByte();
                    byte b2 = dataIn.readByte();
                    println(" salt1=" + b1 + " salt2=" + b2);
                } else if (value == 5) {
                    println(" MD5Password");
                    byte b1 = dataIn.readByte();
                    byte b2 = dataIn.readByte();
                    byte b3 = dataIn.readByte();
                    byte b4 = dataIn.readByte();
                    println(" salt1=" + b1 + " salt2=" + b2 + " 3=" + b3 + " 4=" + b4);
                } else if (value == 6) {
                    println(" SCMCredential");
                }
                break;
            }
            case 'K': {
                println("BackendKeyData");
                println(" process ID " + dataIn.readInt());
                println(" key " + dataIn.readInt());
                break;
            }
            case '2': {
                println("BindComplete");
                break;
            }
            case '3': {
                println("CloseComplete");
                break;
            }
            case 'C': {
                println("CommandComplete");
                println(" command tag: " + readStringNull(dataIn));
                break;
            }
            case 'd': {
                println("CopyData");
                break;
            }
            case 'c': {
                println("CopyDone");
                break;
            }
            case 'G': {
                println("CopyInResponse");
                println(" format: " + dataIn.readByte());
                int columns = dataIn.readShort();
                for (int i = 0; i < columns; i++) {
                    println(" formatCode[" + i + "]: " + dataIn.readShort());
                }
                break;
            }
            case 'H': {
                println("CopyOutResponse");
                println(" format: " + dataIn.readByte());
                int columns = dataIn.readShort();
                for (int i = 0; i < columns; i++) {
                    println(" formatCode[" + i + "]: " + dataIn.readShort());
                }
                break;
            }
            case 'D': {
                println("DataRow");
                int columns = dataIn.readShort();
                println(" columns : " + columns);
                for (int i = 0; i < columns; i++) {
                    int l = dataIn.readInt();
                    if (l > 0) {
                        for (int j = 0; j < l; j++) {
                            dataIn.readByte();
                        }
                    }
                    // println(" ["+i+"] len: " + l);
                }
                break;
            }
            case 'I': {
                println("EmptyQueryResponse");
                break;
            }
            case 'E': {
                println("ErrorResponse");
                while (true) {
                    int fieldType = dataIn.readByte();
                    if (fieldType == 0) {
                        break;
                    }
                    String msg = readStringNull(dataIn);
                    // http://developer.postgresql.org/pgdocs/postgres/protocol-error-fields.html
                    // S Severity
                    // C Code: the SQLSTATE code
                    // M Message
                    // D Detail
                    // H Hint
                    // P Position
                    // p Internal position
                    // q Internal query
                    // W Where
                    // F File
                    // L Line
                    // R Routine
                    println(" fieldType: " + fieldType + " msg: " + msg);
                }
                break;
            }
            case 'V': {
                println("FunctionCallResponse");
                int resultLen = dataIn.readInt();
                println(" len: " + resultLen);
                break;
            }
            case 'n': {
                println("NoData");
                break;
            }
            case 'N': {
                println("NoticeResponse");
                while (true) {
                    int fieldType = dataIn.readByte();
                    if (fieldType == 0) {
                        break;
                    }
                    String msg = readStringNull(dataIn);
                    // http://developer.postgresql.org/pgdocs/postgres/protocol-error-fields.html
                    // S Severity
                    // C Code: the SQLSTATE code
                    // M Message
                    // D Detail
                    // H Hint
                    // P Position
                    // p Internal position
                    // q Internal query
                    // W Where
                    // F File
                    // L Line
                    // R Routine
                    println(" fieldType: " + fieldType + " msg: " + msg);
                }
                break;
            }
            case 'A': {
                println("NotificationResponse");
                println(" processID: " + dataIn.readInt());
                println(" condition: " + readStringNull(dataIn));
                println(" information: " + readStringNull(dataIn));
                break;
            }
            case 't': {
                println("ParameterDescription");
                println(" processID: " + dataIn.readInt());
                int count = dataIn.readShort();
                for (int i = 0; i < count; i++) {
                    println(" [" + i + "] objectId: " + dataIn.readInt());
                }
                break;
            }
            case 'S': {
                println("ParameterStatus");
                println(" parameter " + readStringNull(dataIn) + " = "
                        + readStringNull(dataIn));
                break;
            }
            case '1': {
                println("ParseComplete");
                break;
            }
            case 's': {
                println("ParseComplete");
                break;
            }
            case 'Z': {
                println("ReadyForQuery");
                println(" status (I:idle, T:transaction, E:failed): "
                        + (char) dataIn.readByte());
                break;
            }
            case 'T': {
                println("RowDescription");
                int columns = dataIn.readShort();
                println(" columns : " + columns);
                for (int i = 0; i < columns; i++) {
                    println(" [" + i + "]");
                    println("  name:" + readStringNull(dataIn));
                    println("  tableId:" + dataIn.readInt());
                    println("  columnId:" + dataIn.readShort());
                    println("  dataTypeId:" + dataIn.readInt());
                    println("  dataTypeSize (pg_type.typlen):" + dataIn.readShort());
                    println("  modifier (pg_attribute.atttypmod):" + dataIn.readInt());
                    println("  format code:" + dataIn.readShort());
                }
                break;
            }
            default:
                println("############## UNSUPPORTED: " + (char) x);
            }
            dataOut.flush();
            byte[] buffer = buff.toByteArray();
            printData(buffer, buffer.length);
            try {
                outStream.write(buffer, 0, buffer.length);
                outStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }

        @Override
        public void run() {
            try {
                OutputStream out = write.getOutputStream();
                InputStream in = read.getInputStream();
                while (true) {
                    boolean more;
                    if (client) {
                        more = processClient(in, out);
                    } else {
                        more = processServer(in, out);
                    }
                    if (!more) {
                        break;
                    }
                }
                try {
                    read.close();
                } catch (IOException e) {
                    // ignore
                }
                try {
                    write.close();
                } catch (IOException e) {
                    // ignore
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Print the uninterpreted byte array.
     *
     * @param buffer the byte array
     * @param len the length
     */
    static synchronized void printData(byte[] buffer, int len) {
        if (DEBUG) {
            System.out.print(" ");
            for (int i = 0; i < len; i++) {
                int c = buffer[i] & 255;
                if (c >= ' ' && c <= 127 && c != '[' & c != ']') {
                    System.out.print((char) c);
                } else {
                    System.out.print("[" + Integer.toHexString(c) + "]");
                }
            }
            System.out.println();
        }
    }
}
