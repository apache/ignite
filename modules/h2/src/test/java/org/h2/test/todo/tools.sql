/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */

-- TO_DATE
create alias TO_DATE as $$
java.util.Date toDate(String s) throws Exception {
    return new java.text.SimpleDateFormat("yyyy.MM.dd").parse(s);
}
$$;
call TO_DATE('1990.02.03')

-- TO_CHAR
drop alias if exists TO_CHAR;
create alias TO_CHAR as $$
String toChar(BigDecimal x, String pattern) throws Exception {
    return new java.text.DecimalFormat(pattern).format(x);
}
$$;
call TO_CHAR(123456789.12, '###,###,###,###.##');

-- update all rows in all tables
select 'update ' || table_schema || '.' || table_name || ' set ' || column_name || '=' || column_name || ';'
from information_schema.columns where ORDINAL_POSITION = 1 and table_schema <> 'INFORMATION_SCHEMA';

-- read the first few bytes from a BLOB
drop table test;
drop alias first_bytes;
create alias first_bytes as $$
import java.io.*;
@CODE
byte[] firstBytes(InputStream in, int len) throws IOException {
    try {
        byte[] data = new byte[len];
        DataInputStream dIn = new DataInputStream(in);
        dIn.readFully(data, 0, len);
        return data;
    } finally {
        in.close();
    }
}
$$;
create table test(data blob);
insert into test values('010203040506070809');
select first_bytes(data, 3) from test;

-- encrypt and decrypt strings
CALL CAST(ENCRYPT('AES', HASH('SHA256', STRINGTOUTF8('key'), 1), STRINGTOUTF8('Hello')) AS VARCHAR);
CALL TRIM(CHAR(0) FROM UTF8TOSTRING(DECRYPT('AES', HASH('SHA256', STRINGTOUTF8('key'), 1), '16e44604230717eec9f5fa6058e77e83')));
DROP ALIAS ENC;
DROP ALIAS DEC;
CREATE ALIAS ENC AS $$
import org.h2.security.*;
import org.h2.util.*;
@CODE
String encrypt(String data, String key) throws Exception {
    byte[] k = new SHA256().getHash(key.getBytes("UTF-8"), false);
    byte[] b1 = data.getBytes("UTF-8");
    byte[] buff = new byte[(b1.length + 15) / 16 * 16];
    System.arraycopy(b1, 0, buff, 0, b1.length);
    BlockCipher bc = CipherFactory.getBlockCipher("AES");
    bc.setKey(k);
    bc.encrypt(buff, 0, buff.length);
    return ByteUtils.convertBytesToString(buff);
}
$$;
CREATE ALIAS DEC AS $$
import org.h2.security.*;
import org.h2.util.*;
@CODE
String decrypt(String data, String key) throws Exception {
    byte[] k = new SHA256().getHash(key.getBytes("UTF-8"), false);
    byte[] buff = ByteUtils.convertStringToBytes(data);
    BlockCipher bc = CipherFactory.getBlockCipher("AES");
    bc.setKey(k);
    bc.decrypt(buff, 0, buff.length);
    return StringUtils.trim(new String(buff, "UTF-8"), false, true, "\u0000");
}
$$;
CALL ENC('Hello', 'key');
CALL DEC(ENC('Hello', 'key'), 'key');
