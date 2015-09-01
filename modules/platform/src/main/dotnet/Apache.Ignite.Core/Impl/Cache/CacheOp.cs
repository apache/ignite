﻿﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache
{
    /// <summary>
    /// Cache opcodes.
    /// </summary>
    internal enum CacheOp
    {
        CLEAR = 1,
        CLEAR_ALL = 2,
        CONTAINS_KEY = 3,
        CONTAINS_KEYS = 4,
        GET = 5,
        GET_ALL = 6,
        GET_AND_PUT = 7,
        GET_AND_PUT_IF_ABSENT = 8,
        GET_AND_REMOVE = 9,
        GET_AND_REPLACE = 10,
        GET_NAME = 11,
        INVOKE = 12,
        INVOKE_ALL = 13,
        IS_LOCAL_LOCKED = 14,
        LOAD_CACHE = 15,
        LOC_EVICT = 16,
        LOC_LOAD_CACHE = 17,
        LOC_PROMOTE = 18,
        LOCAL_CLEAR = 20,
        LOCAL_CLEAR_ALL = 21,
        LOCK = 22,
        LOCK_ALL = 23,
        METRICS = 24,
        PEEK = 25,
        PUT = 26,
        PUT_ALL = 27,
        PUT_IF_ABSENT = 28,
        QRY_CONTINUOUS = 29,
        QRY_SCAN = 30,
        QRY_SQL = 31,
        QRY_SQL_FIELDS = 32,
        QRY_TXT = 33,
        REMOVE_ALL = 34,
        REMOVE_BOOL = 35,
        REMOVE_OBJ = 36,
        REPLACE_2 = 37,
        REPLACE_3 = 38
    }
}