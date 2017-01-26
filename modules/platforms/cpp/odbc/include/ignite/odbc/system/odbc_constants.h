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

#ifndef _IGNITE_ODBC_SYSTEM_ODBC_CONSTANTS
#define _IGNITE_ODBC_SYSTEM_ODBC_CONSTANTS

#ifdef _WIN32

#define _WINSOCKAPI_
#include <windows.h>

#ifdef min
#   undef min
#endif // min

#endif //_WIN32

#define ODBCVER 0x0380

#include <sqlext.h>
#include <odbcinst.h>

#ifndef UNREFERENCED_PARAMETER
#define UNREFERENCED_PARAMETER(x) (void)(x)
#endif // UNREFERENCED_PARAMETER

#endif //_IGNITE_ODBC_SYSTEM_ODBC_CONSTANTS
