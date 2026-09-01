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

#include <Python.h>

#ifdef _MSC_VER

typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;

#else
#include <stdint.h>
#endif

static int32_t FNV1_OFFSET_BASIS = 0x811c9dc5;
static int32_t FNV1_PRIME = 0x01000193;


PyObject* hashcode(PyObject* self, PyObject *args);
PyObject* schema_id(PyObject* self, PyObject *args);

PyObject* str_hashcode(PyObject* data);
int32_t str_hashcode_(PyObject* data, int lower);
PyObject* b_hashcode(PyObject* data);

static PyMethodDef methods[] = {
    {"hashcode", (PyCFunction) hashcode, METH_VARARGS, ""},
    {"schema_id", (PyCFunction) schema_id, METH_VARARGS, ""},
    {NULL, NULL, 0, NULL}       /* Sentinel */
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_cutils",
    0,                    /* m_doc */
    -1,                   /* m_size */
    methods,              /* m_methods */
    NULL,                 /* m_slots */
    NULL,                 /* m_traverse */
    NULL,                 /* m_clear */
    NULL,                 /* m_free */
};

static char* hashcode_input_err = "supported only strings, bytearrays, bytes and memoryview";
static char* schema_id_input_err = "input argument must be dict or int";
static char* schema_field_type_err = "schema keys must be strings";

PyMODINIT_FUNC PyInit__cutils(void) {
	return PyModule_Create(&moduledef);
}

PyObject* hashcode(PyObject* self, PyObject *args) {
    PyObject* data;

    if (!PyArg_ParseTuple(args, "O", &data)) {
        return NULL;
    }

    if (data == Py_None) {
        return PyLong_FromLong(0);
    }
    else if (PyUnicode_CheckExact(data)) {
        return str_hashcode(data);
    }
    else {
        return b_hashcode(data);
    }
}

PyObject* str_hashcode(PyObject* data) {
    return PyLong_FromLong(str_hashcode_(data, 0));
}

int32_t str_hashcode_(PyObject *str, int lower) {
    int32_t res = 0;

    Py_ssize_t sz = PyUnicode_GET_LENGTH(str);
    if (!sz) {
        return res;
    }

    int kind = PyUnicode_KIND(str);
    void* buf = PyUnicode_DATA(str);

    Py_ssize_t i;
    for (i = 0; i < sz; i++) {
        Py_UCS4 ch = PyUnicode_READ(kind, buf, i);

        if (lower) {
            ch = Py_UNICODE_TOLOWER(ch);
        }

        res = 31 * res + ch;
    }

    return res;
}

PyObject* b_hashcode(PyObject* data) {
    int32_t res = 1;
    Py_ssize_t sz; char* buf;

    if (PyBytes_CheckExact(data)) {
        sz = PyBytes_GET_SIZE(data);
        buf = PyBytes_AS_STRING(data);
    }
    else if (PyByteArray_CheckExact(data)) {
        sz = PyByteArray_GET_SIZE(data);
        buf = PyByteArray_AS_STRING(data);
    }
    else if (PyMemoryView_Check(data)) {
        Py_buffer* pyBuf = PyMemoryView_GET_BUFFER(data);
        sz = pyBuf->len;
        buf = (char*)pyBuf->buf;
    }
    else {
        PyErr_SetString(PyExc_ValueError, hashcode_input_err);
        return NULL;
    }

    Py_ssize_t i;
    for (i = 0; i < sz; i++) {
        res = 31 * res + (signed char)buf[i];
    }

    return PyLong_FromLong(res);
}

PyObject* schema_id(PyObject* self, PyObject *args) {
    PyObject* data;

    if (!PyArg_ParseTuple(args, "O", &data)) {
        return NULL;
    }

    if (PyLong_CheckExact(data)) {
        return PyNumber_Long(data);
    }
    else if (data == Py_None) {
        return PyLong_FromLong(0);
    }
    else if (PyDict_Check(data)) {
        Py_ssize_t sz = PyDict_Size(data);

        if (sz == 0) {
            return PyLong_FromLong(0);
        }

        int32_t s_id = FNV1_OFFSET_BASIS;

        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(data, &pos, &key, &value)) {
            if (!PyUnicode_CheckExact(key)) {
                PyErr_SetString(PyExc_ValueError, schema_field_type_err);
                return NULL;
            }

            int32_t field_id = str_hashcode_(key, 1);
            s_id ^= field_id & 0xff;
            s_id *= FNV1_PRIME;
            s_id ^= (field_id >> 8) & 0xff;
            s_id *= FNV1_PRIME;
            s_id ^= (field_id >> 16) & 0xff;
            s_id *= FNV1_PRIME;
            s_id ^= (field_id >> 24) & 0xff;
            s_id *= FNV1_PRIME;
        }

        return PyLong_FromLong(s_id);
    }
    else {
        PyErr_SetString(PyExc_ValueError, schema_id_input_err);
        return NULL;
    }
}
