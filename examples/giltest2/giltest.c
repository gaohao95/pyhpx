#include <Python.h>

static PyObject* giltest_calculate(PyObject *self, PyObject *args)
{
    Py_BEGIN_ALLOW_THREADS
    int sum = 0;
    for(int j = 0; j < 1000; j++)
        for(int k = 0; k < 1000; k++)
            sum = (sum + 1) % 10000;
    Py_END_ALLOW_THREADS

    Py_RETURN_NONE;
}

static PyMethodDef GiltestMethods[] = {
    {"calculate", giltest_calculate, METH_VARARGS, "A C Function for testing GIL behavior"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef giltestmodule = {
    PyModuleDef_HEAD_INIT,
    "giltest",
    NULL,
    -1,
    GiltestMethods
};

PyMODINIT_FUNC
PyInit_giltest(void)
{
    return PyModule_Create(&giltestmodule);
}

