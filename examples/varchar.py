#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: varchar.py
##
##  Description:
##      implements varchar(s), varchar(s,n), and varchar(n)
##
##      varchar(s) returns a structure initilized with string s
##      varchar(s,n) returns an n-character structure initilized with string s
##      varchar(n) returns an empty n-character structure
##
##  Examples:
##      vc = varchar('Hello World')
##      vc = varchar('One of these days...', 50)
##      vc = varchar(32)
     

import ctypes
from multipledispatch import dispatch


def _varchar(value, length, capacity):
    '''return an instance of ctypes.Structure that maps an Ingres VARCHAR'''

    ##  value is the byte-string value of the varchar;
    ##  capacity is the declared size of the varchar;
    ##  length is the number of bytes in the value

    ##  assert capacity >= 1 and 0 <= length < capacity

    class Varchar(ctypes.Structure):
        _fields_ = [
            ('_length', ctypes.c_ushort),
            ('_value', ctypes.c_char * capacity)]

        @property
        def value(self):
            return self._value[: self._length]

        def as_str(self):
            v = self._value[: self._length]
            return v.decode()

        def __str__(self):
            return self.as_str()

    instance = Varchar()
    instance._length = length
    instance._value = value[:length]
    instance._capacity = capacity
    return instance


@dispatch(str)
def varchar(string):
    '''instantiate a sufficiently commodious struct with a VARCHAR'''

    _value = string.encode()
    _length = len(_value)
    _capacity = _length
    instance = _varchar(_value, _length, _capacity)
    return instance


@dispatch(str, int)
def varchar(string, size):
    '''instantiate a struct of specificied size with a VARCHAR'''

    _value = string.encode()
    ##  we make no attempt to verify size is large enough for the encoded str
    _capacity = size
    _length = size if size < len(string) else len(string)
    instance = _varchar(_value, _length, _capacity)
    return instance


@dispatch(int)
def varchar(size):
    '''instantiate a sufficiently commodious empty struct for a VARCHAR'''

    _value = b''
    _length = 0
    _capacity = size
    instance = _varchar(_value, _length, _capacity)
    return instance
