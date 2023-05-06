#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisdata.py
##
##  Description:
##      Demonstrates using IIapi_formatData() and IIapi_convertData()
##
##  Following actions are demonstrated in the main
##      Set environment parameter
##      Convert string to DATE using environment date format.
##      Convert DATE to string using installation date format.
##
##  Command syntax:
##      python apisdata.py


from pyngres import *
import ctypes
import sys


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    IIapi_initialize(inp)

    envHandle = inp.in_envHandle
    return envHandle


def IIdemo_term(envHandle):
    '''Terminate API access'''

    rep = IIAPI_RELENVPARM()
    tmp = IIAPI_TERMPARM()

    print('IIdemo_term: releasing environment resources')
    rep.re_envHandle = envHandle
    IIapi_releaseEnv(rep)

    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)

    envHandle = None
    return envHandle


##  this is the main body of the demonstration code
sep = IIAPI_SETENVPRMPARM()
fdp = IIAPI_FORMATPARM()
cvp = IIAPI_CONVERTPARM()
isodate = b'20020304 10:30:00 pm'

argv = sys.argv
script = argv[0]

envHandle = IIdemo_init()

print(f'{script}: set environment parameter')

sep.se_envHandle = envHandle
sep.se_paramID = IIAPI_EP_DATE_FORMAT
paramValue = ctypes.c_long(IIAPI_EPV_DFRMT_ISO)
sep.se_paramValue = ctypes.addressof(paramValue)

IIapi_setEnvParam(sep)

##  convert ISO date string to Ingres internal date
print(f'ISO date = {isodate}')
print(f'{script}: formatting date')

fdp.fd_envHandle = envHandle
fdp.fd_srcDesc.ds_dataType = IIAPI_CHA_TYPE
fdp.fd_srcDesc.ds_nullable = False
fdp.fd_srcDesc.ds_length = len(isodate)
fdp.fd_srcDesc.ds_precision = 0
fdp.fd_srcDesc.ds_scale = 0
fdp.fd_srcDesc.ds_columnType = IIAPI_COL_QPARM
fdp.fd_srcDesc.ds_columnName = None

fdp.fd_srcValue.dv_null = False
fdp.fd_srcValue.dv_length = len(isodate)
isodate_str = ctypes.create_string_buffer(isodate)
fdp.fd_srcValue.dv_value = ctypes.addressof(isodate_str)

fdp.fd_dstDesc.ds_dataType = IIAPI_DTE_TYPE
fdp.fd_dstDesc.ds_nullable = False
fdp.fd_dstDesc.ds_length = 12
fdp.fd_dstDesc.ds_precision = 0
fdp.fd_dstDesc.ds_scale = 0
fdp.fd_dstDesc.ds_columnType = IIAPI_COL_QPARM
fdp.fd_dstDesc.ds_columnName = None

fdp.fd_dstValue.dv_null = False
fdp.fd_dstValue.dv_length = 12
date = ctypes.c_buffer(12)
fdp.fd_dstValue.dv_value = ctypes.addressof(date)

IIapi_formatData(fdp)

##  Convert Ingres internal date to string
##  using installation date format.
print(f'{script}: converting date')

cvp.cv_srcDesc.ds_dataType = IIAPI_DTE_TYPE
cvp.cv_srcDesc.ds_nullable = False
cvp.cv_srcDesc.ds_length = 12
cvp.cv_srcDesc.ds_precision = 0
cvp.cv_srcDesc.ds_scale = 0
cvp.cv_srcDesc.ds_columnType = IIAPI_COL_QPARM
cvp.cv_srcDesc.ds_columnName = None

cvp.cv_srcValue.dv_null = False
cvp.cv_srcValue.dv_length = 12
cvp.cv_srcValue.dv_value = ctypes.addressof(date)

cvp.cv_dstDesc.ds_dataType = IIAPI_CHA_TYPE
cvp.cv_dstDesc.ds_nullable = False
date_str = ctypes.create_string_buffer(26)
cvp.cv_dstDesc.ds_length = ctypes.sizeof(date_str) - 1
cvp.cv_dstDesc.ds_precision = 0
cvp.cv_dstDesc.ds_scale = 0
cvp.cv_dstDesc.ds_columnType = IIAPI_COL_QPARM
cvp.cv_dstDesc.ds_columnName = None

cvp.cv_dstValue.dv_null = False
cvp.cv_dstValue.dv_length = ctypes.sizeof(date_str) - 1
cvp.cv_dstValue.dv_value = ctypes.addressof(date_str)

IIapi_convertData(cvp)

dlen = cvp.cv_dstValue.dv_length
date_string = date_str.value[:dlen]
print(f'{date_string}')

IIdemo_term(envHandle)
quit()
