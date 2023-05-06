#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisinit.c
##
##  Description:
##      Demonstrates using IIapi_init(),IIapi_setEnvParam(),
##      IIapi_releaseEnv() and IIapi_term()
##
##  Following actions are demonstrate in the main()
##      Initialize at level 1
##      Terminate API
##      Initialize at level 2
##      Set environment parameter
##      Release environmment resources
##      Terminate API
##
##  Command syntax:
##      python apisinit.py


from pyngres import *
import ctypes
import sys


argv = sys.argv
script = argv[0]

inp = IIAPI_INITPARM()
tmp = IIAPI_TERMPARM()
sep = IIAPI_SETENVPRMPARM()
rep = IIAPI_RELENVPARM()

##  Initialize API at level 1
print(f'{script}: initializing API at level 1')
inp.in_version = IIAPI_VERSION_1
inp.in_timeout = -1

IIapi_initialize(inp)

##  Terminate API
print(f'{script}: shutting down API')

IIapi_terminate(tmp)

##  Initialize API at level 11.  Save environment handle
print(f'{script}: initializing API at level 11')
inp.in_version = IIAPI_VERSION_11
inp.in_timeout = -1

IIapi_initialize(inp)

envHandle = inp.in_envHandle

##  Set an environment parameter
print(f'{script}: set environment parameter')
sep.se_envHandle = envHandle
sep.se_paramID = IIAPI_EP_DATE_FORMAT
longvalue = ctypes.c_long(IIAPI_EPV_DFRMT_YMD)
sep.se_paramValue = ctypes.addressof(longvalue)
longvalue = IIAPI_EPV_DFRMT_YMD

IIapi_setEnvParam(sep)

##  Release environment resources
print(f'{script}: releasing environment handle')
rep.re_envHandle = envHandle

IIapi_releaseEnv(rep)

##  Terminate API
print(f'{script}: shutting down API')

IIapi_terminate(tmp)
