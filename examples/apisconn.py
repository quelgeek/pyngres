#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisconn.py
##
##  Description:
##  	Demonstrates using IIapi_connect(),IIapi_setConnectParam(),
##  	IIapi_disconnect() and IIapi_abort()
##      Modeled on code supplied by Actian in II_SYSTEM/ingres/demo/api.
##
##  Following actions are demonstrated
##  	Connect (no conn parms)
##  	Disconnect
##  	Set Connection Parameters
##  	Connect (with conn parms)
##  	Abort Connection
##
##  Command syntax: python apisconn.py [vnode::]dbname[/server_class]


from pyngres import *
import ctypes
import sys


def IIdemo_init():
    '''demonstrate how to initialize the Ingres OpenAPI'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    IIapi_initialize(inp)
    status = inp.in_status
    if status != IIAPI_ST_SUCCESS:
        print(f'Error: {status=} ({IIAPI_ST_MSG[status]})')
        quit()
    return inp.in_envHandle


def IIdemo_term(envHandle):
    '''Terminate API access'''

    rep = IIAPI_RELENVPARM()
    rep.re_envHandle = envHandle
    print('IIdemo_term: releasing environment resources')
    IIapi_releaseEnv(rep)
    tmp = IIAPI_TERMPARM()
    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)


##  main body of the sample code
argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()

wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

envHandle = IIdemo_init()

##  connect with no connection parameters
print(f'{script}: establishing connection')
cop = IIAPI_CONNPARM()
cop.co_genParm.gp_callback = None
cop.co_genParm.gp_closure = None
cop.co_target = target
cop.co_connHandle = envHandle
cop.co_tranHandle = None
cop.co_type = IIAPI_CT_SQL
cop.co_username = None
cop.co_password = None
cop.co_timeout = -1
IIapi_connect(cop)
while not cop.co_genParm.gp_completed:
    IIapi_wait(wtp)
connHandle = cop.co_connHandle
tranHandle = cop.co_tranHandle

##  disconnect
print(f'{script}: releasing connection')
dcp = IIAPI_DISCONNPARM()
dcp.dc_genParm.gp_callback = None
dcp.dc_genParm.gp_closure = None
dcp.dc_connHandle = connHandle
IIapi_disconnect(dcp)
while not dcp.dc_genParm.gp_completed:
    IIapi_wait(wtp)

##  set connection parameter
print(f'{script}: set connection parameter')
scp = IIAPI_SETCONPRMPARM()
scp.sc_genParm.gp_callback = None
scp.sc_connHandle = envHandle
scp.sc_paramID = IIAPI_CP_DATE_FORMAT
longvalue = ctypes.c_long(IIAPI_CPV_DFRMT_YMD)
scp.sc_paramValue = ctypes.addressof(longvalue)
IIapi_setConnectParam(scp)
while not scp.sc_genParm.gp_completed:
    IIapi_wait(wtp)
connHandle = scp.sc_connHandle

##  connect with connection parameter...
print(f'{script}: establishing connection')
cop.co_genParm.gp_callback = None
cop.co_genParm.gp_closure = None
cop.co_target = target
cop.co_connHandle = connHandle
cop.co_tranHandle = None
cop.co_username = None
cop.co_password = None
cop.co_timeout = -1
IIapi_connect(cop)
while not cop.co_genParm.gp_completed:
    IIapi_wait(wtp)
connHandle = cop.co_connHandle
tranHandle = cop.co_tranHandle

##  abort the connection
print(f'{script}: abort connection')
abp = IIAPI_ABORTPARM()
abp.ab_genParm.gp_callback = None
abp.ab_genParm.gp_closure = None
abp.ab_connHandle = connHandle
IIapi_abort(abp)
while not abp.ab_genParm.gp_completed:
    IIapi_wait(wtp)
IIdemo_term(envHandle)
quit()
