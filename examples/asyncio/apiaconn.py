#!/usr/bin/env python

##  Copyright (c) 2024 Roy Hann

##  Name: apiaconn.py
##
##  Description:
##  	Demonstrates using IIapi_connect(),IIapi_setConnectParam(),
##  	IIapi_disconnect() and IIapi_abort() with Python asyncio.
##      Modelled on code supplied by Actian in II_SYSTEM/ingres/demo/api,
##      with the addition of a concurrent task (spin) to illustrate (or at
##      least suggest) the benefit of using asynchronous OpenAPI
##
##  Following actions are demonstrated
##  	Connect (no conn parms)
##  	Disconnect
##  	Set Connection Parameters
##  	Connect (with conn parms)
##  	Abort Connection
##
##  Command syntax: python apiaconn.py [vnode::]dbname[/server_class]


import asyncio
from pyngres.asyncio import *
import ctypes
import sys
import random


def IIdemo_init():
    '''demonstrate how to initialize the Ingres OpenAPI'''

    print('IIdemo_init: initializing API')
    inp = IIAPI_INITPARM()
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


async def ingres_task( target,spinner ):
    ##  connect with no connection parameters
    print(f'{script}: task1 establishing connection')
    cop = IIAPI_CONNPARM()
    cop.co_target = target
    cop.co_connHandle = envHandle
    cop.co_type = IIAPI_CT_SQL
    cop.co_timeout = -1
    await IIapi_connect(cop)

    connHandle = cop.co_connHandle
    tranHandle = cop.co_tranHandle

    ##  disconnect
    print(f'{script}: task1 releasing connection')
    dcp = IIAPI_DISCONNPARM()
    dcp.dc_connHandle = connHandle
    await IIapi_disconnect(dcp)

    ##  set connection parameter
    print(f'{script}: set connection parameter')
    scp = IIAPI_SETCONPRMPARM()
    scp.sc_connHandle = envHandle
    scp.sc_paramID = IIAPI_CP_DATE_FORMAT
    longvalue = ctypes.c_long(IIAPI_CPV_DFRMT_YMD)
    scp.sc_paramValue = ctypes.addressof(longvalue)
    await IIapi_setConnectParam(scp)

    connHandle = scp.sc_connHandle

    ##  connect with connection parameter...
    print(f'{script}: task2 establishing connection')
    cop = IIAPI_CONNPARM()
    cop.co_target = target
    cop.co_connHandle = connHandle
    cop.co_type = IIAPI_CT_SQL
    cop.co_timeout = -1
    await IIapi_connect(cop)

    connHandle = cop.co_connHandle
    tranHandle = cop.co_tranHandle

    ##  abort the connection
    print(f'{script}: abort connection')
    abp = IIAPI_ABORTPARM()
    abp.ab_connHandle = connHandle
    await IIapi_abort(abp)

    ##  tell the spinner to terminate
    spinner.cancel()


async def spin():
    ##  display a spinning rotor until cancelled
    rotor = '-\\|/'
    i = 0
    while True:
        i = (i+1) % 4
        output = rotor[i] + ' ' + '\b\b'
        print(output, end='', flush=True)
        try:
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            return


##  main body 
async def main(target):
    spinner = asyncio.create_task( spin() )
    dbtask = asyncio.create_task( ingres_task(target,spinner) )
    await asyncio.gather( spinner,dbtask )


argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()

dbtarget = argv[1]
target = dbtarget.encode()

envHandle = IIdemo_init()
asyncio.run(main(target))
IIdemo_term(envHandle)
quit()
