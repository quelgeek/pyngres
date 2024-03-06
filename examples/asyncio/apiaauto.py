#!/usr/bin/env python

##  Copyright (c) 2024 Roy Hann

##  Name: apiaauto.py
##
##  Description:
##      Demonstrates using IIapi_autocommit().
##      Modelled on code supplied by Actian in II_SYSTEM/ingres/demo/api,
##      with the addition of a concurrent task (spin) to illustrate (or at
##      least suggest) the benefit of using asynchronous OpenAPI
##
##  Following actions are demonstrated:
##      Enable autocommit
##      Disable autocommit
##
##  Command syntax:
##      python apiaauto.py [vnode::]dbname[/server_class]


import asyncio
from pyngres.asyncio import *
import sys


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    IIapi_initialize(inp)
    status = inp.in_status

    envHandle = inp.in_envHandle
    return envHandle


def IIdemo_term(envHandle):
    '''Terminate API access'''

    rep = IIAPI_RELENVPARM()
    tmp = IIAPI_TERMPARM()

    rep.re_envHandle = envHandle
    print('IIdemo_term: releasing environment resources')
    IIapi_releaseEnv(rep)

    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)


async def IIdemo_conn(target, envHandle):
    '''Open connection to target database'''

    print('IIdemo_conn: establishing connection')
    cop = IIAPI_CONNPARM()
    cop.co_target = target
    cop.co_connHandle = envHandle
    cop.co_type = IIAPI_CT_SQL
    cop.co_timeout = -1
    await IIapi_connect(cop)
    status = cop.co_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    connHandle = cop.co_connHandle
    return connHandle


async def IIdemo_disconn(connHandle):
    '''Release DBMS connection'''

    print('IIdemo_disconn: releasing connection')
    dcp = IIAPI_DISCONNPARM()
    dcp.dc_connHandle = connHandle
    await IIapi_disconnect(dcp)
    status = dcp.dc_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    connHandle = None
    return connHandle


async def IIdemo_query(connHandle, tranHandle, queryText):
    '''Execute SQL statement taking no parameters and returning no rows'''

    print(f'IIdemo_query: {queryText}')
    qyp = IIAPI_QUERYPARM()
    ##  call IIapi_query to execute statement
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_QUERY
    qyp.qy_queryText = queryText
    qyp.qy_parameters = False
    qyp.qy_tranHandle = tranHandle
    await IIapi_query(qyp)
    status = qyp.qy_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    stmtHandle = qyp.qy_stmtHandle
    tranHandle = qyp.qy_tranHandle

    ##  call IIapi_getQueryInfo() to get results
    gqp = IIAPI_GETQINFOPARM()
    gqp.gq_stmtHandle = stmtHandle
    await IIapi_getQueryInfo(gqp)

    ##  call IIapi_close() to release resources
    clp = IIAPI_CLOSEPARM()
    clp.cl_stmtHandle = stmtHandle
    await IIapi_close(clp)


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

async def ingres_task( target,spinner ):
    envHandle = IIdemo_init()
    connHandle = await IIdemo_conn( target, envHandle )

    ##  enable autocommit
    print(f'{script}: enable autocommit')
    acp = IIAPI_AUTOPARM()
    acp.ac_connHandle = connHandle
    await IIapi_autocommit(acp)
    status = acp.ac_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    tranHandle = acp.ac_tranHandle

    ##  perform operations during autocommit
    sql_create = (
        b'CREATE TABLE api_demo_auto'
        b'( '
        b'name char(20) NOT NULL, '
        b'age i4 NOT NULL'
        b')')
    await IIdemo_query(connHandle, tranHandle, sql_create)

    sql_drop = b'DROP TABLE api_demo_auto'
    await IIdemo_query(connHandle, tranHandle, sql_drop)

    ##  disable autocommit
    print(f'{script}: disable autocommit')
    acp.ac_tranHandle = tranHandle
    await IIapi_autocommit(acp)
    status = acp.ac_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()

    dcp = await IIdemo_disconn(connHandle)
    IIdemo_term(envHandle)

    ##  tell the spinner to terminate
    spinner.cancel()


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
asyncio.run(main(target))
quit()
