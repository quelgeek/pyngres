#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apiscomm.c
##
##  Description:
## 	    Demonstrates using IIapi_commit()
##
##  Following actions are demonstrated in the main()
## 	    Commit transaction
##
##  Command syntax:
##      python apisauto.py [vnode::]dbname[/server_class]


from pyngres import *
import sys


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    IIapi_initialize(inp)
    return inp.in_envHandle


def IIdemo_term():
    '''Terminate API access'''

    tmp = IIAPI_TERMPARM()
    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)


def IIdemo_conn(target, envHandle):
    '''Open connection to target database'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    cop = IIAPI_CONNPARM()

    print('IIdemo_conn: establishing connection')

    cop.co_genParm.gp_callback = None
    cop.co_genParm.gp_closure = None
    cop.co_target = target
    cop.co_connHandle = None
    cop.co_tranHandle = None
    cop.co_username = None
    cop.co_password = None
    cop.co_timeout = -1

    IIapi_connect(cop)

    while not cop.co_genParm.gp_completed:
        IIapi_wait(wtp)

    connHandle = cop.co_connHandle
    return connHandle


def IIdemo_disconn(connHandle):
    '''Release DBMS connection'''

    dcp = IIAPI_DISCONNPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    print('IIdemo_disconn: releasing connection')

    dcp.dc_genParm.gp_callback = None
    dcp.dc_genParm.gp_closure = None
    dcp.dc_connHandle = connHandle

    IIapi_disconnect(dcp)

    while not dcp.dc_genParm.gp_completed:
        IIapi_wait(wtp)

    connHandle = None
    return connHandle


def IIdemo_query(connHandle, tranHandle, queryText):
    '''Execute SQL statement taking no parameters and returning no rows'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    qyp = IIAPI_QUERYPARM()

    ##  call IIapi_query to execute statement
    qyp.qy_genParm.gp_callback = None
    qyp.qy_genParm.gp_closure = None
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_QUERY
    qyp.qy_queryText = queryText
    qyp.qy_parameters = False
    qyp.qy_tranHandle = tranHandle
    qyp.qy_stmtHandle = None

    print(f'IIdemo_query: {queryText}')

    IIapi_query(qyp)

    while not qyp.qy_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  return transaction handle
    tranHandle = qyp.qy_tranHandle
    stmtHandle = qyp.qy_stmtHandle

    ##  call IIapi_getQueryInfo() to get results
    gqp = IIAPI_GETQINFOPARM()
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle

    IIapi_getQueryInfo(gqp)

    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  call IIapi_close() to release resources
    clp = IIAPI_CLOSEPARM()
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = stmtHandle

    IIapi_close(clp)

    while not clp.cl_genParm.gp_completed:
        IIapi_wait(wtp)

    return tranHandle


##  this is the main body of the demonstration code
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
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None
queryText = b'BEGIN TRANSACTION'
tranHandle = IIdemo_query(connHandle, tranHandle, queryText)

print(f'{script}: commiting a transaction')

cmp = IIAPI_COMMITPARM()
cmp.cm_genParm.gp_callback = None
cmp.cm_genParm.gp_closure = None
cmp.cm_tranHandle = tranHandle

IIapi_commit(cmp)

while not cmp.cm_genParm.gp_completed:
    IIapi_wait(wtp)

IIdemo_disconn(connHandle)
IIdemo_term()
quit()
