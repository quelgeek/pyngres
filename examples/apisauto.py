#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisauto.py
##
##  Description:
##      Demonstrates using IIapi_autocommit().
##
##  Following actions are demonstrated:
##      Enable autocommit
##      Disable autocommit
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
    status = inp.in_status
    if status != IIAPI_ST_SUCCESS:
        print(f'Error: {status=} ({IIAPI_ST_MSG[status]})')
        quit()
    return inp.in_envHandle


def IIdemo_term():
    '''Terminate API access'''

    tmp = IIAPI_TERMPARM()

    print('IIdemo_init: shutting down API')
    IIapi_terminate(tmp)
    status = tmp.tm_status
    if status >= IIAPI_ST_ERROR:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')


def IIdemo_conn(target, envHandle):
    '''Open connection to target database'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    cop = IIAPI_CONNPARM()

    print('IIdemo_conn: establishing connection')
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
    status = cop.co_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    connHandle = cop.co_connHandle
    return connHandle


def IIdemo_disconn(connHandle):
    '''Release DBMS connection'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    dcp = IIAPI_DISCONNPARM()

    print('IIdemo_disconn: releasing connection')
    dcp.dc_genParm.gp_callback = None
    dcp.dc_genParm.gp_closure = None
    dcp.dc_connHandle = connHandle
    IIapi_disconnect(dcp)
    while not dcp.dc_genParm.gp_completed:
        IIapi_wait(wtp)
    status = dcp.dc_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    connHandle = None
    return connHandle


def IIdemo_query(connHandle, tranHandle, queryText):
    '''Execute SQL statement taking no parameters and returning no rows'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    qyp = IIAPI_QUERYPARM()

    print(f'IIdemo_query: {queryText}')
    ##  call IIapi_query to execute statement
    qyp.qy_genParm.gp_callback = None
    qyp.qy_genParm.gp_closure = None
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_QUERY
    qyp.qy_queryText = queryText
    qyp.qy_parameters = False
    qyp.qy_tranHandle = tranHandle
    qyp.qy_stmtHandle = None
    IIapi_query(qyp)
    while not qyp.qy_genParm.gp_completed:
        IIapi_wait(wtp)
    status = qyp.qy_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    stmtHandle = qyp.qy_stmtHandle
    tranHandle = qyp.qy_tranHandle

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

##  enable autocommit
print(f'{script}: enable autocommit')
acp = IIAPI_AUTOPARM()
acp.ac_genParm.gp_callback = None
acp.ac_genParm.gp_closure = None
acp.ac_connHandle = connHandle
acp.ac_tranHandle = None
IIapi_autocommit(acp)
while not acp.ac_genParm.gp_completed:
    IIapi_wait(wtp)
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
    b')'
)
IIdemo_query(connHandle, tranHandle, sql_create)

sql_drop = b'DROP TABLE api_demo_auto'
IIdemo_query(connHandle, tranHandle, sql_drop)

##  disable autocommit
print(f'{script}: disable autocommit')
acp.ac_connHandle = None
acp.ac_tranHandle = tranHandle
IIapi_autocommit(acp)
while not acp.ac_genParm.gp_completed:
    IIapi_wait(wtp)
status = acp.ac_genParm.gp_status
if status != IIAPI_ST_SUCCESS:
    print(f'{status=} ({IIAPI_ST_MSG[status]})')
    quit()

dcp = IIdemo_disconn(connHandle)
tmp = IIdemo_term()
quit()
