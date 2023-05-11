#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisroll.py
##
##  Description:
##      Demonstrates using IIapi_savePoint() and IIapi_rollback()
##
##  Following actions are demonstrated in the main()
##      Create savepoint
##      Rollback to savepoint
##      Rollback transaction.
##
##  Command syntax:
##      python apisroll.py [vnode::]dbname[/server_class]


from pyngres import *
import ctypes
import sys


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_2
    inp.in_timeout = -1

    IIapi_initialize(inp)
    status = inp.in_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
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


def IIdemo_conn(target, envHandle):
    '''open connection with target Database'''

    cop = IIAPI_CONNPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    print('IIdemo_conn: establishing connection')
    cop.co_genParm.gp_callback = None
    cop.co_genParm.gp_closure = None
    cop.co_target = target
    cop.co_connHandle = envHandle
    cop.co_type = IIAPI_CT_SQL
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

    qyp = IIAPI_QUERYPARM()
    gqp = IIAPI_GETQINFOPARM()
    clp = IIAPI_CLOSEPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    print(f'IIdemo_query: {queryText}')

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

    ##  Return transaction handle
    tranHandle = qyp.qy_tranHandle
    stmtHandle = qyp.qy_stmtHandle

    ##  Call IIapi_getQueryInfo() to get results
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle

    IIapi_getQueryInfo(gqp)

    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  Call IIapi_close() to release resources
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


queryText = b'CREATE TABLE api_demo_roll ( c1 integer )'


spp = IIAPI_SAVEPTPARM()
rbp = IIAPI_ROLLBACKPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1


envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None
tranHandle = IIdemo_query(connHandle, tranHandle, queryText)

##  create savepoint
(f'{script}: creating savepoint')

spp.sp_genParm.gp_callback = None
spp.sp_genParm.gp_closure = None
spp.sp_tranHandle = tranHandle
savePtName = ctypes.create_string_buffer(b'save_point')
spp.sp_savePoint = ctypes.addressof(savePtName)

IIapi_savePoint(spp)

while not spp.sp_genParm.gp_completed:
    IIapi_wait(wtp)

savePointHandle = spp.sp_savePointHandle

##  rollback to savepoint
print(f'{script}: rolling back to savepoint')

rbp.rb_genParm.gp_callback = None
rbp.rb_genParm.gp_closure = None
rbp.rb_tranHandle = tranHandle
rbp.rb_savePointHandle = savePointHandle

IIapi_rollback(rbp)

while not rbp.rb_genParm.gp_completed:
    IIapi_wait(wtp)

##  rollback transaction
print(f'{script}: rolling back transaction')

rbp.rb_genParm.gp_callback = None
rbp.rb_genParm.gp_closure = None
rbp.rb_tranHandle = tranHandle
rbp.rb_savePointHandle = None

IIapi_rollback(rbp)

while not rbp.rb_genParm.gp_completed:
    IIapi_wait(wtp)

IIdemo_disconn(connHandle)
IIdemo_term(envHandle)
quit()
