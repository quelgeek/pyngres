#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apissell.py
##
##  Description:
##      Demonstrates using IIapi_query(), IIapi_getDescriptor(),
##      IIapi_getColumns(), IIapi_cancel() and IIapi_close().
##
##	SELECT data using a select loop.
##
##  Following actions are demonstrated in the main()
##      Issue query
##      Get query result descriptors
##      Get query results
##      Cancel query processing
##      Free query resources.
##
##  Command syntax:
##      python apissell.py [vnode::]dbname[/server_class]


from pyngres import *
import sys
import ctypes


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


def IIdemo_rollback(tranHandle):
    '''rollback current transaction and reset transaction handle'''

    rbp = IIAPI_ROLLBACKPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    print('IIdemo_rollback: rolling back transaction')
    rbp.rb_genParm.gp_callback = None
    rbp.rb_genParm.gp_closure = None
    rbp.rb_tranHandle = tranHandle
    rbp.rb_savePointHandle = None

    IIapi_rollback(rbp)

    while not rbp.rb_genParm.gp_completed:
        IIapi_wait(wtp)

    tranHandle = None
    return tranHandle


##  this is the main body of the demonstration code
argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()

queryText = b'SELECT table_name FROM iitables'
tablename_buffer = ctypes.addressof(ctypes.create_string_buffer(257))

qyp = IIAPI_QUERYPARM()
gdp = IIAPI_GETDESCRPARM()
gcp = IIAPI_GETCOLPARM()
cnp = IIAPI_CANCELPARM()
clp = IIAPI_CLOSEPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None

##  Issue query
print(f'{script}: select table names')

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

tranHandle = qyp.qy_tranHandle
stmtHandle = qyp.qy_stmtHandle

##  Get query result descriptors
print(f'{script} get descriptors...')
gdp.gd_genParm.gp_callback = None
gdp.gd_genParm.gp_closure = None
gdp.gd_stmtHandle = stmtHandle
gdp.gd_descriptorCount = 0
gdp.gd_descriptor = None

IIapi_getDescriptor(gdp)

while not gdp.gd_genParm.gp_completed:
    IIapi_wait(wtp)

##  Get query results
print(f'{script} get results')


gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = 1
gcp.gc_rowsReturned = 0
dataPtrArray = (IIAPI_DATAVALUE * gcp.gc_rowCount * gcp.gc_columnCount)()
gcp.gc_columnData = ctypes.cast(dataPtrArray, ctypes.POINTER(IIAPI_DATAVALUE))
gcp.gc_columnData[0].dv_value = tablename_buffer
gcp.gc_stmtHandle = stmtHandle

for i in range(5):
    IIapi_getColumns(gcp)

    while not gcp.gc_genParm.gp_completed:
        IIapi_wait(wtp)

    if gcp.gc_genParm.gp_status >= IIAPI_ST_NO_DATA:
        break

    ptr = gcp.gc_columnData[0].dv_value
    value = ctypes.string_at(ptr, 32)
    print(f'table_name: {value}')

##  Cancel query processing
print(f'{script}: cancel query')

cnp.cn_genParm.gp_callback = None
cnp.cn_genParm.gp_closure = None
cnp.cn_stmtHandle = stmtHandle

IIapi_cancel(cnp)

while not cnp.cn_genParm.gp_completed:
    IIapi_wait(wtp)

##  Free query resources
print(f'{script}: free query resources')

clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = stmtHandle

IIapi_close(clp)

while not clp.cl_genParm.gp_completed:
    IIapi_wait(wtp)

IIdemo_rollback(tranHandle)
IIdemo_disconn(connHandle)
IIdemo_term(envHandle)
quit()
