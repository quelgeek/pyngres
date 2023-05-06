#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisparm.py
##
##  Description:
##      Demonstrates using IIapi_setDescriptor() and IIapi_putParams().
##
##  Following actions are demonstrated in the main
##      execute a prepared statement
##      describe parameter values
##      set parameter values on a prepared statement
##      get statement results
##      free query resources
##
##  Command syntax:
##      python apisparm.py [vnode::]dbname[/server_class]


from pyngres import *
import ctypes
import struct
import sys


def IIdemo_init():
    '''Initialize API access'''

    print('IIdemo_init: initializing API')
    inp = IIAPI_INITPARM()
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1

    IIapi_initialize(inp)

    envHandle = inp.in_envHandle
    return envHandle


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


def IIdemo_query(connHandle, tranHandle, queryText):
    '''Execute SQL statement taking no parameters and returning no rows'''

    qyp = IIAPI_QUERYPARM()
    gqp = IIAPI_GETQINFOPARM()
    clp = IIAPI_CLOSEPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    print(f'IIdemo_query: {queryText}')

    ##  Call IIapi_query to execute statement
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


def IIdemo_rollback(tranHandle):
    '''rollback current transaction'''

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


def IIdemo_term():
    '''Terminate API access'''

    tmp = IIAPI_TERMPARM()

    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)


##  this is the main body of the demonstration code
argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()

createTBLText = (
    b'CREATE TABLE api_demo_parm'
    b'( '
    b'name char(20) NOT NULL, '
    b'age i4 NOT NULL'
    b')'
)

prepText = b'PREPARE s0 FROM ' b'INSERT INTO api_demo_parm ' b'VALUES( ?, ? )'

execText = b'EXECUTE s0'

insTBLInfo = [
    (b'Abraham, Barbara T.', 35),
    (b'Haskins, Jill G.', 56),
    (b'Poon, Jennifer C.', 50),
    (b'Thurman, Roberta F.', 32),
    (b'Wilson, Frank N.', 24),
]
DEMO_TABLE_SIZE = len(insTBLInfo)
descrArray = (IIAPI_DESCRIPTOR * 2)()
descrArrayPtr = ctypes.cast(descrArray, ctypes.POINTER(IIAPI_DESCRIPTOR))
dataArray = (IIAPI_DATAVALUE * 2)()
dataArrayPtr = ctypes.cast(dataArray, ctypes.POINTER(IIAPI_DATAVALUE))

clp = IIAPI_CLOSEPARM()
sdp = IIAPI_SETDESCRPARM()
ppp = IIAPI_PUTPARMPARM()
qyp = IIAPI_QUERYPARM()
gdp = IIAPI_GETDESCRPARM()
gcp = IIAPI_GETCOLPARM()
gqp = IIAPI_GETQINFOPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None
tranHandle = IIdemo_query(connHandle, tranHandle, createTBLText)
tranHandle = IIdemo_query(connHandle, tranHandle, prepText)

for row in range(DEMO_TABLE_SIZE):
    ##  Execute prepared insert statement
    print(f'{script}: execute insert')

    qyp.qy_genParm.gp_callback = None
    qyp.qy_genParm.gp_closure = None
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_EXEC
    qyp.qy_queryText = execText
    qyp.qy_parameters = True
    qyp.qy_tranHandle = tranHandle
    qyp.qy_stmtHandle = None

    IIapi_query(qyp)

    while not qyp.qy_genParm.gp_completed:
        IIapi_wait(wtp)

    tranHandle = qyp.qy_tranHandle
    stmtHandle = qyp.qy_stmtHandle

    ##  Describe query parameters
    sdp.sd_genParm.gp_callback = None
    sdp.sd_genParm.gp_closure = None
    sdp.sd_stmtHandle = stmtHandle
    sdp.sd_descriptorCount = 2

    sdp.sd_descriptor = descrArrayPtr
    sdp.sd_descriptor[0].ds_dataType = IIAPI_CHA_TYPE
    sdp.sd_descriptor[0].ds_nullable = False
    sdp.sd_descriptor[0].ds_length = len(insTBLInfo[row][0])
    sdp.sd_descriptor[0].ds_precision = 0
    sdp.sd_descriptor[0].ds_scale = 0
    sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_QPARM
    sdp.sd_descriptor[0].ds_columnName = None

    sdp.sd_descriptor[1].ds_dataType = IIAPI_INT_TYPE
    sdp.sd_descriptor[1].ds_nullable = False
    sdp.sd_descriptor[1].ds_length = ctypes.sizeof(ctypes.c_int)
    sdp.sd_descriptor[1].ds_precision = 0
    sdp.sd_descriptor[1].ds_scale = 0
    sdp.sd_descriptor[1].ds_columnType = IIAPI_COL_QPARM
    sdp.sd_descriptor[1].ds_columnName = None

    IIapi_setDescriptor(sdp)

    while not sdp.sd_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  Send query parameter values
    ppp.pp_genParm.gp_callback = None
    ppp.pp_genParm.gp_closure = None
    ppp.pp_stmtHandle = stmtHandle
    ppp.pp_parmCount = 2

    ppp.pp_parmData = dataArrayPtr
    ppp.pp_parmData[0].dv_null = False
    ppp.pp_parmData[0].dv_length = len(insTBLInfo[row][0])
    dv_value1 = ctypes.create_string_buffer(insTBLInfo[row][0])
    ppp.pp_parmData[0].dv_value = ctypes.addressof(dv_value1)

    ppp.pp_parmData[1].dv_null = False
    ppp.pp_parmData[1].dv_length = ctypes.sizeof(ctypes.c_int)
    dv_value2 = ctypes.c_int(insTBLInfo[row][1])
    ppp.pp_parmData[1].dv_value = ctypes.addressof(dv_value2)
    ppp.pp_moreSegments = 0

    IIapi_putParms(ppp)

    while not ppp.pp_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  Get statement results
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle

    IIapi_getQueryInfo(gqp)

    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  Release statement resources
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = stmtHandle

    IIapi_close(clp)

    while not clp.cl_genParm.gp_completed:
        IIapi_wait(wtp)

tranHandle = IIdemo_rollback(tranHandle)
IIdemo_disconn(connHandle)
IIdemo_term()
quit()
