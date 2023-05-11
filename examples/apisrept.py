#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisrept.py
##
##  Description:
##      Demonstrates using IIapi_query(), IIapi_setDescriptor() and
##      IIapi_putParams() to define and execute a repeated statement.
##
##  Following actions are demonstrated in the main()
##      Define repeated insert.
##      Execute repeated insert.
##
##  Command syntax:
##      python apisrept.py [vnode::]dbname[/server_class]


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


##  this is the main body of the demonstration code
argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()

createTBLText = (
    b'CREATE TABLE api_demo_rept'
    b'( '
    b'  name char(20) NOT NULL, '
    b'  age i4 NOT NULL'
    b')'
)
insertText = b'INSERT INTO api_demo_rept VALUES ( $0 = ~V , $1 = ~V )'
insTBLInfo = [
    (b'Abraham, Barbara T.', 35),
    (b'Haskins, Jill G.', 56),
    (b'Poon, Jennifer C.', 50),
    (b'Thurman, Roberta F.', 32),
    (b'Wilson, Frank N.', 24),
]


qyp = IIAPI_QUERYPARM()
sdp = IIAPI_SETDESCRPARM()
ppp = IIAPI_PUTPARMPARM()
gqp = IIAPI_GETQINFOPARM()
clp = IIAPI_CLOSEPARM()
gqp = IIAPI_GETQINFOPARM()
clp = IIAPI_CLOSEPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1


envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None
tranHandle = IIdemo_query(connHandle, tranHandle, createTBLText)

##  define repeat statement
qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_DEF_REPEAT_QUERY
qyp.qy_queryText = insertText
qyp.qy_parameters = True
qyp.qy_tranHandle = tranHandle
qyp.qy_stmtHandle = None

IIapi_query(qyp)

while not qyp.qy_genParm.gp_completed:
    IIapi_wait(wtp)

tranHandle = qyp.qy_tranHandle
stmtHandle = qyp.qy_stmtHandle

##  describe query parameters
descrArray = (IIAPI_DESCRIPTOR * 3)()
descrArrayPtr = ctypes.cast(descrArray, ctypes.POINTER(IIAPI_DESCRIPTOR))
sdp.sd_genParm.gp_callback = None
sdp.sd_genParm.gp_closure = None
sdp.sd_stmtHandle = stmtHandle
sdp.sd_descriptorCount = 2
sdp.sd_descriptor = descrArrayPtr

sdp.sd_descriptor[0].ds_dataType = IIAPI_CHA_TYPE
sdp.sd_descriptor[0].ds_nullable = False
sdp.sd_descriptor[0].ds_length = 20
sdp.sd_descriptor[0].ds_precision = 0
sdp.sd_descriptor[0].ds_scale = 0
sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_QPARM
sdp.sd_descriptor[0].ds_columnName = None

sdp.sd_descriptor[1].ds_dataType = IIAPI_INT_TYPE
sdp.sd_descriptor[1].ds_nullable = False
sdp.sd_descriptor[1].ds_length = ctypes.sizeof(II_INT4)
sdp.sd_descriptor[1].ds_precision = 0
sdp.sd_descriptor[1].ds_scale = 0
sdp.sd_descriptor[1].ds_columnType = IIAPI_COL_QPARM
sdp.sd_descriptor[1].ds_columnName = None

IIapi_setDescriptor(sdp)

while not sdp.sd_genParm.gp_completed:
    IIapi_wait(wtp)

##  send query parameters
DEMO_TABLE_SIZE = len(insTBLInfo)
dataArray = (IIAPI_DATAVALUE * 3)()
dataArrayPtr = ctypes.cast(dataArray, ctypes.POINTER(IIAPI_DATAVALUE))
ppp.pp_genParm.gp_callback = None
ppp.pp_genParm.gp_closure = None
ppp.pp_stmtHandle = stmtHandle
ppp.pp_parmCount = 2
ppp.pp_parmData = dataArrayPtr
ppp.pp_moreSegments = 0

name = ctypes.create_string_buffer(insTBLInfo[0][0])
ppp.pp_parmData[0].dv_null = False
ppp.pp_parmData[0].dv_length = 20
ppp.pp_parmData[0].dv_value = ctypes.addressof(name)

age = II_INT4(insTBLInfo[0][1])
ppp.pp_parmData[1].dv_null = False
ppp.pp_parmData[1].dv_length = ctypes.sizeof(II_INT4)
ppp.pp_parmData[1].dv_value = ctypes.addressof(age)

IIapi_putParms(ppp)

while not ppp.pp_genParm.gp_completed:
    IIapi_wait(wtp)

##  Get results of defining repeat statement
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle

IIapi_getQueryInfo(gqp)

while not gqp.gq_genParm.gp_completed:
    IIapi_wait(wtp)

if gqp.gq_mask & IIAPI_GQ_REPEAT_QUERY_ID:
    reptHandle = II_PTR(gqp.gq_repeatQueryHandle)

##  free resources
clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = stmtHandle

IIapi_close(clp)

while not clp.cl_genParm.gp_completed:
    IIapi_wait(wtp)

##  Execute (repeated) insert
for row in range(DEMO_TABLE_SIZE):
    print(f'{script}: execute repeated insert')

    qyp.qy_genParm.gp_callback = None
    qyp.qy_genParm.gp_closure = None
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_EXEC_REPEAT_QUERY
    qyp.qy_queryText = None
    qyp.qy_parameters = True
    qyp.qy_tranHandle = tranHandle
    qyp.qy_stmtHandle = None

    IIapi_query(qyp)

    while not qyp.qy_genParm.gp_completed:
        IIapi_wait(wtp)

    stmtHandle = qyp.qy_stmtHandle

    ##  describe query parameters
    sdp.sd_genParm.gp_callback = None
    sdp.sd_genParm.gp_closure = None
    sdp.sd_stmtHandle = stmtHandle
    sdp.sd_descriptorCount = 3
    sdp.sd_descriptor = descrArrayPtr

    sdp.sd_descriptor[0].ds_dataType = IIAPI_HNDL_TYPE
    sdp.sd_descriptor[0].ds_length = ctypes.sizeof(II_PTR)
    sdp.sd_descriptor[0].ds_nullable = False
    sdp.sd_descriptor[0].ds_precision = 0
    sdp.sd_descriptor[0].ds_scale = 0
    sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_SVCPARM
    sdp.sd_descriptor[0].ds_columnName = None

    sdp.sd_descriptor[1].ds_dataType = IIAPI_CHA_TYPE
    sdp.sd_descriptor[1].ds_nullable = False
    sdp.sd_descriptor[1].ds_length = 20
    sdp.sd_descriptor[1].ds_precision = 0
    sdp.sd_descriptor[1].ds_scale = 0
    sdp.sd_descriptor[1].ds_columnType = IIAPI_COL_QPARM
    sdp.sd_descriptor[1].ds_columnName = None

    sdp.sd_descriptor[2].ds_dataType = IIAPI_INT_TYPE
    sdp.sd_descriptor[2].ds_nullable = False
    sdp.sd_descriptor[2].ds_length = ctypes.sizeof(II_INT4)
    sdp.sd_descriptor[2].ds_precision = 0
    sdp.sd_descriptor[2].ds_scale = 0
    sdp.sd_descriptor[2].ds_columnType = IIAPI_COL_QPARM
    sdp.sd_descriptor[2].ds_columnName = None

    IIapi_setDescriptor(sdp)

    while not sdp.sd_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  send the query parameters
    ppp.pp_genParm.gp_callback = None
    ppp.pp_genParm.gp_closure = None
    ppp.pp_stmtHandle = stmtHandle
    ppp.pp_parmCount = 3
    ppp.pp_parmData = dataArrayPtr
    ppp.pp_moreSegments = 0

    ppp.pp_parmData[0].dv_null = False
    ppp.pp_parmData[0].dv_length = ctypes.sizeof(II_PTR)
    ppp.pp_parmData[0].dv_value = ctypes.addressof(reptHandle)

    name = ctypes.create_string_buffer(insTBLInfo[row][0])
    ppp.pp_parmData[1].dv_null = False
    ppp.pp_parmData[1].dv_length = len(name.value)
    ppp.pp_parmData[1].dv_value = ctypes.addressof(name)

    age = II_INT4(insTBLInfo[row][1])
    ppp.pp_parmData[2].dv_null = False
    ppp.pp_parmData[2].dv_length = ctypes.sizeof(II_INT4)
    ppp.pp_parmData[2].dv_value = ctypes.addressof(age)

    IIapi_putParms(ppp)

    while not ppp.pp_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  Get results of executing repeat statement
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle

    IIapi_getQueryInfo(gqp)

    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  free resources
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = stmtHandle

    IIapi_close(clp)

    while not clp.cl_genParm.gp_completed:
        IIapi_wait(wtp)

IIdemo_rollback(stmtHandle)
IIdemo_disconn(connHandle)
IIdemo_term(envHandle)
quit()
