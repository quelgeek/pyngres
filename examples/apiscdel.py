#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apiscdel.py
##
##  Description:
##    	Demonstrates using IIapi_query(), IIapi_setDescriptor() and
##    	IIapi_putParams() to do a cursor positioned delete.
##
##  Following actions are demonstrated:
##    	Open cursor
##    	Position cursor on row
##    	Issue cursor delete statement
##    	Describe parameters
##    	Send parameter values
##    	Get delete statement results
##    	Free delete statement resources
##    	Get cursor results
##    	Free cursor resources
##
##  Command syntax:
##      python apiscdel.py [vnode::]dbname[/server_class]


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


def IIdemo_conn(target, envHandle):
    '''open connection to target database'''

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


def IIdemo_insert(connHandle, tranHandle):
    '''Insert rows into table'''

    insTBLInfo = [
        (b'Abraham, Barbara T.', 35),
        (b'Haskins, Jill G.', 56),
        (b'Poon, Jennifer C.', 50),
        (b'Thurman, Roberta F.', 32),
        (b'Wilson, Frank N.', 24),
    ]
    DEMO_TABLE_SIZE = len(insTBLInfo)

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    qyp = IIAPI_QUERYPARM()
    sdp = IIAPI_SETDESCRPARM()
    ppp = IIAPI_PUTPARMPARM()
    gqp = IIAPI_GETQINFOPARM()
    clp = IIAPI_CLOSEPARM()

    descrArray = (IIAPI_DESCRIPTOR * 2)()
    descrArrayPtr = ctypes.cast(descrArray, ctypes.POINTER(IIAPI_DESCRIPTOR))
    dataArray = (IIAPI_DATAVALUE * 2)()
    dataArrayPtr = ctypes.cast(dataArray, ctypes.POINTER(IIAPI_DATAVALUE))

    for row in range(DEMO_TABLE_SIZE):
        print('IIdemo_insert: execute insert')
        qyp.qy_connHandle = connHandle
        qyp.qy_queryType = IIAPI_QT_EXEC
        sql_execute = b'EXECUTE s0'
        qyp.qy_queryText = sql_execute
        qyp.qy_parameters = True
        qyp.qy_tranHandle = tranHandle
        qyp.qy_stmtHandle = None
        IIapi_query(qyp)
        while not qyp.qy_genParm.gp_completed:
            IIapi_wait(wtp)
        status = qyp.qy_genParm.gp_status
        if status != IIAPI_ST_SUCCESS:
            print(f'{status=} ({IIAPI_ST_MSG[status]})')
            quit()
        tranHandle = qyp.qy_tranHandle
        stmtHandle = qyp.qy_stmtHandle

        ##  describe query parameters
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
        status = sdp.sd_genParm.gp_status
        if status != IIAPI_ST_SUCCESS:
            print(f'{status=} ({IIAPI_ST_MSG[status]})')
            quit()

        ##  send query parameters
        ppp.pp_genParm.gp_callback = None
        ppp.pp_genParm.gp_closure = None
        ppp.pp_stmtHandle = stmtHandle
        ppp.pp_parmCount = 2
        ppp.pp_moreSegments = False

        ppp.pp_parmData = dataArrayPtr
        ppp.pp_parmData[0].dv_null = False
        ppp.pp_parmData[0].dv_length = len(insTBLInfo[row][0])
        dv_value1 = ctypes.create_string_buffer(insTBLInfo[row][0])
        ppp.pp_parmData[0].dv_value = ctypes.addressof(dv_value1)

        ppp.pp_parmData[1].dv_null = False
        ppp.pp_parmData[1].dv_length = ctypes.sizeof(ctypes.c_int)
        dv_value2 = ctypes.c_int(insTBLInfo[row][1])
        ppp.pp_parmData[1].dv_value = ctypes.addressof(dv_value2)
        IIapi_putParms(ppp)
        while not ppp.pp_genParm.gp_completed:
            IIapi_wait(wtp)
        status = ppp.pp_genParm.gp_status
        if status != IIAPI_ST_SUCCESS:
            print(f'{status=} ({IIAPI_ST_MSG[status]})')
            quit()

        ##  get insert results
        gqp.gq_genParm.gp_callback = None
        gqp.gq_genParm.gp_closure = None
        gqp.gq_stmtHandle = stmtHandle
        IIapi_getQueryInfo(gqp)
        while not gqp.gq_genParm.gp_completed:
            IIapi_wait(wtp)

        ##  free statement resources
        clp.cl_genParm.gp_callback = None
        clp.cl_genParm.gp_closure = None
        clp.cl_stmtHandle = stmtHandle
        IIapi_close(clp)
        while not clp.cl_genParm.gp_completed:
            IIapi_wait(wtp)
    return tranHandle


def IIdemo_rollback(tranHandle):
    '''rollback current transaction'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    rbp = IIAPI_ROLLBACKPARM()

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
    return None


def IIdemo_term(envHandle):
    '''Terminate API access'''

    rep = IIAPI_RELENVPARM()
    tmp = IIAPI_TERMPARM()

    rep.re_envHandle = envHandle
    print('IIdemo_term: releasing environment resources')
    IIapi_releaseEnv(rep)

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

sql_create = (
    b'CREATE TABLE api_demo_cdel'
    b'( '
    b'name char(20) NOT NULL, '
    b'age i4 NOT NULL'
    b')'
)

openText = b'SELECT * ' b'FROM api_demo_cdel ' b'FOR UPDATE'

sql_prepare = b'PREPARE s0 FROM ' b'INSERT INTO api_demo_cdel ' b'VALUES( ?, ? )'

deleteText = b'DELETE ' b'FROM api_demo_cdel '

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
tranHandle = IIdemo_query(connHandle, tranHandle, sql_create)
tranHandle = IIdemo_query(connHandle, tranHandle, sql_prepare)
tranHandle = IIdemo_insert(connHandle, tranHandle)

print(f'{script}: open cursor')
qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_OPEN
qyp.qy_queryText = openText
qyp.qy_parameters = False
qyp.qy_tranHandle = tranHandle
qyp.qy_stmtHandle = None

IIapi_query(qyp)
while not qyp.qy_genParm.gp_completed:
    IIapi_wait(wtp)
tranHandle = qyp.qy_tranHandle
stmtHandle = cursorID = qyp.qy_stmtHandle

##  get cursor row descriptor
gdp.gd_genParm.gp_callback = None
gdp.gd_genParm.gp_closure = None
gdp.gd_stmtHandle = cursorID
gdp.gd_descriptorCount = 0
gdp.gd_descriptor = None
IIapi_getDescriptor(gdp)
while not gdp.gd_genParm.gp_completed:
    IIapi_wait(wtp)

##  position cursor on first row
dataArray = (IIAPI_DATAVALUE * 2)()
dataArrayPtr = ctypes.cast(dataArray, ctypes.POINTER(IIAPI_DATAVALUE))
gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = gdp.gd_descriptorCount
gcp.gc_columnData = dataArrayPtr

var1 = ctypes.create_string_buffer(33)
var2 = ctypes.c_int()
gcp.gc_columnData[0].dv_value = ctypes.addressof(var1)
gcp.gc_columnData[1].dv_value = ctypes.addressof(var2)
gcp.gc_stmtHandle = cursorID
gcp.gc_moreSegments = False

while True:
    print(f'{script}: next row')
    IIapi_getColumns(gcp)
    while not gcp.gc_genParm.gp_completed:
        IIapi_wait(wtp)
    status = gcp.gc_genParm.gp_status
    if status >= IIAPI_ST_NO_DATA:
        break

    print(f'{script}: delete row')
    qyp.qy_genParm.gp_callback = None
    qyp.qy_genParm.gp_closure = None
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_CURSOR_DELETE
    qyp.qy_queryText = deleteText
    qyp.qy_parameters = True
    qyp.qy_tranHandle = tranHandle
    qyp.qy_stmtHandle = None

    IIapi_query(qyp)
    while not qyp.qy_genParm.gp_completed:
        IIapi_wait(wtp)
    status = qyp.qy_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')

    ##  describe parameters - cursor handle
    descrArray = (IIAPI_DESCRIPTOR * 1)()
    descrArrayPtr = ctypes.cast(descrArray, ctypes.POINTER(IIAPI_DESCRIPTOR))
    sdp.sd_genParm.gp_callback = None
    sdp.sd_genParm.gp_closure = None
    sdp.sd_stmtHandle = qyp.qy_stmtHandle
    sdp.sd_descriptorCount = 1
    sdp.sd_descriptor = descrArrayPtr

    sdp.sd_descriptor[0].ds_dataType = IIAPI_HNDL_TYPE
    sdp.sd_descriptor[0].ds_nullable = False
    sdp.sd_descriptor[0].ds_length = ctypes.sizeof(II_PTR)
    sdp.sd_descriptor[0].ds_precision = 0
    sdp.sd_descriptor[0].ds_scale = 0
    sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_SVCPARM
    sdp.sd_descriptor[0].ds_columnName = None

    IIapi_setDescriptor(sdp)
    while not sdp.sd_genParm.gp_completed:
        IIapi_wait(wtp)
    status = sdp.sd_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')

    ##  send parameters - cursor handle
    cursorArray = (IIAPI_DATAVALUE * 1)()
    cursorArrayPtr = ctypes.cast(cursorArray, ctypes.POINTER(IIAPI_DATAVALUE))
    ppp.pp_genParm.gp_callback = None
    ppp.pp_genParm.gp_closure = None
    ppp.pp_stmtHandle = qyp.qy_stmtHandle
    ppp.pp_parmCount = sdp.sd_descriptorCount
    ppp.pp_parmData = cursorArrayPtr
    ppp.pp_moreSegments = False

    ppp.pp_parmData[0].dv_null = False
    length = ctypes.sizeof(II_PTR)
    ppp.pp_parmData[0].dv_length = length
    cursorIDptr = II_PTR(cursorID)
    dv_value = ctypes.addressof(cursorIDptr)
    ppp.pp_parmData[0].dv_value = dv_value

    IIapi_putParms(ppp)
    while not ppp.pp_genParm.gp_completed:
        IIapi_wait(wtp)
    status = ppp.pp_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()

    ##  get delete results
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = qyp.qy_stmtHandle
    IIapi_getQueryInfo(gqp)
    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  free delete resourceclose
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = qyp.qy_stmtHandle
    IIapi_close(clp)
    while not clp.cl_genParm.gp_completed:
        IIapi_wait(wtp)

##  get cursor fetch results
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = cursorID

IIapi_getQueryInfo(gqp)
while not gqp.gq_genParm.gp_completed:
    IIapi_wait(wtp)

##  free cursor resources
print(f'{script}: close cursor')
clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = cursorID

IIapi_close(clp)
while not clp.cl_genParm.gp_completed:
    IIapi_wait(wtp)

tranHandle = IIdemo_rollback(tranHandle)
IIdemo_disconn(connHandle)
IIdemo_term(envHandle)
