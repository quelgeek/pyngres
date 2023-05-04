#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisblob.py
##
##  Description:
##       Demonstrates using IIapi_setEnvParam(), IIapi_query(),
##	     IIapi_getDescriptor(), IIapi_getColumns(), IIapi_getQueryInfo(),
##	     IIapi_close(), and error handling.
##
##       SELECT row with blob data.
##
##  Following actions are demonstrated in the main()
##	Set EnvParam for segment length
##       Issue query
##       Get query result descriptors
##       Get query results
##       Close query processing
##       Free query resources.
##
##  Command syntax:
##      python apisblob.py [vnode::]dbname[/server_class]


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
    rep.re_envHandle = envHandle
    print('IIdemo_term: releasing environment resources')
    IIapi_releaseEnv(rep)

    tmp = IIAPI_TERMPARM()
    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)


def IIdemo_conn(target, envHandle):
    '''open connection with target Database'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    cop = IIAPI_CONNPARM()

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
    status = cop.co_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
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

    print('IIdemo_query: Executing Query')
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
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle

    IIapi_getQueryInfo(gqp)
    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)

    ##  call IIapi_close() to release resources
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = stmtHandle

    IIapi_close(clp)
    while not clp.cl_genParm.gp_completed:
        IIapi_wait(wtp)

    return connHandle, tranHandle


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

    return None


def IIdemo_checkError(genParm):
    '''check status of API function call and process error information'''

    gep = IIAPI_GETEINFOPARM()

    print('IIdemo_checkError: ...')
    status = genParm.gp_status
    if status in IIAPI_ST_MSG.keys():
        print(f'tgp_status = {IIAPI_ST_MSG[status]}')
    else:
        print(f'Unknown IIAPI_STATUS value {status}')

    ##  check for error information
    if not genParm.gp_errorHandle:
        return

    gep.ge_errorHandle = genParm.gp_errorHandle
    while True:
        ##  invoke API function call
        IIapi_getErrorInfo(gep)

        ##  break out of the loop if no data or failed
        if gep.ge_status != IIAPI_ST_SUCCESS:
            break

        ##  process result
        type = gep.ge_type
        if type == IIAPI_GE_ERROR:
            label = 'ERROR'
        elif type == IIAPI_GE_WARNING:
            label = 'WARNING'
        elif type == IIAPI_GE_MESSAGE:
            label = 'USER MESSAGE'
        else:
            label = f'unknown error type {type}'
        message = gep.ge_message if gep.ge_message else 'NULL'
        report = (
            f'Error info: {label} '
            f'{gep.ge_SQLSTATE} {gep.ge_errorCode:#0x} '
            f'{message}'
        )
        print(report)


def IIdemo_insert(connHandle, tranHandle):
    '''Insert rows into table'''

    blobdata = ctypes.c_char_p(
        b'test blob data segment 11111111111111111111111111111111111111111111111'
        b'2222222222222222222222222222222222222222222222222222222222222222222222'
        b'3333333333333333333333333333333333333333333333333333333333333333333333'
        b'4444444444444444444444444444444444444444444444444444444444444444444444'
        b'5555555555555555555555555555555555555555555555555555555555555555555555'
        b'6666666666666666666666666666666666666666666666666666666666666666666666'
        b'7777777777777777777777777777777777777777777777777777777777777777777777'
        b'8888888888888888888888888888888888888888888888888888888888888888888888'
        b'9999999999999999999999999999999999999999999999999999999999999999999999'
        b'end of blobdata segment '
    )
    insTBLInfo = [
        (b'Abraham, Barbara T.', b'comment field'),
        (b'Haskins, Jill G.', b'comment field'),
        (b'Poon, Jennifer C.', b'comment field'),
        (b'Thurman, Roberta F.', b'comment field'),
        (b'Wilson, Frank N.', b'comment field'),
    ]
    DEMO_TABLE_SIZE = len(insTBLInfo)
    insertText = b'INSERT INTO api_demo VALUES ( ~V , ~V , ~V )'

    qyp = IIAPI_QUERYPARM()
    sdp = IIAPI_SETDESCRPARM()
    ppp = IIAPI_PUTPARMPARM()
    gqp = IIAPI_GETQINFOPARM()
    clp = IIAPI_CLOSEPARM()

    print('IIdemo_insert: execute insert')
    qyp.qy_genParm.gp_callback = None
    qyp.qy_genParm.gp_closure = None
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_QUERY
    qyp.qy_queryText = insertText
    qyp.qy_parameters = True
    qyp.qy_tranHandle = tranHandle
    qyp.qy_stmtHandle = None

    IIapi_query(qyp)
    while not qyp.qy_genParm.gp_completed:
        IIapi_wait(wtp)
    status = qyp.qy_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(qyp.qy_genParm)

    tranHandle = qyp.qy_tranHandle
    stmtHandle = qyp.qy_stmtHandle

    ##  describe query parameters
    descrArray = (IIAPI_DESCRIPTOR * 3)()
    descrArrayPtr = ctypes.cast(descrArray, ctypes.POINTER(IIAPI_DESCRIPTOR))
    sdp.sd_genParm.gp_callback = None
    sdp.sd_genParm.gp_closure = None
    sdp.sd_stmtHandle = stmtHandle
    sdp.sd_descriptorCount = 3
    sdp.sd_descriptor = descrArrayPtr

    sdp.sd_descriptor[0].ds_dataType = IIAPI_CHA_TYPE
    sdp.sd_descriptor[0].ds_nullable = False
    sdp.sd_descriptor[0].ds_length = 20
    sdp.sd_descriptor[0].ds_precision = 0
    sdp.sd_descriptor[0].ds_scale = 0
    sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_QPARM
    sdp.sd_descriptor[0].ds_columnName = None

    sdp.sd_descriptor[1].ds_dataType = IIAPI_LVCH_TYPE
    sdp.sd_descriptor[1].ds_nullable = True
    sdp.sd_descriptor[1].ds_length = 3000
    sdp.sd_descriptor[1].ds_precision = 0
    sdp.sd_descriptor[1].ds_scale = 0
    sdp.sd_descriptor[1].ds_columnType = IIAPI_COL_QPARM
    sdp.sd_descriptor[1].ds_columnName = None

    sdp.sd_descriptor[2].ds_dataType = IIAPI_CHA_TYPE
    sdp.sd_descriptor[2].ds_nullable = False  # should really be True...
    sdp.sd_descriptor[2].ds_length = 20
    sdp.sd_descriptor[2].ds_precision = 0
    sdp.sd_descriptor[2].ds_scale = 0
    sdp.sd_descriptor[2].ds_columnType = IIAPI_COL_QPARM
    sdp.sd_descriptor[2].ds_columnName = None

    IIapi_setDescriptor(sdp)

    while not sdp.sd_genParm.gp_completed:
        IIapi_wait(wtp)
    status = sdp.sd_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(sdp.sd_genParm)

    ##  send query parameters
    dataArray = (IIAPI_DATAVALUE * 3)()
    dataArrayPtr = ctypes.cast(dataArray, ctypes.POINTER(IIAPI_DATAVALUE))
    ppp.pp_genParm.gp_callback = None
    ppp.pp_genParm.gp_closure = None
    ppp.pp_stmtHandle = stmtHandle
    ppp.pp_parmCount = 1
    ppp.pp_parmData = dataArrayPtr
    ppp.pp_parmData[0].dv_null = False
    ppp.pp_parmData[0].dv_length = len(insTBLInfo[0][0])
    dv_value1 = ctypes.create_string_buffer(insTBLInfo[0][0])
    ppp.pp_parmData[0].dv_value = ctypes.addressof(dv_value1)
    ppp.pp_moreSegments = False

    IIapi_putParms(ppp)

    while not ppp.pp_genParm.gp_completed:
        IIapi_wait(wtp)
    status = ppp.pp_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(ppp.pp_genParm)

    blobdata2 = ctypes.c_buffer(4100)
    blen = ctypes.c_short(len(blobdata.value))
    ctypes.memmove(ctypes.addressof(blobdata2), ctypes.addressof(blen), 2)
    ctypes.memmove(ctypes.addressof(blobdata2) + 2, blobdata, blen.value)

    total = 0
    while total < 5000:
        ppp.pp_genParm.gp_callback = None
        ppp.pp_genParm.gp_closure = None
        ppp.pp_stmtHandle = stmtHandle
        ppp.pp_parmCount = 1
        ppp.pp_parmData = dataArrayPtr
        ppp.pp_parmData[0].dv_null = False
        ppp.pp_parmData[0].dv_length = blen.value + 2
        ppp.pp_parmData[0].dv_value = ctypes.addressof(blobdata2)
        total = total + blen.value
        ppp.pp_moreSegments = total < 5000
        bsl = blen.value
        if ppp.pp_moreSegments:
            print(f'{script}: blob segment length: {bsl} (more segments)')
        else:
            print(f'{script}: blob segment length: {bsl} (end of segments)')

        IIapi_putParms(ppp)

        while not ppp.pp_genParm.gp_completed:
            IIapi_wait(wtp)
        status = ppp.pp_genParm.gp_status
        if status != IIAPI_ST_SUCCESS:
            IIdemo_checkError(ppp.pp_genParm)

    ppp.pp_genParm.gp_callback = None
    ppp.pp_genParm.gp_closure = None
    ppp.pp_stmtHandle = stmtHandle
    ppp.pp_parmCount = 1
    ppp.pp_parmData = dataArrayPtr
    ppp.pp_parmData[0].dv_null = False
    ppp.pp_parmData[0].dv_length = len(insTBLInfo[0][1])
    comment = ctypes.create_string_buffer(insTBLInfo[0][1])
    ppp.pp_parmData[0].dv_value = ctypes.addressof(comment)
    ppp.pp_moreSegments = False

    IIapi_putParms(ppp)

    while not ppp.pp_genParm.gp_completed:
        IIapi_wait(wtp)
    status = ppp.pp_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(ppp.pp_genParm)

    ##  get insert results
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle

    IIapi_getQueryInfo(gqp)

    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)
    status = gqp.gq_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(gqp.gq_genParm)

    ##  free statement resources
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = stmtHandle

    IIapi_close(clp)

    while not clp.cl_genParm.gp_completed:
        IIapi_wait(wtp)
    status = clp.cl_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(clp.pp_genParm)


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

print(f'{script}: create table')
tranHandle = None
createTBLText = b'''CREATE TABLE api_demo ( name char(20) NOT NULL, 
				blobdata long varchar, 
				comment char(20))'''
connHandle, tranHandle = IIdemo_query(connHandle, tranHandle, createTBLText)

print(f'{script}: insert one row')
IIdemo_insert(connHandle, tranHandle)

##  set EnvParm for segment length of 4K
print(f'{script}: setEnvParam seg len to 4k')
sep = IIAPI_SETENVPRMPARM()
sep.se_envHandle = envHandle
sep.se_paramID = IIAPI_EP_MAX_SEGMENT_LEN
paramvalue = ctypes.c_long(4096)
sep.se_paramValue = ctypes.addressof(paramvalue)

IIapi_setEnvParam(sep)
if sep.se_status != IIAPI_ST_SUCCESS:
    print(f'{script}: setEnvParm error')

print(f'{script}: select one row')
##  provide parameters for IIapi_query()
selectText = b'SELECT * FROM api_demo'
qyp = IIAPI_QUERYPARM()
qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_QUERY
qyp.qy_queryText = selectText
qyp.qy_parameters = False
qyp.qy_tranHandle = tranHandle
qyp.qy_stmtHandle = None

IIapi_query(qyp)

while not qyp.qy_genParm.gp_completed:
    IIapi_wait(wtp)
if qyp.qy_genParm.gp_status != IIAPI_ST_SUCCESS:
    IIdemo_checkError(qyp.qy_genParm)
tranHandle = qyp.qy_tranHandle
stmtHandle = qyp.qy_stmtHandle

##  get query result descriptors
print(f'{script}: get descriptors')
gdp = IIAPI_GETDESCRPARM()
gdp.gd_genParm.gp_callback = None
gdp.gd_genParm.gp_closure = None
gdp.gd_stmtHandle = stmtHandle
gdp.gd_descriptorCount = 0
gdp.gd_descriptor = None

IIapi_getDescriptor(gdp)

while not gdp.gd_genParm.gp_completed:
    IIapi_wait(wtp)
if gdp.gd_genParm.gp_status != IIAPI_ST_SUCCESS:
    IIdemo_checkError(gdp.gd_genParm)

##  get query results
print(f'{script}: get results')
dataArray = (IIAPI_DATAVALUE * 3)()
dataArrayPtr = ctypes.cast(dataArray, ctypes.POINTER(IIAPI_DATAVALUE))
var1 = ctypes.create_string_buffer(21)
gcp = IIAPI_GETCOLPARM()
gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = 1
gcp.gc_columnData = dataArrayPtr
gcp.gc_columnData[0].dv_value = ctypes.addressof(var1)
gcp.gc_stmtHandle = stmtHandle
gcp.gc_moreSegments = 0

IIapi_getColumns(gcp)

while not gcp.gc_genParm.gp_completed:
    IIapi_wait(wtp)
if gcp.gc_genParm.gp_status != IIAPI_ST_SUCCESS:
    IIdemo_checkError(gcp.gc_genParm)

username = var1.value
print(f'{script}: first column {username=}')

##  call IIapi_getColumns() in loop until all segments for the blob
##  column are retrieved
blobdata2 = ctypes.c_buffer(4100)
blobdata3 = ctypes.c_buffer(4100)
while True:
    gcp.gc_genParm.gp_callback = None
    gcp.gc_genParm.gp_closure = None
    gcp.gc_rowCount = 1
    gcp.gc_columnCount = 1
    gcp.gc_columnData = dataArrayPtr
    gcp.gc_columnData[0].dv_value = ctypes.addressof(blobdata2)
    gcp.gc_stmtHandle = stmtHandle

    IIapi_getColumns(gcp)

    while not gcp.gc_genParm.gp_completed:
        IIapi_wait(wtp)
    if gcp.gc_genParm.gp_status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(gcp.gc_genParm)

    blen = ctypes.c_short()
    ctypes.memmove(ctypes.addressof(blen), ctypes.addressof(blobdata2), 2)
    dst = ctypes.addressof(blobdata3)
    src = ctypes.addressof(blobdata2) + 2
    ctypes.memmove(dst, src, blen.value)

    segs = 'more segments' if gcp.gc_moreSegments else 'end of segments'
    print(f'{script}: blob segment length: {blen.value} ({segs})')
    if not gcp.gc_moreSegments:
        break

##  call IIapi_getColumns() to retrieve last column
var2 = ctypes.create_string_buffer(21)
gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = 1
gcp.gc_columnData = dataArrayPtr
gcp.gc_columnData[0].dv_value = ctypes.addressof(var2)
gcp.gc_stmtHandle = stmtHandle
gcp.gc_moreSegments = 0

IIapi_getColumns(gcp)

while not gcp.gc_genParm.gp_completed:
    IIapi_wait(wtp)
if gcp.gc_genParm.gp_status != IIAPI_ST_SUCCESS:
    IIdemo_checkError(gcp.gc_genParm)

clen = gcp.gc_columnData[0].dv_length
var2 = var2[:clen]
print(f'{script}: last column comment={var2}')

## call IIapi_getQueryInfo()
gqp = IIAPI_GETQINFOPARM()
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle
IIapi_getQueryInfo(gqp)
while not gqp.gq_genParm.gp_completed:
    IIapi_wait(wtp)
if gqp.gq_genParm.gp_status != IIAPI_ST_SUCCESS:
    IIdemo_checkError(gqp.gq_genParm)

##  close the query
clp = IIAPI_CLOSEPARM()
clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = stmtHandle

IIapi_close(clp)

while not clp.cl_genParm.gp_completed:
    IIapi_wait(wtp)
if clp.cl_genParm.gp_status != IIAPI_ST_SUCCESS:
    IIdemo_checkError(clp.cl_genParm)

IIdemo_rollback(tranHandle)
IIdemo_disconn(connHandle)
IIdemo_term(envHandle)
quit()
