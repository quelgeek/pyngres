#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisprbr.py
##
##  Description:
##      Demonstrates using IIapi_query(), IIapi_setDescriptor(),
##      IIapi_putParams(), IIapi_getDescriptor() and IIapi_getColumns()
##      to execute a database procedure with BYREF parameters and
##      retrieve the parameter result and procedure return values.
##
##  Following actions are demonstrated in the main()
##      Create procedure
##      Execute procedure
##      Retrieve BYREF parameter results
##      Get procedure results
##
##  Command syntax:
##      python apisprbr.py [vnode::]dbname[/server_class]


from varchar import *
from pyngres import *
import ctypes
import struct
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


def IIdemo_rollback(tranHandle):
    '''rollback current transaction and reset the transaction handle'''

    rbp = IIAPI_ROLLBACKPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    print('IIdemo_conn: rolling back transaction')

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


##  this is the main body of the demonstration code
argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()

procText = (
    b'CREATE PROCEDURE api_demo_prbr( '
    b'    table_name varchar(256) WITH NULL, '
    b'    column_count integer WITH NULL ) '
    b'AS DECLARE '
    b'    table_count integer NOT NULL; '
    b'BEGIN '
    b'    IF ( :table_name IS NULL ) THEN '
    b'        SELECT table_name INTO :table_name FROM iitables; '
    b'    ENDIF; '
    b'    SELECT count(*) INTO :column_count'
    b'    FROM iicolumns '
    b'	  WHERE table_name = :table_name; '
    b'    SELECT count(*) INTO :table_count'
    b'    FROM iitables; '
    b'    RETURN :table_count; '
    b'END'
)

procName = ctypes.create_string_buffer(b'api_demo_prbr')
parmName = ctypes.create_string_buffer(b'table_name')
parmCount = ctypes.create_string_buffer(b'column_count')
descrArray = (IIAPI_DESCRIPTOR * 3)()
descrArrayPtr = ctypes.cast(descrArray, ctypes.POINTER(IIAPI_DESCRIPTOR))
dataArray = (IIAPI_DATAVALUE * 3)()
dataArrayPtr = ctypes.cast(dataArray, ctypes.POINTER(IIAPI_DATAVALUE))

qyp = IIAPI_QUERYPARM()
gdp = IIAPI_GETDESCRPARM()
gcp = IIAPI_GETCOLPARM()
sdp = IIAPI_SETDESCRPARM()
ppp = IIAPI_PUTPARMPARM()
gqp = IIAPI_GETQINFOPARM()
clp = IIAPI_CLOSEPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None
tranHandle = IIdemo_query(connHandle, tranHandle, procText)

##  execute procedure
print(f'{script}: execute procedure')

qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_EXEC_PROCEDURE
qyp.qy_queryText = None
qyp.qy_parameters = True
qyp.qy_tranHandle = tranHandle
qyp.qy_stmtHandle = None

IIapi_query(qyp)

while not qyp.qy_genParm.gp_completed:
    IIapi_wait(wtp)

tranHandle = qyp.qy_tranHandle
stmtHandle = qyp.qy_stmtHandle

##  describe procedure parameters
sdp.sd_genParm.gp_callback = None
sdp.sd_genParm.gp_closure = None
sdp.sd_stmtHandle = stmtHandle
sdp.sd_descriptorCount = 3
sdp.sd_descriptor = descrArrayPtr

sdp.sd_descriptor[0].ds_dataType = IIAPI_CHA_TYPE
sdp.sd_descriptor[0].ds_nullable = False
sdp.sd_descriptor[0].ds_length = len(procName.value)
sdp.sd_descriptor[0].ds_precision = 0
sdp.sd_descriptor[0].ds_scale = 0
sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_SVCPARM
sdp.sd_descriptor[0].ds_columnName = None

sdp.sd_descriptor[1].ds_dataType = IIAPI_CHA_TYPE
sdp.sd_descriptor[1].ds_nullable = True
sdp.sd_descriptor[1].ds_length = 256
sdp.sd_descriptor[1].ds_precision = 0
sdp.sd_descriptor[1].ds_scale = 0
sdp.sd_descriptor[1].ds_columnType = IIAPI_COL_PROCBYREFPARM
sdp.sd_descriptor[1].ds_columnName = ctypes.addressof(parmName)

sdp.sd_descriptor[2].ds_dataType = IIAPI_INT_TYPE
sdp.sd_descriptor[2].ds_nullable = True
sdp.sd_descriptor[2].ds_length = ctypes.sizeof(II_INT4)
sdp.sd_descriptor[2].ds_precision = 0
sdp.sd_descriptor[2].ds_scale = 0
sdp.sd_descriptor[2].ds_columnType = IIAPI_COL_PROCBYREFPARM
sdp.sd_descriptor[2].ds_columnName = ctypes.addressof(parmCount)

IIapi_setDescriptor(sdp)

while not sdp.sd_genParm.gp_completed:
    IIapi_wait(wtp)

## send procedure parameters
ppp.pp_genParm.gp_callback = None
ppp.pp_genParm.gp_closure = None
ppp.pp_stmtHandle = stmtHandle
ppp.pp_parmCount = 3
ppp.pp_moreSegments = False

ppp.pp_parmData = dataArrayPtr
ppp.pp_parmData[0].dv_null = False
ppp.pp_parmData[0].dv_length = len(procName.value)
ppp.pp_parmData[0].dv_value = ctypes.addressof(procName)

ppp.pp_parmData[1].dv_null = True
ppp.pp_parmData[1].dv_length = 0
ppp.pp_parmData[1].dv_value = None

ppp.pp_parmData[2].dv_null = True
ppp.pp_parmData[2].dv_length = 0
ppp.pp_parmData[2].dv_value = None

IIapi_putParms(ppp)

while not ppp.pp_genParm.gp_completed:
    IIapi_wait(wtp)

##  get BYREF parameter descriptors
gdp.gd_genParm.gp_callback = None
gdp.gd_genParm.gp_closure = None
gdp.gd_stmtHandle = stmtHandle
gdp.gd_descriptorCount = 0
gdp.gd_descriptor = None

IIapi_getDescriptor(gdp)

while not gdp.gd_genParm.gp_completed:
    IIapi_wait(wtp)

##  get BYREF parameter results
gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = 2
gcp.gc_columnData = dataArrayPtr
name = varchar(256)
count = ctypes.c_int()
gcp.gc_columnData[0].dv_value = ctypes.addressof(name)
gcp.gc_columnData[1].dv_value = ctypes.addressof(count)
gcp.gc_stmtHandle = stmtHandle
gcp.gc_moreSegments = 0

IIapi_getColumns(gcp)

while not gcp.gc_genParm.gp_completed:
    IIapi_wait(wtp)

if gcp.gc_genParm.gp_status < IIAPI_ST_NO_DATA:
    print(f'table {name} has {count.value} columns')

##  get procedure results
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle

IIapi_getQueryInfo(gqp)

while not gqp.gq_genParm.gp_completed:
    IIapi_wait(wtp)

if gqp.gq_mask & IIAPI_GQ_PROCEDURE_RET:
    dbpret = gqp.gq_procedureReturn
    print(f'there are {dbpret} tables')

##  free resources
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
