#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisprgt.py
##
##  Description:
##      Demonstrates using IIapi_query(), IIapi_setDescriptor()
##      and IIapi_putParams() to execute a database procedure
##      with a global temporary table parameter.
##
##  Following actions are demonstrated in the main()
##      Create procedure
##      Execute procedure
##
## Command syntax:
##      python apisprgt.py [vnode::]dbname[/server_class]


from varchar import *
from pyngres import *
import ctypes
import struct
import sys


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_initL initializing API')
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    IIapi_initialize(inp)

    envHandle = inp.in_envHandle
    return envHandle


def IIdemo_term():
    '''Terminate API access'''

    tmp = IIAPI_TERMPARM()

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


##  this is the main body of the demonstration code
argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()

declareTBLText = (
    b'DECLARE GLOBAL TEMPORARY TABLE session.api_demo_gtt'
    b'('
    b'  name char(20) NOT NULL,'
    b'  age i4 NOT NULL'
    b')'
    b'ON COMMIT PRESERVE ROWS '
    b'WITH NORECOVERY'
)
insertText = [
    b"INSERT INTO session.api_demo_gtt VALUES ('Shirley',29)",
    b"INSERT INTO session.api_demo_gtt VALUES ('Barb',35)",
]
procText = (
    b'CREATE PROCEDURE api_demo_prgt'
    b'(gtt SET OF (name char(20) NOT NULL, age i4 NOT NULL))'
    b'AS declare rcnt integer NOT NULL;'
    b'BEGIN '
    b'  SELECT count(*) INTO :rcnt FROM gtt;'
    b'  RETURN :rcnt;'
    b'END'
)

qyp = IIAPI_QUERYPARM()
sdp = IIAPI_SETDESCRPARM()
ppp = IIAPI_PUTPARMPARM()
gqp = IIAPI_GETQINFOPARM()
clp = IIAPI_CLOSEPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None

tranHandle = IIdemo_query(connHandle, tranHandle, declareTBLText)
tranHandle = IIdemo_query(connHandle, tranHandle, insertText[0])
tranHandle = IIdemo_query(connHandle, tranHandle, insertText[1])
tranHandle = IIdemo_query(connHandle, tranHandle, procText)

##  Execute procedure
print('apisprgt: executing procedure')

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

##  Describe procedure parameters
sdp.sd_genParm.gp_callback = None
sdp.sd_genParm.gp_closure = None
sdp.sd_stmtHandle = stmtHandle
sdp.sd_descriptorCount = 2
descrArray = (IIAPI_DESCRIPTOR * 2)()
descrArrayPtr = ctypes.cast(descrArray, ctypes.POINTER(IIAPI_DESCRIPTOR))
sdp.sd_descriptor = descrArrayPtr

sdp.sd_descriptor[0].ds_dataType = IIAPI_CHA_TYPE
sdp.sd_descriptor[0].ds_nullable = False
procName = ctypes.create_string_buffer(b'api_demo_prgt')
sdp.sd_descriptor[0].ds_length = len(procName.value)
sdp.sd_descriptor[0].ds_precision = 0
sdp.sd_descriptor[0].ds_scale = 0
sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_SVCPARM
sdp.sd_descriptor[0].ds_columnName = None

sdp.sd_descriptor[1].ds_dataType = IIAPI_VCH_TYPE
sdp.sd_descriptor[1].ds_nullable = False
sdp.sd_descriptor[1].ds_length = 20
sdp.sd_descriptor[1].ds_precision = 0
sdp.sd_descriptor[1].ds_scale = 0
sdp.sd_descriptor[1].ds_columnType = IIAPI_COL_PROCGTTPARM
parmGTT = ctypes.create_string_buffer(b'gtt')
sdp.sd_descriptor[1].ds_columnName = ctypes.addressof(parmGTT)

IIapi_setDescriptor(sdp)

while not sdp.sd_genParm.gp_completed:
    IIapi_wait(wtp)

##  Send procedure parameters
ppp.pp_genParm.gp_callback = None
ppp.pp_genParm.gp_closure = None
ppp.pp_stmtHandle = stmtHandle
ppp.pp_parmCount = sdp.sd_descriptorCount
dataBuffer = (IIAPI_DATAVALUE * 2)()
ppp.pp_parmData = ctypes.cast(dataBuffer, ctypes.POINTER(IIAPI_DATAVALUE))
ppp.pp_moreSegments = 0

ppp.pp_parmData[0].dv_null = False
ppp.pp_parmData[0].dv_length = len(procName.value)
ppp.pp_parmData[0].dv_value = ctypes.addressof(procName)

name = varchar('api_demo_gtt')
ppp.pp_parmData[1].dv_null = False
ppp.pp_parmData[1].dv_length = ctypes.sizeof(name)
ppp.pp_parmData[1].dv_value = ctypes.addressof(name)

IIapi_putParms(ppp)

while not ppp.pp_genParm.gp_completed:
    IIapi_wait(wtp)

##  get procedure results
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle

IIapi_getQueryInfo(gqp)

while not gqp.gq_genParm.gp_completed:
    IIapi_wait(wtp)

if gqp.gq_mask & IIAPI_GQ_PROCEDURE_RET:
    print(f'\t{gqp.gq_procedureReturn} rows')

##  free resources
clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = stmtHandle

IIapi_close(clp)

while not clp.cl_genParm.gp_completed:
    IIapi_wait(waitParm)

IIdemo_rollback(tranHandle)
IIdemo_disconn(connHandle)
IIdemo_term()
quit()
