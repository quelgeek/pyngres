##!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisprrp.py
##
##  Description:
##      Demonstrates using IIapi_query(), IIapi_setDescriptor(),
##      IIapi_putParams(), IIapi_getDescriptor() and IIapi_getColumns()
##      to execute a database procedure which returns multiple rows.
##
##  Following actions are demonstrated in the main()
##      Create procedure
##      Execute procedure
##      Retrieve result rows
##      Get procedure results
##
##  Command syntax:
##      python apisprrp.py [vnode::]dbname[/server_class]


from varchar import *
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


queryText = (
    b'CREATE PROCEDURE api_demo_prrp '
    b'RESULT ROW( varchar(32), varchar(32) ) '
    b'AS DECLARE '
    b'      towner varchar(32); '
    b'      tname varchar(32);  '
    b'      rcnt integer NOT NULL; '
    b'BEGIN   '
    b'  rcnt = 0; '
    b'  FOR SELECT table_owner, table_name '
    b'  INTO :towner, :tname '
    b'  FROM iitables '
    b'  DO  '
    b'      rcnt = rcnt + 1; '
    b'      RETURN ROW( :towner, :tname ); '
    b'  ENDFOR; '
    b'RETURN :rcnt; '
    b'END'
)


qyp = IIAPI_QUERYPARM()
sdp = IIAPI_SETDESCRPARM()
ppp = IIAPI_PUTPARMPARM()
gdp = IIAPI_GETDESCRPARM()
gcp = IIAPI_GETCOLPARM()
gqp = IIAPI_GETQINFOPARM()
clp = IIAPI_CLOSEPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None
tranHandle = IIdemo_query(connHandle, tranHandle, queryText)

##  Execute procedure
print('apisprrp: execute procedure')

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

##  describe parameters
sdp.sd_genParm.gp_callback = None
sdp.sd_genParm.gp_closure = None
sdp.sd_stmtHandle = stmtHandle
sdp.sd_descriptorCount = 1

descrArray = (IIAPI_DESCRIPTOR * 2)()
sdp.sd_descriptor = descrArray

sdp.sd_descriptor[0].ds_dataType = IIAPI_CHA_TYPE
sdp.sd_descriptor[0].ds_nullable = False
procName = ctypes.create_string_buffer(b'api_demo_prrp')
sdp.sd_descriptor[0].ds_length = len(procName.value)
sdp.sd_descriptor[0].ds_precision = 0
sdp.sd_descriptor[0].ds_scale = 0
sdp.sd_descriptor[0].ds_columnType = IIAPI_COL_SVCPARM
sdp.sd_descriptor[0].ds_columnName = None

IIapi_setDescriptor(sdp)

while not sdp.sd_genParm.gp_completed:
    IIapi_wait(wtp)

##  send parameters
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

IIapi_putParms(ppp)

while not ppp.pp_genParm.gp_completed:
    IIapi_wait(wtp)

##  get description of result rows
gdp.gd_genParm.gp_callback = None
gdp.gd_genParm.gp_closure = None
gdp.gd_stmtHandle = stmtHandle
gdp.gd_descriptorCount = 0
gdp.gd_descriptor = None

IIapi_getDescriptor(gdp)

while not gdp.gd_genParm.gp_completed:
    IIapi_wait(wtp)

##  get result rows
gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = 2
dataArray = (IIAPI_DATAVALUE * 2)()
gcp.gc_columnData = dataArray
owner = varchar(33)
name = varchar(33)
gcp.gc_columnData[0].dv_value = ctypes.addressof(owner)
gcp.gc_columnData[1].dv_value = ctypes.addressof(name)
gcp.gc_stmtHandle = stmtHandle
gcp.gc_moreSegments = 0

while True:
    IIapi_getColumns(gcp)

    while not gcp.gc_genParm.gp_completed:
        IIapi_wait(wtp)

    if gcp.gc_genParm.gp_status >= IIAPI_ST_NO_DATA:
        break
    print(f'owner={str(owner)}, name={str(name)}')

##  get procedure results
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle

IIapi_getQueryInfo(gqp)

while not gqp.gq_genParm.gp_completed:
    IIapi_wait(wtp)

if gqp.gq_mask & IIAPI_GQ_PROCEDURE_RET:
    dbpret = gqp.gq_procedureReturn
    print(f'{dbpret} tables')

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
