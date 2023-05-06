#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apiserr.py
##
##  Description:
##      Demonstrates using IIapi_getQueryInfo() and IIapi_getErrorInfo().
##      This demo program can be taken as an example of a generic error
##      handler function for handling OpenAPI errors returned to the
##      application. Additionally please refer to the OpenAPI documentation,
##      section "Generic Parameters" to understand the meaning of each of
##      IIAPI_ST_* error codes returned in "gp_status" o/p parameter of
##      IIAPI_GENPARM structure.
##
##  Following actions are demonstrated in main
##      Check status of statement with no rows.
##      Check status of insert statement.
##      Check status of invalid statement.
##
##  Command syntax:
##      python apiserr.py [vnode::]dbname[/server_class]


from pyngres import *
import sys


def IIdemo_checkError(genParm):
    '''Check status of API function call and process error information'''

    gep = IIAPI_GETEINFOPARM()

    ##  Check API call status
    status = genParm.gp_status
    if status == IIAPI_ST_SUCCESS:
        print(IIAPI_ST_MSG[status])
    elif status == IIAPI_ST_MESSAGE:
        print(IIAPI_ST_MSG[status])
    elif status in (IIAPI_ST_WARNING, IIAPI_ST_NO_DATA):
        print(IIAPI_ST_MSG[status])
    elif status in IIAPI_ST_MSG.keys():
        print(IIAPI_ST_MSG[status])
    else:
        print(f'Unknown IIAPI_STATUS value {status}')

    ##  check for error information
    if not genParm.gp_errorHandle:
        return
    gep.ge_errorHandle = genParm.gp_errorHandle

    while True:
        ##  Invoke API function call
        IIapi_getErrorInfo(gep)

        ##  Break out of the loop if no data or failed
        if gep.ge_status != IIAPI_ST_SUCCESS:
            break

        ##  Process result
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
        print(
            f'Error info: {label} '
            f'{gep.ge_SQLSTATE} {gep.ge_errorCode:#0x} '
            f'{message}'
        )


def IIdemo_checkQInfo(gqp):
    '''Processes the information returned by IIapi_getQInfo()'''

    ##  Check query result flags
    flags = gqp.gq_flags
    if flags & IIAPI_GQF_FAIL:
        print('flag = IIAPI_GQF_FAIL')
    if flags & IIAPI_GQF_ALL_UPDATED:
        print('flag = IIAPI_GQF_ALL_UPDATED')
    if flags & IIAPI_GQF_NULLS_REMOVED:
        print('flag = IIAPI_GQF_NULLS_REMOVED')
    if flags & IIAPI_GQF_UNKNOWN_REPEAT_QUERY:
        print('flag = IIAPI_GQF_UNKNOWN_REPEAT_QUERY')
    if flags & IIAPI_GQF_END_OF_DATA:
        print('flag = IIAPI_GQF_END_OF_DATA')
    if flags & IIAPI_GQF_CONTINUE:
        print('flag = IIAPI_GQF_CONTINUE')
    if flags & IIAPI_GQF_INVALID_STATEMENT:
        print('flag = IIAPI_GQF_INVALID_STATEMENT')
    if flags & IIAPI_GQF_TRANSACTION_INACTIVE:
        print('flag = IIAPI_GQF_TRANSACTION_INACTIVE')
    if flags & IIAPI_GQF_OBJECT_KEY:
        print('flag = IIAPI_GQF_OBJECT_KEY')
    if flags & IIAPI_GQF_TABLE_KEY:
        print('flag = IIAPI_GQF_TABLE_KEY')
    if flags & IIAPI_GQF_NEW_EFFECTIVE_USER:
        print('flag = IIAPI_GQF_NEW_EFFECTIVE_USER')
    if flags & IIAPI_GQF_FLUSH_QUERY_ID:
        print('flag = IIAPI_GQF_FLUSH_QUERY_ID')
    if flags & IIAPI_GQF_ILLEGAL_XACT_STMT:
        print('flag = IIAPI_GQF_ILLEGAL_XACT_STMT')

    ##  Check query result values
    mask = gqp.gq_mask
    if mask & IIAPI_GQ_ROW_COUNT:
        print(f'row count = {gqp.gq_rowCount}')
    if mask & IIAPI_GQ_CURSOR:
        print('readonly = True')
    if mask & IIAPI_GQ_PROCEDURE_RET:
        print(f'procedure return = {gqp.gq_procedureReturn}')
    if mask & IIAPI_GQ_PROCEDURE_ID:
        print(f'procedure handle = {gqp.gq_procedureHandle:#0x}')
    if mask & IIAPI_GQ_REPEAT_QUERY_ID:
        print(f'repeat query ID = {gqp.gq_repeatQueryHandle:#0x}')
    if mask & IIAPI_GQ_TABLE_KEY:
        print('received table key')
    if mask & IIAPI_GQ_OBJECT_KEY:
        print('received object key')
    if mask & IIAPI_GQ_ROW_STATUS:
        print(f'row satus = {gqp.gq_rowStatus}')
    if mask & IIAPI_GQ_ROW_COUNT_EX:
        print(f'affected row count = {gqp.gq_rowCountEx}')


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    IIapi_initialize(inp)

    envHandle = inp.in_envHandle
    return envHandle


def IIdemo_query(connHandle, tranHandle, queryText):
    '''Execute SQL statement'''

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
    status = qyp.qy_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(qyp.qy_genParm)

    ##  Return transaction handle
    tranHandle = qyp.qy_tranHandle

    ##  Call IIapi_getQueryInfo() to get results
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = qyp.qy_stmtHandle

    IIapi_getQueryInfo(gqp)

    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)
    status = gqp.gq_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(gqp.gq_genParm)
    else:
        IIdemo_checkQInfo(gqp)

    ## release resources
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = qyp.qy_stmtHandle

    IIapi_close(clp)

    while not clp.cl_genParm.gp_completed:
        IIapi_wait(wtp)

    status = clp.cl_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        IIdemo_checkError(clp.cl_genParm)

    return tranHandle


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
    b"CREATE TABLE api_demo_err"
    b"( "
    b"name char(20) NOT NULL, "
    b"age i4 NOT NULL"
    b")"
)

insertText = b"INSERT INTO api_demo_err " b"VALUES ('John' , 30)"

invalidText = b"SELECT badcolumn " b"FROM api_demo_err"


envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)
tranHandle = None

##  Valid query: no row count
tranHandle = IIdemo_query(connHandle, tranHandle, createTBLText)

##  Valid query: row count
tranHandle = IIdemo_query(connHandle, tranHandle, insertText)

##  Invalid query
tranHandle = IIdemo_query(connHandle, tranHandle, invalidText)

IIdemo_rollback(tranHandle)
IIdemo_disconn(connHandle)
IIdemo_term()
quit()
