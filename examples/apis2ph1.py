#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apis2ph1.py
##
##  Description:
##  	Demonstrates using IIapi_registerXID(), IIapi_prepareCommit().
##      Modeled on code supplied by Actian in II_SYSTEM/ingres/demo/api.
##
##	First part of the two phase commit demo.  Begin a distributed
##	transaction and exit without disconnecting.
##
##	Run apis2ph2 to finish processing of distributed transaction.
##
##  Following actions are demonstrated in the main()
##     Register distributed transaction ID.
##     Prepare distributed transaction to be committed.
##
##  Command syntax: python apis2ph1.py [vnode::]dbname[/server_class]


import ctypes
import sys
from pyngres import *


def IIdemo_init():
    '''initialize Ingres OpenAPI access'''

    inp = IIAPI_INITPARM()

    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    print('IIdemo_init: initializing API')
    IIapi_initialize(inp)
    status = inp.in_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    envHandle inp.envHandle
    return envHandle


def IIdemo_conn(target, envHandle):
    '''open connection with target database'''

    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    cop = IIAPI_CONNPARM()

    print('IIdemo_init: establishing connection')
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

    ##  call IIapi_query to execute statement
    IIapi_query(qyp)
    while not qyp.qy_genParm.gp_completed:
        IIapi_wait(wtp)
    status = qyp.qy_genParm.gp_status
    if status != IIAPI_ST_SUCCESS:
        print(f'{status=} ({IIAPI_ST_MSG[status]})')
        quit()
    ## return transaction handle and statement handle
    tranHandle = qyp.qy_tranHandle
    stmtHandle = qyp.qy_stmtHandle

    ## call IIapi_getQueryInfo() to get results
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
    return tranHandle


##  main body of the sample code
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

##  register the distributed transaction ID
print(f'{script}: register XID')
highID = 9999
lowID = 1111
rgp = IIAPI_REGXIDPARM()
rgp.rg_tranID.ti_type = IIAPI_TI_IIXID
rgp.rg_tranID.ti_value.iiXID.ii_tranID.it_highTran = highID
rgp.rg_tranID.ti_value.iiXID.ii_tranID.it_lowTran = lowID
IIapi_registerXID(rgp)
tranIDHandle = rgp.rg_tranIdHandle

##  perform operations in distributed transaction
tranHandle = tranIDHandle
sql = b'CREATE TABLE api_demo_2pc( name char(20) NOT NULL, age i4 NOT NULL )'
tranHandle = IIdemo_query(connHandle, tranHandle, sql)

##  prepare distributed transaction
print(f'{script}: prepare to commit')
prp = IIAPI_PREPCMTPARM()
prp.pr_genParm.gp_callback = None
prp.pr_genParm.gp_closure = None
prp.pr_tranHandle = tranHandle
IIapi_prepareCommit(prp)
while not prp.pr_genParm.gp_completed:
    IIapi_wait(wtp)

##  program terminates abruptly without ending the transaction, closing the
##  connection, or shutting down OpenAPI.  The distributed transaction will
##  still exist in the DBMS since it prepared to commit.  Running the demo
##  apis2ph2.py will establish a connection which will end the distributed
##  transaction.
quit()
