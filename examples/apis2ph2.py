#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apis2ph2.py
##
##  Description:
##     Demonstrates using IIapi_registerXID(), IIapi_connect() with
##     transaction handle, IIapi_releaseXID().
##
##     Second part of two phase commit demo.  Run apis2ph1 to begin
##     distributed transaction.  Reconnects to distribute transaction
##     and rolls back the transaction.
##
##  Following actions are demonstrated in the main()
##     Register distributed transaction ID.
##     Establish connection with distributed transaction.
##     Rollback distributed transaction.
##     Release distributed transaction ID.
##
##  Command syntax: python apis2ph2.py [vnode::]dbname[/server_class]


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
    return inp.in_envHandle


def IIdemo_term():
    '''terminate API access'''

    tmp = IIAPI_TERMPARM()

    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)


def IIdemo_disconn(connHandle):
    '''release DBMS connection'''

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

    connHandle = None
    return connHandle


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

##  establish connection which is associated with the distributed
##  transaction
print(f'{script}: establishing connection')
cop = IIAPI_CONNPARM()
cop.co_genParm.gp_callback = None
cop.co_genParm.gp_closure = None
cop.co_target = target
cop.co_connHandle = envHandle
cop.co_tranHandle = tranIDHandle
cop.co_type = IIAPI_CT_SQL
cop.co_username = None
cop.co_password = None
cop.co_timeout = -1
IIapi_connect(cop)
while not cop.co_genParm.gp_completed:
    IIapi_wait(wtp)
connHandle = cop.co_connHandle
tranHandle = cop.co_tranHandle

##  rollback the distributed transaction
print(f'{script}: rollback')
rbp = IIAPI_ROLLBACKPARM()
rbp.rb_genParm.gp_callback = None
rbp.rb_genParm.gp_closure = None
rbp.rb_tranHandle = tranHandle
rbp.rb_savePointHandle = None
IIapi_rollback(rbp)
while not rbp.rb_genParm.gp_completed:
    IIapi_wait(wtp)

##  release distributed transaction ID
print(f'{script}: release XID')
rlp = IIAPI_RELXIDPARM()
rlp.rl_tranIdHandle = tranIDHandle
IIapi_releaseXID(rlp)

IIdemo_disconn(connHandle)
IIdemo_term()
quit()
