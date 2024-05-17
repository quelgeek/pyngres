#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisscrl.py
##
##  Description:
##      Demonstrates using IIapi_scroll() and IIapi_position().
##
##
##  Following actions are demonstrated in the main()
##      Issue query
##      Get query result descriptors
##      Scroll/position cursor
##      Get query results
##      Free query resources
##
##  Command syntax:
##      python apisscrl.py [vnode::]dbname[/server_class]


from varchar import *
from pyngres import *
import ctypes
import sys


ROW_COUNT = 10

dataArray = (IIAPI_DATAVALUE * ROW_COUNT)()
resultSize = 0


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1

    IIapi_initialize(inp)
    status = inp.in_status

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


def IIdemo_rollback(tranHandle):
    '''rollback current transaction and reset transaction handle'''

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


def IIdemo_scroll(stmtHandle, orientation, offset):
    '''Invokes IIapi_scroll() to scroll cursor in result set'''

    slp = IIAPI_SCROLLPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    slp.sl_genParm.gp_callback = None
    slp.sl_genParm.gp_closure = None
    slp.sl_stmtHandle = stmtHandle
    slp.sl_orientation = orientation
    slp.sl_offset = offset

    IIapi_scroll(slp)

    while not slp.sl_genParm.gp_completed:
        IIapi_wait(wtp)


def IIdemo_position(stmtHandle, ref, offset, rows):
    '''Invokes IIapi_position() to position cursor in result set'''

    pop = IIAPI_POSPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    pop.po_genParm.gp_callback = None
    pop.po_genParm.gp_closure = None
    pop.po_stmtHandle = stmtHandle
    pop.po_reference = ref
    pop.po_offset = offset
    pop.po_rowCount = rows

    IIapi_position(pop)

    while not pop.po_genParm.gp_completed:
        IIapi_wait(wtp)


def IIdemo_get(stmtHandle, rows):
    '''receive the query results'''

    global resultSize
    gcp = IIAPI_GETCOLPARM()
    gqp = IIAPI_GETQINFOPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1

    if rows > 0:
        gcp.gc_genParm.gp_callback = None
        gcp.gc_genParm.gp_closure = None
        gcp.gc_stmtHandle = stmtHandle
        gcp.gc_rowCount = rows
        gcp.gc_columnCount = 1
        gcp.gc_columnData = dataArray
        gcp.gc_moreSegments = 0

        IIapi_getColumns(gcp)

        while not gcp.gc_genParm.gp_completed:
            IIapi_wait(wtp)

    ##  get FETCH result info
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle

    IIapi_getQueryInfo(gqp)

    while not gqp.gq_genParm.gp_completed:
        IIapi_wait(wtp)

    posn = gqp.gq_rowPosition
    nrows = gqp.gq_rowCount

    if gqp.gq_mask & IIAPI_GQ_ROW_STATUS:
        if gqp.gq_rowStatus & IIAPI_ROW_BEFORE:
            if gqp.gq_mask & IIAPI_GQ_ROW_COUNT:
                print(f'Cursor BEFORE (position {posn}, rows {nrows})')
            else:
                print(f'Cursor BEFORE (position {posn})')
        elif gqp.gq_rowStatus & IIAPI_ROW_AFTER:
            if gqp.gq_mask & IIAPI_GQ_ROW_COUNT:
                print(f'Cursor AFTER (position {posn}, rows {nrows})')
            else:
                print(f'Cursor AFTER (position {posn})')
        else:
            if gqp.gq_mask & IIAPI_GQ_ROW_COUNT:
                msg = f'Cursor @ {posn}, received {nrows} row(s)'
                print(msg)
            else:
                print(f'Cursor @ {posn}')
            if gqp.gq_rowStatus & IIAPI_ROW_LAST:
                resultSize = gqp.gq_rowPosition
    elif gqp.gq_mask & IIAPI_GQ_ROW_COUNT:
        print(f'Received {nrows} row' + 's' if nrows != 1 else '')


##  this is the main body of the demonstration code
argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()

queryText = b'SELECT table_name FROM iitables'
varvalue = []
for i in range(ROW_COUNT):
    varvalue.append(varchar(256))

qyp = IIAPI_QUERYPARM()
gdp = IIAPI_GETDESCRPARM()
clp = IIAPI_CLOSEPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

envHandle = IIdemo_init()
connHandle = IIdemo_conn(target, envHandle)

##  open scrollable cursor
print(f'{script}: opening cursor')

qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_OPEN
qyp.qy_queryText = queryText
qyp.qy_flags = IIAPI_QF_SCROLL
qyp.qy_parameters = False
qyp.qy_tranHandle = None
qyp.qy_stmtHandle = None

IIapi_query(qyp)

while not qyp.qy_genParm.gp_completed:
    IIapi_wait(wtp)

tranHandle = qyp.qy_tranHandle
stmtHandle = qyp.qy_stmtHandle

##  Get query result descriptors
print(f'{script}: get query descriptors')

gdp.gd_genParm.gp_callback = None
gdp.gd_genParm.gp_closure = None
gdp.gd_stmtHandle = stmtHandle
gdp.gd_descriptorCount = 0
gdp.gd_descriptor = None

IIapi_getDescriptor(gdp)

while not gdp.gd_genParm.gp_completed:
    IIapi_wait(wtp)

for i in range(ROW_COUNT):
    dataArray[i].dv_value = ctypes.addressof(varvalue[i])

##  scroll to last row to determine result set size
print(f'{script}: scroll last')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_LAST, 0)
IIdemo_get(stmtHandle, 0)

##  scroll to first row
print(f'{script}: scroll first')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_FIRST, 0)
IIdemo_get(stmtHandle, 1)

##  scroll after last row
print(f'{script}: scroll after')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_AFTER, 0)
IIdemo_get(stmtHandle, 0)

##  scroll after last row
print(f'{script}: scroll prior')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_PRIOR, 0)
IIdemo_get(stmtHandle, 1)

##  scroll before first row
print(f'{script}: scroll before')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_BEFORE, 0)
IIdemo_get(stmtHandle, 0)

##  scroll next to first row
print(f'{script}: scroll next')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_NEXT, 0)
IIdemo_get(stmtHandle, 1)

##  scroll to specific row
print(f'{script}: scroll absolute')
midway = int(resultSize / 2)
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_ABSOLUTE, midway)
IIdemo_get(stmtHandle, 1)

##  refresh current row
print(f'{script}: scroll current')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_CURRENT, 0)
IIdemo_get(stmtHandle, 1)

##  scroll relative to current position
print(f'{script}: scroll relative')
IIdemo_scroll(stmtHandle, IIAPI_SCROLL_RELATIVE, -ROW_COUNT)
IIdemo_get(stmtHandle, 1)

##  position to first block of rows
print(f'{script}: position first block')
IIdemo_position(stmtHandle, IIAPI_POS_BEGIN, 1, ROW_COUNT)
IIdemo_get(stmtHandle, ROW_COUNT)

##  position to next block of rows
print(f'{script}: position second block')
IIdemo_position(stmtHandle, IIAPI_POS_CURRENT, 1, ROW_COUNT)
IIdemo_get(stmtHandle, ROW_COUNT)

##  position to last block of rows
print(f'{script}: position last block')
IIdemo_position(stmtHandle, IIAPI_POS_END, -ROW_COUNT, ROW_COUNT)
IIdemo_get(stmtHandle, ROW_COUNT)

##  free resources
print(f'{script}: close cursor')

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
