#!/usr/bin/env python

##  Copyright (c) 2026 Roy Hann

##  Name: apiscopy.py 
##  
##  Description:
##      Demonstrates using IIapi_query(), IIapi_getCopyMap(),
##      IIapi_putColumns() and IIapi_getColumns() to execute
##      'copy from' and 'copy into' statements.
##  
##  Following actions are demonstrated in the main()
##      Copy data from program into table.
##      Copy data into program from table.
##
##  Command syntax:
##      python apiscopy.py [vnode::]dbname[/server_class]


from pyngres import *
import ctypes
import sys


##  this SQL is different from that used in the C version of apiscopy. Here we
##  declare name as CHAR(20) rather than VARCHAR(20) because VARCHARs add a
##  complication that distracts from the essential point of the example
createTBLText = (
    b'CREATE TABLE api_demo_copy'
    b'( '
    b'name char(20), '
    b'age i4'
    b')' )

sql_copyfrom = ( b'copy table api_demo_copy() from program' )
sql_copyinto = ( b'copy table api_demo_copy() into program' )

insTBLInfo = [  ( b'Abrham, Barbara T.', 35 ),
                ( b'Haskins, Jill G.', 56 ),
                ( b'Poon, Jennifer C.', 50 ),
                ( b'Thurman, Roberta F.', 32 ),
                ( b'Wilson, Frank N.', 24 ) ]
DEMO_TABLE_SIZE = len(insTBLInfo)


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_11
    inp.in_timeout = -1
    IIapi_initialize( inp )
    status = inp.in_status
    return inp.in_envHandle        


def IIdemo_term():
   tmp = IIAPI_TERMPARM()
   print('IIdemo_term: shutting down API')
   IIapi_terminate( tmp )


def IIdemo_conn( target ):
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    print('IIdemo_conn: establishing connection')
    cop = IIAPI_CONNPARM()
    cop.co_genParm.gp_callback = None
    cop.co_genParm.gp_closure = None
    cop.co_target =  target
    cop.co_connHandle = None
    cop.co_tranHandle = None
    cop.co_username = None
    cop.co_password = None
    cop.co_timeout = -1
    IIapi_connect( cop )
    while not cop.co_genParm.gp_completed: 
        IIapi_wait( wtp )
    status = cop.co_genParm.gp_status
    connHandle = cop.co_connHandle
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


def IIdemo_disconn( connHandle ):
    dcp = IIAPI_DISCONNPARM()
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    dcp.dc_genParm.gp_callback = None
    dcp.dc_genParm.gp_closure = None
    dcp.dc_connHandle = connHandle
    IIapi_disconnect( dcp )
    while not dcp.dc_genParm.gp_completed:
        IIapi_wait( wtp )
    return None


def IIdemo_query( connHandle, tranHandle, query ):
    wtp = IIAPI_WAITPARM()
    wtp.wt_timeout = -1
    qyp = IIAPI_QUERYPARM()
    qyp.qy_genParm.gp_callback = None
    qyp.qy_genParm.gp_closure = None
    qyp.qy_connHandle = connHandle
    qyp.qy_queryType = IIAPI_QT_QUERY
    qyp.qy_queryText = query 
    qyp.qy_parameters = False
    qyp.qy_tranHandle = tranHandle
    qyp.qy_stmtHandle = None
    IIapi_query( qyp )
    while not qyp.qy_genParm.gp_completed:
        IIapi_wait( wtp )
    tranHandle = qyp.qy_tranHandle
    stmtHandle = qyp.qy_stmtHandle

    gqp = IIAPI_GETQINFOPARM()
    gqp.gq_genParm.gp_callback = None
    gqp.gq_genParm.gp_closure = None
    gqp.gq_stmtHandle = stmtHandle;
    IIapi_getQueryInfo( gqp );
    while not gqp.gq_genParm.gp_completed:
        IIapi_wait( wtp );

    clp = IIAPI_CLOSEPARM()
    clp.cl_genParm.gp_callback = None
    clp.cl_genParm.gp_closure = None
    clp.cl_stmtHandle = stmtHandle
    IIapi_close( clp );
    while not clp.cl_genParm.gp_completed:
        IIapi_wait( wtp );
    return tranHandle


##  this is the main body of the sample code

argv = sys.argv
script = argv[0]
if len(argv) != 2:
    print(f'usage: python {script} [vnode::]dbname[/server_class]')
    quit()
dbtarget = argv[1]
target = dbtarget.encode()


wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1

IIdemo_init()
connHandle = IIdemo_conn( target )
tranHandle = None
tranHandle = IIdemo_query( connHandle, tranHandle, createTBLText )

##  Execute 'copy from' statement
print( 'apiscopy: copy rows from program to table')

qyp = IIAPI_QUERYPARM()
qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_QUERY
qyp.qy_queryText = sql_copyfrom 
qyp.qy_parameters = False
qyp.qy_tranHandle = tranHandle
qyp.qy_stmtHandle = None

IIapi_query( qyp )
while not qyp.qy_genParm.gp_completed:
    IIapi_wait( wtp )

tranHandle = qyp.qy_tranHandle
stmtHandle = qyp.qy_stmtHandle

##  get copy map describing copy data
gmp = IIAPI_GETCOPYMAPPARM()
gmp.gm_genParm.gp_callback = None
gmp.gm_genParm.gp_closure = None
gmp.gm_stmtHandle = stmtHandle
IIapi_getCopyMap( gmp )
while not gmp.gm_genParm.gp_completed:
    IIapi_wait( wtp )

##  insert row into the table
pcp = IIAPI_PUTCOLPARM()
dataBuffer = (IIAPI_DATAVALUE * 2)()

for row in range(DEMO_TABLE_SIZE):
    print( '\tinsert row')

    var1 = ctypes.create_string_buffer(insTBLInfo[row][0])
    var2 = ctypes.c_int(insTBLInfo[row][1])

    pcp.pc_genParm.gp_callback = None
    pcp.pc_genParm.gp_closure = None
    pcp.pc_stmtHandle = stmtHandle
    pcp.pc_columnCount = gmp.gm_copyMap.cp_dbmsCount
    pcp.pc_moreSegments = False

    pcp.pc_columnData = dataBuffer
    pcp.pc_columnData[0].dv_null = False
    pcp.pc_columnData[0].dv_length = ctypes.sizeof(var1)
    pcp.pc_columnData[0].dv_value = ctypes.addressof(var1) 

    pcp.pc_columnData[1].dv_null = False
    pcp.pc_columnData[1].dv_length = ctypes.sizeof(ctypes.c_int)
    pcp.pc_columnData[1].dv_value = ctypes.addressof( var2 ) 

    IIapi_putColumns( pcp )

    while not pcp.pc_genParm.gp_completed:
        IIapi_wait( wtp )

##  get copy results
gqp = IIAPI_GETQINFOPARM()
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle;

IIapi_getQueryInfo( gqp );

while not gqp.gq_genParm.gp_completed:
    IIapi_wait( wtp );

##  free resources    
clp = IIAPI_CLOSEPARM()
clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = stmtHandle

IIapi_close( clp );

while not clp.cl_genParm.gp_completed:
    IIapi_wait( wtp );

##  execute 'copy into' statement
qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_QUERY
qyp.qy_queryText = sql_copyinto 
qyp.qy_parameters = False
qyp.qy_tranHandle = tranHandle
qyp.qy_stmtHandle = None

IIapi_query( qyp )

while not qyp.qy_genParm.gp_completed:
    IIapi_wait( wtp )

tranHandle = qyp.qy_tranHandle
stmtHandle = qyp.qy_stmtHandle

##  get copy map describing copy data
gmp = IIAPI_GETCOPYMAPPARM()
gmp.gm_genParm.gp_callback = None
gmp.gm_genParm.gp_closure = None
gmp.gm_stmtHandle = stmtHandle

IIapi_getCopyMap( gmp )

while not gmp.gm_genParm.gp_completed:
    IIapi_wait( wtp )

##  get rows from table
gcp = IIAPI_GETCOLPARM()
gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = gmp.gm_copyMap.cp_dbmsCount
gcp.gc_columnData = dataBuffer
gcp.gc_columnData[0].dv_value = ctypes.addressof(var1)
gcp.gc_columnData[1].dv_value = ctypes.addressof(var2)
gcp.gc_stmtHandle = stmtHandle
gcp.gc_moreSegments = 0

while True:
    IIapi_getColumns(gcp)

    while not gcp.gc_genParm.gp_completed:
        IIapi_wait( wtp )

    status = gcp.gc_genParm.gp_status
    if status == IIAPI_ST_NO_DATA:
        break

    print('\t{} = {}, {} = {}'.format(
        gmp.gm_copyMap.cp_dbmsDescr[0].ds_columnName.decode(),
        var1.value,
        gmp.gm_copyMap.cp_dbmsDescr[1].ds_columnName.decode(),
        var2.value))

##  get copy results
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle;

IIapi_getQueryInfo( gqp );

while not gqp.gq_genParm.gp_completed:
    IIapi_wait( wtp );

## free resources
clp = IIAPI_CLOSEPARM()
clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = stmtHandle

IIapi_close( clp );

while not clp.cl_genParm.gp_completed:
    IIapi_wait( wtp );

IIdemo_rollback(tranHandle)
IIdemo_disconn(connHandle)
IIdemo_term()
quit()
