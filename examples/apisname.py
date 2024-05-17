#!/usr/bin/env python

##  Copyright (c) 2023 Roy Hann

##  Name: apisname.c
##
##  Description:
##      Demonstrates using IIapi_connect(), IIapi_autocommit() and
##      IIapi_query() to access the Name Server database.
##
##  Following actions are demonstrate in the main()
##      Connect to local Name Server
##      Enable Autocommit
##      Query connection info
##      Disable autocommit
##
##  Command syntax:
##      python apisname.py


from pyngres import *
import ctypes
import struct
import sys


def IIdemo_init():
    '''Initialize API access'''

    inp = IIAPI_INITPARM()

    print('IIdemo_init: initializing API')
    inp.in_version = IIAPI_VERSION_2
    inp.in_timeout = -1
    IIapi_initialize(inp)

    envHandle = inp.in_envHandle
    return envHandle


def IIdemo_term(envHandle):
    '''Terminate API access'''

    rep = IIAPI_RELENVPARM()
    tmp = IIAPI_TERMPARM()

    print('IIdemo_term: releasing environment resources')
    rep.re_envHandle = envHandle
    IIapi_releaseEnv(rep)

    print('IIdemo_term: shutting down API')
    IIapi_terminate(tmp)

    envHandle = None
    return envHandle


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


##  this is the main body of the demonstration code
argv = sys.argv
script = argv[0]

connHandle = None
tranHandle = None
stmtHandle = None
envHandle = None
cop = IIAPI_CONNPARM()
acp = IIAPI_AUTOPARM()
qyp = IIAPI_QUERYPARM()
gdp = IIAPI_GETDESCRPARM()
gcp = IIAPI_GETCOLPARM()
gqp = IIAPI_GETQINFOPARM()
clp = IIAPI_CLOSEPARM()
wtp = IIAPI_WAITPARM()
wtp.wt_timeout = -1
DataBuffer = (IIAPI_DATAVALUE * 5)()

envHandle = IIdemo_init()

##  Connect to local Name Server
print(f'{script}: establishing connection to Name Server')

cop.co_genParm.gp_callback = None
cop.co_genParm.gp_closure = None
cop.co_target = None
cop.co_type = IIAPI_CT_NS
cop.co_connHandle = envHandle
cop.co_tranHandle = None
cop.co_username = None
cop.co_password = None
cop.co_timeout = -1

IIapi_connect(cop)

while not cop.co_genParm.gp_completed:
    IIapi_wait(wtp)

connHandle = cop.co_connHandle
tranHandle = cop.co_tranHandle

##  Enable autocommit
print(f'{script}: enable autocommit')

acp.ac_genParm.gp_callback = None
acp.ac_genParm.gp_closure = None
acp.ac_connHandle = connHandle
acp.ac_tranHandle = None

IIapi_autocommit(acp)

while not acp.ac_genParm.gp_completed:
    logger.info('...waiting...')
    IIapi_wait(wtp)

tranHandle = acp.ac_tranHandle

##  Execute 'show' statement
print(f'{script}: retrieving VNODE connection info')

showText = b'show global connection * * * * '
qyp.qy_genParm.gp_callback = None
qyp.qy_genParm.gp_closure = None
qyp.qy_connHandle = connHandle
qyp.qy_queryType = IIAPI_QT_QUERY
qyp.qy_queryText = showText
qyp.qy_parameters = False
qyp.qy_tranHandle = tranHandle
qyp.qy_stmtHandle = None

IIapi_query(qyp)

while not qyp.qy_genParm.gp_completed:
    IIapi_wait(wtp)

stmtHandle = qyp.qy_stmtHandle

##  Get result row descriptors
gdp.gd_genParm.gp_callback = None
gdp.gd_genParm.gp_closure = None
gdp.gd_stmtHandle = stmtHandle
gdp.gd_descriptorCount = 0
gdp.gd_descriptor = None

IIapi_getDescriptor(gdp)

while not gdp.gd_genParm.gp_completed:
    IIapi_wait(wtp)

descriptorCount = gdp.gd_descriptorCount

##  Retrieve result rows
gcp.gc_genParm.gp_callback = None
gcp.gc_genParm.gp_closure = None
gcp.gc_rowCount = 1
gcp.gc_columnCount = descriptorCount
gcp.gc_columnData = DataBuffer
gcp.gc_stmtHandle = stmtHandle
gcp.gc_moreSegments = 0
buffers = [ctypes.create_string_buffer(129) for i in range(descriptorCount)]
address = [ctypes.addressof(b) for b in buffers]

for i in range(descriptorCount):
    gcp.gc_columnData[i].dv_value = address[i]

while True:
    IIapi_getColumns(gcp)

    while not gcp.gc_genParm.gp_completed:
        IIapi_wait(wtp)

    status = gcp.gc_genParm.gp_status
    if status == IIAPI_ST_NO_DATA:
        break
    if status != IIAPI_ST_SUCCESS:
        quit()

    row = {}
    for i in range(descriptorCount):
        columnName = gdp.gd_descriptor[i].ds_columnName.decode()
        if gdp.gd_descriptor[i].ds_dataType == IIAPI_VCH_TYPE:
            # the first two bytes is the length
            ptr = gcp.gc_columnData[i].dv_value
            vlenfield = ctypes.string_at(ptr, 2)
            vlen = struct.unpack_from('h', vlenfield)[0]
            value = ctypes.string_at(ptr + 2, vlen)
        else:
            ptr = gcp.gc_columnData[i].dv_value
            value = ctypes.string_at(ptr, 1)
        row[columnName] = value
    gp = row['type']
    vnode = row['vnode']
    host = row['network_address']
    prot = row['protocol']
    addr = row['listen_address']
    print(f'G/P={gp} {vnode=} {host=} {prot=} {addr=}')

##  Get query results
gqp.gq_genParm.gp_callback = None
gqp.gq_genParm.gp_closure = None
gqp.gq_stmtHandle = stmtHandle

IIapi_getQueryInfo(gqp)

while not gqp.gq_genParm.gp_completed:
    IIapi_wait(wtp)

##  Close query
clp.cl_genParm.gp_callback = None
clp.cl_genParm.gp_closure = None
clp.cl_stmtHandle = stmtHandle

IIapi_close(clp)

while not clp.cl_genParm.gp_completed:
    IIapi_wait(wtp)

##  Disable autocommit
print(f'{script}: disable autocommit')

acp.ac_connHandle = None
acp.ac_tranHandle = tranHandle

IIapi_autocommit(acp)

while not acp.ac_genParm.gp_completed:
    IIapi_wait(wtp)

IIdemo_disconn(connHandle)
IIdemo_term(envHandle)
