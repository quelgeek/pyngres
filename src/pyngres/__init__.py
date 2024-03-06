##  Copyright (c) 2022 Rational Commerce Ltd.

'''
Python bindings for the Actian Ingres OpenAPI
'''


from functools import wraps
import ctypes
from loguru import logger
import os
from sys import platform


from .IIAPI_CONSTANTS import *
from .IIAPI_PARM import *


##  the IIAPI_DEV_MODE envar puts pyngres in development mode
_name = __name__
if 'IIAPI_DEV_MODE' in os.environ:
    IIAPI_DEV_MODE = True
    logger.warning(f'using {_name} in development mode')
    logger.info(f'to disable logging messages: logger.disable(\'{_name}\')')
    logger.info(f'to enable logging messages: logger.enable(\'{_name}\')')
else:
    IIAPI_DEV_MODE = False
    logger.disable('pyngres')


##  load the Ingres OpenAPI using ctypes
if platform == 'linux':
    ##  expect to find libiiapi.1.so in ../ingres/lib
    try:
        II_SYSTEM = os.environ['II_SYSTEM']
    except KeyError:
        print('II_SYSTEM is not set in the environment')
        quit()
    except Exception as exception:
        print(exception)
        quit()
    api_pathname = f'{II_SYSTEM}/ingres/lib/libiiapi.1.so'
elif platform == 'win32':
    ##  expect to find iilibapi.dll in ..\ingres\bin
    ##  II_SYSTEM isn't necessarily set in the Windows environment; search
    ##  PATH for ingres\bin
    PATH = os.environ['PATH']
    paths = PATH.split(';')
    folders = [
        folder for folder in paths
        if folder.endswith('ingres\\bin') and '.' not in folder]
    nfolders = len(folders)
    if nfolders == 0:
        print('ingres\\bin is not defined in PATH')
        quit()
    ##  use the first match
    path = folders[0]
    api_pathname = f'{path}\\iilibapi.dll'
else:
    print(f'No python binding is available for Ingres OpenAPI on {platform}.')
    print('(If the API exists, a binding is easy to add. Give us a call.)')
    quit()
##  don't use try/except here; just let it explode if it wants to
logger.debug(f'attempting to load {api_pathname}')
iiapi = ctypes.CDLL(api_pathname)
logger.success(f'loaded {api_pathname}')


##  decorator to simplify the binding of functions
def bind_function(lib, funcname, restype, argtypes):
    '''return a python wrapper for a C function'''
    func = lib.__getattr__(funcname)
    func.restype = restype
    func.argtypes = argtypes
    return func


##  bind the Ingres OpenAPI functions
IIapi_abort = bind_function(
    iiapi, 'IIapi_abort', None, [ctypes.POINTER(IIAPI_ABORTPARM)]
)
IIapi_autocommit = bind_function(
    iiapi, 'IIapi_autocommit', None, [ctypes.POINTER(IIAPI_AUTOPARM)]
)
IIapi_batch = bind_function(
    iiapi, 'IIapi_batch', None, [ctypes.POINTER(IIAPI_BATCHPARM)]
)
IIapi_cancel = bind_function(
    iiapi, 'IIapi_cancel', None, [ctypes.POINTER(IIAPI_CANCELPARM)]
)
IIapi_catchEvent = bind_function(
    iiapi, 'IIapi_catchEvent', None, [ctypes.POINTER(IIAPI_CATCHEVENTPARM)]
)
IIapi_close = bind_function(
    iiapi, 'IIapi_close', None, [ctypes.POINTER(IIAPI_CLOSEPARM)]
)
IIapi_commit = bind_function(
    iiapi, 'IIapi_commit', None, [ctypes.POINTER(IIAPI_COMMITPARM)]
)
IIapi_connect = bind_function(
    iiapi, 'IIapi_connect', None, [ctypes.POINTER(IIAPI_CONNPARM)]
)
IIapi_convertData = bind_function(
    iiapi, 'IIapi_convertData', None, [ctypes.POINTER(IIAPI_CONVERTPARM)]
)
IIapi_disconnect = bind_function(
    iiapi, 'IIapi_disconnect', None, [ctypes.POINTER(IIAPI_DISCONNPARM)]
)
IIapi_formatData = bind_function(
    iiapi, 'IIapi_formatData', None, [ctypes.POINTER(IIAPI_FORMATPARM)]
)
IIapi_getColumnInfo = bind_function(
    iiapi, 'IIapi_getColumnInfo', None, [ctypes.POINTER(IIAPI_GETCOLINFOPARM)]
)
IIapi_getColumns = bind_function(
    iiapi, 'IIapi_getColumns', None, [ctypes.POINTER(IIAPI_GETCOLPARM)]
)
IIapi_getCopyMap = bind_function(
    iiapi, 'IIapi_getCopyMap', None, [ctypes.POINTER(IIAPI_GETCOPYMAPPARM)]
)
IIapi_getDescriptor = bind_function(
    iiapi, 'IIapi_getDescriptor', None, [ctypes.POINTER(IIAPI_GETDESCRPARM)]
)
IIapi_getErrorInfo = bind_function(
    iiapi, 'IIapi_getErrorInfo', None, [ctypes.POINTER(IIAPI_GETEINFOPARM)]
)
IIapi_getEvent = bind_function(
    iiapi, 'IIapi_getEvent', None, [ctypes.POINTER(IIAPI_GETEVENTPARM)]
)
IIapi_getQueryInfo = bind_function(
    iiapi, 'IIapi_getQueryInfo', None, [ctypes.POINTER(IIAPI_GETQINFOPARM)]
)
IIapi_initialize = bind_function(
    iiapi, 'IIapi_initialize', None, [ctypes.POINTER(IIAPI_INITPARM)]
)
IIapi_modifyConnect = bind_function(
    iiapi, 'IIapi_modifyConnect', None, [ctypes.POINTER(IIAPI_MODCONNPARM)]
)
IIapi_position = bind_function(
    iiapi, 'IIapi_position', None, [ctypes.POINTER(IIAPI_POSPARM)]
)
IIapi_prepareCommit = bind_function(
    iiapi, 'IIapi_prepareCommit', None, [ctypes.POINTER(IIAPI_PREPCMTPARM)]
)
IIapi_putColumns = bind_function(
    iiapi, 'IIapi_putColumns', None, [ctypes.POINTER(IIAPI_PUTCOLPARM)]
)
IIapi_putParms = bind_function(
    iiapi, 'IIapi_putParms', None, [ctypes.POINTER(IIAPI_PUTPARMPARM)]
)
IIapi_query = bind_function(
    iiapi, 'IIapi_query', None, [ctypes.POINTER(IIAPI_QUERYPARM)]
)
IIapi_registerXID = bind_function(
    iiapi, 'IIapi_registerXID', None, [ctypes.POINTER(IIAPI_REGXIDPARM)]
)
IIapi_releaseEnv = bind_function(
    iiapi, 'IIapi_releaseEnv', None, [ctypes.POINTER(IIAPI_RELENVPARM)]
)
IIapi_releaseXID = bind_function(
    iiapi, 'IIapi_releaseXID', None, [ctypes.POINTER(IIAPI_RELXIDPARM)]
)
IIapi_rollback = bind_function(
    iiapi, 'IIapi_rollback', None, [ctypes.POINTER(IIAPI_ROLLBACKPARM)]
)
IIapi_savePoint = bind_function(
    iiapi, 'IIapi_savePoint', None, [ctypes.POINTER(IIAPI_SAVEPTPARM)]
)
IIapi_scroll = bind_function(
    iiapi, 'IIapi_scroll', None, [ctypes.POINTER(IIAPI_SCROLLPARM)]
)
IIapi_setConnectParam = bind_function(
    iiapi, 'IIapi_setConnectParam', None, [ctypes.POINTER(IIAPI_SETCONPRMPARM)]
)
IIapi_setDescriptor = bind_function(
    iiapi, 'IIapi_setDescriptor', None, [ctypes.POINTER(IIAPI_SETDESCRPARM)]
)
IIapi_setEnvParam = bind_function(
    iiapi, 'IIapi_setEnvParam', None, [ctypes.POINTER(IIAPI_SETENVPRMPARM)]
)
IIapi_terminate = bind_function(
    iiapi, 'IIapi_terminate', None, [ctypes.POINTER(IIAPI_TERMPARM)]
)
IIapi_wait = bind_function(iiapi, 'IIapi_wait', None, [ctypes.POINTER(IIAPI_WAITPARM)])
IIapi_xaCommit = bind_function(
    iiapi, 'IIapi_xaCommit', None, [ctypes.POINTER(IIAPI_XACOMMITPARM)]
)
IIapi_xaStart = bind_function(
    iiapi, 'IIapi_xaStart', None, [ctypes.POINTER(IIAPI_XASTARTPARM)]
)
IIapi_xaEnd = bind_function(
    iiapi, 'IIapi_xaEnd', None, [ctypes.POINTER(IIAPI_XAENDPARM)]
)
IIapi_xaPrepare = bind_function(
    iiapi, 'IIapi_xaPrepare', None, [ctypes.POINTER(IIAPI_XAPREPPARM)]
)
IIapi_xaRollback = bind_function(
    iiapi, 'IIapi_xaRollback', None, [ctypes.POINTER(IIAPI_XAROLLPARM)]
)


##  OpenAPI callback factory
IIAPI_CBFUNC = ctypes.CFUNCTYPE(
    None, 
    ctypes.POINTER(ctypes.c_void_p), 
    ctypes.POINTER(ctypes.c_void_p)
)


def trace(iiapi_func):
    '''decorator to trace (OpenAPI) calls'''

    @wraps(iiapi_func)
    def api_trace(*args, **kwargs):
        logger.opt(depth=1).trace(f'calling {iiapi_func.__name__}()...')
        result = iiapi_func(*args, **kwargs)
        logger.opt(depth=1).trace(f'... {iiapi_func.__name__} returned')
        return result

    return api_trace


##  trace the OpenAPI calls when IIAPI_DEV_MODE envar is set
if IIAPI_DEV_MODE:
    IIapi_abort = trace(IIapi_abort)
    IIapi_autocommit = trace(IIapi_autocommit)
    IIapi_batch = trace(IIapi_batch)
    IIapi_cancel = trace(IIapi_cancel)
    IIapi_catchEvent = trace(IIapi_catchEvent)
    IIapi_close = trace(IIapi_close)
    IIapi_commit = trace(IIapi_commit)
    IIapi_connect = trace(IIapi_connect)
    IIapi_convertData = trace(IIapi_convertData)
    IIapi_disconnect = trace(IIapi_disconnect)
    IIapi_formatData = trace(IIapi_formatData)
    IIapi_getColumnInfo = trace(IIapi_getColumnInfo)
    IIapi_getColumns = trace(IIapi_getColumns)
    IIapi_getCopyMap = trace(IIapi_getCopyMap)
    IIapi_getDescriptor = trace(IIapi_getDescriptor)
    IIapi_getErrorInfo = trace(IIapi_getErrorInfo)
    IIapi_getEvent = trace(IIapi_getEvent)
    IIapi_getQueryInfo = trace(IIapi_getQueryInfo)
    IIapi_initialize = trace(IIapi_initialize)
    IIapi_modifyConnect = trace(IIapi_modifyConnect)
    IIapi_position = trace(IIapi_position)
    IIapi_prepareCommit = trace(IIapi_prepareCommit)
    IIapi_putColumns = trace(IIapi_putColumns)
    IIapi_putParms = trace(IIapi_putParms)
    IIapi_query = trace(IIapi_query)
    IIapi_registerXID = trace(IIapi_registerXID)
    IIapi_releaseEnv = trace(IIapi_releaseEnv)
    IIapi_releaseXID = trace(IIapi_releaseXID)
    IIapi_rollback = trace(IIapi_rollback)
    IIapi_savePoint = trace(IIapi_savePoint)
    IIapi_scroll = trace(IIapi_scroll)
    IIapi_setConnectParam = trace(IIapi_setConnectParam)
    IIapi_setDescriptor = trace(IIapi_setDescriptor)
    IIapi_setEnvParam = trace(IIapi_setEnvParam)
    IIapi_terminate = trace(IIapi_terminate)
    IIapi_wait = trace(IIapi_wait)
    IIapi_xaCommit = trace(IIapi_xaCommit)
    IIapi_xaStart = trace(IIapi_xaStart)
    IIapi_xaEnd = trace(IIapi_xaEnd)
    IIapi_xaPrepare = trace(IIapi_xaPrepare)
    IIapi_xaRollback = trace(IIapi_xaRollback)
