##  Copyright (c) 2022 Rational Commerce Ltd.

'''
Python bindings for the Actian Ingres OpenAPI
'''


from functools import wraps
import ctypes as C
from loguru import logger
import os
from sys import platform


from .IIAPI_CONSTANTS import *
from .IIAPI_PARM import *


_name = __name__


##  pick up environment settings
IIAPI_DEV_MODE = ('IIAPI_DEV_MODE' in os.environ
    and os.environ['IIAPI_DEV_MODE'] == 'ON')
IIAPI_DEBUG_ONERROR = ('IIAPI_DEBUG_ONERROR' in os.environ 
    and os.environ['IIAPI_DEBUG_ONERROR'] == 'ON')


##  IIAPI_DEV_MODE=ON puts pyngres in development mode
if IIAPI_DEV_MODE:
    import importlib.metadata
    _version = importlib.metadata.version(_name)
    logger.warning(f'using {_name} {_version} in development mode')
    logger.info(f'to disable logging messages: logger.disable(\'{_name}\')')
    logger.info(f'to enable logging messages: logger.enable(\'{_name}\')')
else:
    logger.disable(_name)


##  load the Ingres OpenAPI using ctypes
if platform == 'linux':
    ##  expect to find libiiapi.1.so in $II_SYSTEM/ingres/lib
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
elif platform == 'darwin':
    ##  expect to find libq.1.dylib and libiiapi.1.dylib in 
    ##  $II_SYSTEM/ingres/lib; libq.1.dylib needs to be loaded before loading
    ##  libiiapai.1.dylib
    try:
        II_SYSTEM = os.environ['II_SYSTEM']
    except KeyError:
        print('II_SYSTEM is not set in the environment')
        quit()
    except Exception as exception:
        print(exception)
        quit()
    api_pathname = f'{II_SYSTEM}/ingres/lib/libiiapi.1.dylib'
    ##  load libq so it's ready when we load the OpenAPI
    libq_pathname = f'{II_SYSTEM}/ingres/lib/libq.1.dylib'
    logger.debug(f'attempting to load {libq_pathname}')
    libq = C.CDLL(libq_pathname, mode=C.RTLD_GLOBAL)
    logger.success(f'loaded {libq_pathname}')
else:
    print(f'No python binding is available for Ingres OpenAPI on {platform}.')
    print('(If the API exists, a binding is easy to add. Give us a call.)')
    quit()
##  don't use try/except here; just let it explode if it wants to
logger.debug(f'attempting to load {api_pathname}')
iiapi = C.CDLL(api_pathname)
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
    iiapi, 'IIapi_abort', None, [C.POINTER(IIAPI_ABORTPARM)]
)
IIapi_autocommit = bind_function(
    iiapi, 'IIapi_autocommit', None, [C.POINTER(IIAPI_AUTOPARM)]
)
IIapi_batch = bind_function(
    iiapi, 'IIapi_batch', None, [C.POINTER(IIAPI_BATCHPARM)]
)
IIapi_cancel = bind_function(
    iiapi, 'IIapi_cancel', None, [C.POINTER(IIAPI_CANCELPARM)]
)
IIapi_catchEvent = bind_function(
    iiapi, 'IIapi_catchEvent', None, [C.POINTER(IIAPI_CATCHEVENTPARM)]
)
IIapi_close = bind_function(
    iiapi, 'IIapi_close', None, [C.POINTER(IIAPI_CLOSEPARM)]
)
IIapi_commit = bind_function(
    iiapi, 'IIapi_commit', None, [C.POINTER(IIAPI_COMMITPARM)]
)
IIapi_connect = bind_function(
    iiapi, 'IIapi_connect', None, [C.POINTER(IIAPI_CONNPARM)]
)
IIapi_convertData = bind_function(
    iiapi, 'IIapi_convertData', None, [C.POINTER(IIAPI_CONVERTPARM)]
)
IIapi_disconnect = bind_function(
    iiapi, 'IIapi_disconnect', None, [C.POINTER(IIAPI_DISCONNPARM)]
)
IIapi_formatData = bind_function(
    iiapi, 'IIapi_formatData', None, [C.POINTER(IIAPI_FORMATPARM)]
)
IIapi_getColumnInfo = bind_function(
    iiapi, 'IIapi_getColumnInfo', None, [C.POINTER(IIAPI_GETCOLINFOPARM)]
)
IIapi_getColumns = bind_function(
    iiapi, 'IIapi_getColumns', None, [C.POINTER(IIAPI_GETCOLPARM)]
)
IIapi_getCopyMap = bind_function(
    iiapi, 'IIapi_getCopyMap', None, [C.POINTER(IIAPI_GETCOPYMAPPARM)]
)
IIapi_getDescriptor = bind_function(
    iiapi, 'IIapi_getDescriptor', None, [C.POINTER(IIAPI_GETDESCRPARM)]
)
IIapi_getErrorInfo = bind_function(
    iiapi, 'IIapi_getErrorInfo', None, [C.POINTER(IIAPI_GETEINFOPARM)]
)
IIapi_getEvent = bind_function(
    iiapi, 'IIapi_getEvent', None, [C.POINTER(IIAPI_GETEVENTPARM)]
)
IIapi_getQueryInfo = bind_function(
    iiapi, 'IIapi_getQueryInfo', None, [C.POINTER(IIAPI_GETQINFOPARM)]
)
IIapi_initialize = bind_function(
    iiapi, 'IIapi_initialize', None, [C.POINTER(IIAPI_INITPARM)]
)
IIapi_modifyConnect = bind_function(
    iiapi, 'IIapi_modifyConnect', None, [C.POINTER(IIAPI_MODCONNPARM)]
)
IIapi_position = bind_function(
    iiapi, 'IIapi_position', None, [C.POINTER(IIAPI_POSPARM)]
)
IIapi_prepareCommit = bind_function(
    iiapi, 'IIapi_prepareCommit', None, [C.POINTER(IIAPI_PREPCMTPARM)]
)
IIapi_putColumns = bind_function(
    iiapi, 'IIapi_putColumns', None, [C.POINTER(IIAPI_PUTCOLPARM)]
)
IIapi_putParms = bind_function(
    iiapi, 'IIapi_putParms', None, [C.POINTER(IIAPI_PUTPARMPARM)]
)
IIapi_query = bind_function(
    iiapi, 'IIapi_query', None, [C.POINTER(IIAPI_QUERYPARM)]
)
IIapi_registerXID = bind_function(
    iiapi, 'IIapi_registerXID', None, [C.POINTER(IIAPI_REGXIDPARM)]
)
IIapi_releaseEnv = bind_function(
    iiapi, 'IIapi_releaseEnv', None, [C.POINTER(IIAPI_RELENVPARM)]
)
IIapi_releaseXID = bind_function(
    iiapi, 'IIapi_releaseXID', None, [C.POINTER(IIAPI_RELXIDPARM)]
)
IIapi_rollback = bind_function(
    iiapi, 'IIapi_rollback', None, [C.POINTER(IIAPI_ROLLBACKPARM)]
)
IIapi_savePoint = bind_function(
    iiapi, 'IIapi_savePoint', None, [C.POINTER(IIAPI_SAVEPTPARM)]
)
IIapi_scroll = bind_function(
    iiapi, 'IIapi_scroll', None, [C.POINTER(IIAPI_SCROLLPARM)]
)
IIapi_setConnectParam = bind_function(
    iiapi, 'IIapi_setConnectParam', None, [C.POINTER(IIAPI_SETCONPRMPARM)]
)
IIapi_setDescriptor = bind_function(
    iiapi, 'IIapi_setDescriptor', None, [C.POINTER(IIAPI_SETDESCRPARM)]
)
IIapi_setEnvParam = bind_function(
    iiapi, 'IIapi_setEnvParam', None, [C.POINTER(IIAPI_SETENVPRMPARM)]
)
IIapi_terminate = bind_function(
    iiapi, 'IIapi_terminate', None, [C.POINTER(IIAPI_TERMPARM)]
)
IIapi_wait = bind_function(
    iiapi, 'IIapi_wait', None, [C.POINTER(IIAPI_WAITPARM)]
)
IIapi_xaCommit = bind_function(
    iiapi, 'IIapi_xaCommit', None, [C.POINTER(IIAPI_XACOMMITPARM)]
)
IIapi_xaStart = bind_function(
    iiapi, 'IIapi_xaStart', None, [C.POINTER(IIAPI_XASTARTPARM)]
)
IIapi_xaEnd = bind_function(
    iiapi, 'IIapi_xaEnd', None, [C.POINTER(IIAPI_XAENDPARM)]
)
IIapi_xaPrepare = bind_function(
    iiapi, 'IIapi_xaPrepare', None, [C.POINTER(IIAPI_XAPREPPARM)]
)
IIapi_xaRollback = bind_function(
    iiapi, 'IIapi_xaRollback', None, [C.POINTER(IIAPI_XAROLLPARM)]
)


##  OpenAPI callback assistance functions

IIapi_callback = C.CFUNCTYPE( None, C.POINTER(C.py_object), IIAPI_GENPARM )


def IIapi_getCallbackPtr(callback):
    '''return c_void_p pointer to an OpenAPI-compliant Python callback'''
    ptr =  C.cast(callback,C.c_void_p)
    return ptr


def IIapi_getClosurePtr(object):
    '''return c_void_p pointer to Python object to use as callback closure'''
    closure = C.pointer(C.py_object(object))
    ptr = C.cast(closure,C.c_void_p)
    return ptr


def IIapi_getClosure(ptr):
    '''return the Python object pointed to by C.c_void_p pointer'''
    object = C.cast(ptr,C.POINTER(C.py_object))
    closure = object.contents.value
    return closure


##  trace the OpenAPI calls when IIAPI_DEV_MODE envar is set

def traced(iiapi_func):
    '''decorator to trace (OpenAPI) calls'''

    @wraps(iiapi_func)
    def api_trace(*args, **kwargs):
        logger.opt(depth=1).trace(f'calling {iiapi_func.__name__}()...')
        result = iiapi_func(*args, **kwargs)
        logger.opt(depth=1).trace(f'... {iiapi_func.__name__} returned')
        return result
    return api_trace


if IIAPI_DEV_MODE:
    IIapi_abort = traced(IIapi_abort)
    IIapi_autocommit = traced(IIapi_autocommit)
    IIapi_batch = traced(IIapi_batch)
    IIapi_cancel = traced(IIapi_cancel)
    IIapi_catchEvent = traced(IIapi_catchEvent)
    IIapi_close = traced(IIapi_close)
    IIapi_commit = traced(IIapi_commit)
    IIapi_connect = traced(IIapi_connect)
    IIapi_convertData = traced(IIapi_convertData)
    IIapi_disconnect = traced(IIapi_disconnect)
    IIapi_formatData = traced(IIapi_formatData)
    IIapi_getColumnInfo = traced(IIapi_getColumnInfo)
    IIapi_getColumns = traced(IIapi_getColumns)
    IIapi_getCopyMap = traced(IIapi_getCopyMap)
    IIapi_getDescriptor = traced(IIapi_getDescriptor)
    IIapi_getErrorInfo = traced(IIapi_getErrorInfo)
    IIapi_getEvent = traced(IIapi_getEvent)
    IIapi_getQueryInfo = traced(IIapi_getQueryInfo)
    IIapi_initialize = traced(IIapi_initialize)
    IIapi_modifyConnect = traced(IIapi_modifyConnect)
    IIapi_position = traced(IIapi_position)
    IIapi_prepareCommit = traced(IIapi_prepareCommit)
    IIapi_putColumns = traced(IIapi_putColumns)
    IIapi_putParms = traced(IIapi_putParms)
    IIapi_query = traced(IIapi_query)
    IIapi_registerXID = traced(IIapi_registerXID)
    IIapi_releaseEnv = traced(IIapi_releaseEnv)
    IIapi_releaseXID = traced(IIapi_releaseXID)
    IIapi_rollback = traced(IIapi_rollback)
    IIapi_savePoint = traced(IIapi_savePoint)
    IIapi_scroll = traced(IIapi_scroll)
    IIapi_setConnectParam = traced(IIapi_setConnectParam)
    IIapi_setDescriptor = traced(IIapi_setDescriptor)
    IIapi_setEnvParam = traced(IIapi_setEnvParam)
    IIapi_terminate = traced(IIapi_terminate)
    IIapi_wait = traced(IIapi_wait)
    IIapi_xaCommit = traced(IIapi_xaCommit)
    IIapi_xaStart = traced(IIapi_xaStart)
    IIapi_xaEnd = traced(IIapi_xaEnd)
    IIapi_xaPrepare = traced(IIapi_xaPrepare)
    IIapi_xaRollback = traced(IIapi_xaRollback)


def debugged(function):
    '''decorator to debug on error'''
    ##  WARNING: using IIAPI_DEBUG_ONERROR calls all OpenAPI functions
    ##  synchronously, so pyngres.asyncio performance will be degraded
    def break_on_error(arg):
        ##  does arg contain an IIAPI_GENPARM? (i.e. function is asynchronous)?
        genParm = arg.genParm()
        if genParm:
            ##  function is asynchronous; status not available until completed
            function(arg)
            wtp = IIAPI_WAITPARM()
            wtp.wt_timeout = -1
            while not genParm.gp_completed:
                IIapi_wait(wtp)
            status = genParm.gp_status
        else:
            ##  function is synchronous; get status by suffix
            function(arg)
            status = arg.field_by_suffix('status')

        if status and status >= IIAPI_ST_ERROR:
            name = function.__name__
            logger.error(f'{name}() returned {status=}')
            print(arg)
            logger.info('(Pdb) is the debugger prompt')
            logger.info('to advance to the next statement press "n" then enter')
            logger.info('to continue/resume executing press "c" then enter')
            logger.info('for help press "h" then enter')
            breakpoint()
    return break_on_error
 

##  IIAPI_DEBUG_ONERROR=ON causes pyngres to invoke the pdb degugger
##  when an OpenAPI error is detected
if IIAPI_DEBUG_ONERROR:
    IIapi_abort = debugged(IIapi_abort)
    IIapi_autocommit = debugged(IIapi_autocommit)
    IIapi_batch = debugged(IIapi_batch)
    IIapi_cancel = debugged(IIapi_cancel)
    IIapi_catchEvent = debugged(IIapi_catchEvent)
    IIapi_close = debugged(IIapi_close)
    IIapi_commit = debugged(IIapi_commit)
    IIapi_connect = debugged(IIapi_connect)
    IIapi_convertData = debugged(IIapi_convertData)
    IIapi_disconnect = debugged(IIapi_disconnect)
    IIapi_formatData = debugged(IIapi_formatData)
    IIapi_getColumnInfo = debugged(IIapi_getColumnInfo)
    IIapi_getColumns = debugged(IIapi_getColumns)
    IIapi_getCopyMap = debugged(IIapi_getCopyMap)
    IIapi_getDescriptor = debugged(IIapi_getDescriptor)
    IIapi_getErrorInfo = debugged(IIapi_getErrorInfo)
    IIapi_getEvent = debugged(IIapi_getEvent)
    IIapi_getQueryInfo = debugged(IIapi_getQueryInfo)
    IIapi_initialize = debugged(IIapi_initialize)
    IIapi_modifyConnect = debugged(IIapi_modifyConnect)
    IIapi_position = debugged(IIapi_position)
    IIapi_prepareCommit = debugged(IIapi_prepareCommit)
    IIapi_putColumns = debugged(IIapi_putColumns)
    IIapi_putParms = debugged(IIapi_putParms)
    IIapi_query = debugged(IIapi_query)
    IIapi_registerXID = debugged(IIapi_registerXID)
    IIapi_releaseEnv = debugged(IIapi_releaseEnv)
    IIapi_releaseXID = debugged(IIapi_releaseXID)
    IIapi_rollback = debugged(IIapi_rollback)
    IIapi_savePoint = debugged(IIapi_savePoint)
    IIapi_scroll = debugged(IIapi_scroll)
    IIapi_setConnectParam = debugged(IIapi_setConnectParam)
    IIapi_setDescriptor = debugged(IIapi_setDescriptor)
    IIapi_setEnvParam = debugged(IIapi_setEnvParam)
    IIapi_terminate = debugged(IIapi_terminate)
    IIapi_wait = debugged(IIapi_wait)
    IIapi_xaCommit = debugged(IIapi_xaCommit)
    IIapi_xaStart = debugged(IIapi_xaStart)
    IIapi_xaEnd = debugged(IIapi_xaEnd)
    IIapi_xaPrepare = debugged(IIapi_xaPrepare)
    IIapi_xaRollback = debugged(IIapi_xaRollback)
    
    logger.warning('on-error debugging using pdb is enabled')
