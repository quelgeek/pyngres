##  Copyright (c) 2024 Rational Commerce Ltd.

'''
non-blocking Python asyncio-aware Actian Vector/X/Ingres OpenAPI
'''


from functools import wraps
from loguru import logger
import asyncio
import pyngres as py
from .IIAPI_CONSTANTS import *
from .IIAPI_PARM import *
##  the following functions are synchronous and not awaitable; we just
##  import them into the pyngres.asyncio namespace
from pyngres import ( IIapi_convertData, IIapi_formatData, IIapi_getColumnInfo, 
    IIapi_getErrorInfo, IIapi_initialize, IIapi_registerXID, IIapi_releaseEnv, 
    IIapi_releaseXID, IIapi_setEnvParam, IIapi_terminate ) 
##  import the callback helper functions
from pyngres import ( IIapi_callback, IIapi_getCallbackPtr,
    IIapi_getClosurePtr, IIapi_getClosure ) 


##  the IIAPI_DEV_MODE envar puts pyngres.asyncio in development mode
_name = __name__
if 'IIAPI_DEV_MODE' in os.environ:
    IIAPI_DEV_MODE = True
    logger.warning(f'using {_name} in development mode')
    logger.info(f'to disable logging messages: logger.disable(\'{_name}\')')
    logger.info(f'to enable logging messages: logger.enable(\'{_name}\')')
else:
    IIAPI_DEV_MODE = False
    logger.disable(_name)


_NOWAIT = py.IIAPI_WAITPARM()
_NOWAIT.wt_timeout = -1


def _IIapi_yield_to_OpenAPI( future ):
    py.IIapi_wait( _NOWAIT )
    future.set_result(True)
    return future


async def _IIapi_complete( future ):
    await asyncio.sleep(0.0)
    _IIapi_yield_to_OpenAPI( future )
    await future


def _IIapi_awaitable( IIapi_function ):
    '''make a pyngres function awaitable'''
    @wraps(IIapi_function)
    async def awaitable( pcb ):
        loop = asyncio.get_event_loop()
        genParm = pcb.genParm()
        IIapi_function( pcb )
        ##  poll for completion of the the OpenAPI
        while not genParm.gp_completed:
            future = loop.create_future()
            await _IIapi_complete( future) 
    return awaitable


@_IIapi_awaitable
def IIapi_abort( pcb ):
    py.IIapi_abort( pcb )


@_IIapi_awaitable
def IIapi_autocommit(pcb):
	py.IIapi_autocommit( pcb )


@_IIapi_awaitable
def IIapi_batch(pcb):
	py.IIapi_batch( pcb )


@_IIapi_awaitable
def IIapi_cancel(pcb):
	py.IIapi_cancel( pcb )


@_IIapi_awaitable
def IIapi_catchEvent(pcb):
	py.IIapi_catchEvent( pcb )


@_IIapi_awaitable
def IIapi_close(pcb):
	py.IIapi_close( pcb )


@_IIapi_awaitable
def IIapi_commit(pcb):
	py.IIapi_commit( pcb )


@_IIapi_awaitable
def IIapi_connect(pcb):
	py.IIapi_connect( pcb )


@_IIapi_awaitable
def IIapi_disconnect(pcb):
	py.IIapi_disconnect( pcb )


@_IIapi_awaitable
def IIapi_getColumns(pcb):
	py.IIapi_getColumns( pcb )


@_IIapi_awaitable
def IIapi_getCopyMap(pcb):
	py.IIapi_getCopyMap( pcb )


@_IIapi_awaitable
def IIapi_getDescriptor(pcb):
	py.IIapi_getDescriptor( pcb )


@_IIapi_awaitable
def IIapi_getEvent(pcb):
	py.IIapi_getEvent( pcb )


@_IIapi_awaitable
def IIapi_getQueryInfo(pcb):
	py.IIapi_getQueryInfo( pcb )


@_IIapi_awaitable
def IIapi_modifyConnect(pcb):
	py.IIapi_modifyConnect( pcb )


@_IIapi_awaitable
def IIapi_position(pcb):
	py.IIapi_position( pcb )


@_IIapi_awaitable
def IIapi_prepareCommit(pcb):
	py.IIapi_prepareCommit( pcb )


@_IIapi_awaitable
def IIapi_putColumns(pcb):
	py.IIapi_putColumns( pcb )


@_IIapi_awaitable
def IIapi_putParms(pcb):
	py.IIapi_putParms( pcb )


@_IIapi_awaitable
def IIapi_query(pcb):
	py.IIapi_query( pcb )


@_IIapi_awaitable
def IIapi_rollback(pcb):
	py.IIapi_rollback( pcb )


@_IIapi_awaitable
def IIapi_savePoint(pcb):
	py.IIapi_savePoint( pcb )


@_IIapi_awaitable
def IIapi_scroll(pcb):
	py.IIapi_scroll( pcb )


@_IIapi_awaitable
def IIapi_setConnectParam(pcb):
	py.IIapi_setConnectParam( pcb )


@_IIapi_awaitable
def IIapi_setDescriptor(pcb):
	py.IIapi_setDescriptor( pcb )


##  pyngres.asyncio makes IIapi_wait() defunct so don't add it. (If someone
##  thinks they have a use for it they can explicitly import it from pyngres)


@_IIapi_awaitable
def IIapi_xaCommit(pcb):
	py.IIapi_xaCommit( pcb )


@_IIapi_awaitable
def IIapi_xaStart(pcb):
	py.IIapi_xaStart( pcb )


@_IIapi_awaitable
def IIapi_xaEnd(pcb):
	py.IIapi_xaEnd( pcb )


@_IIapi_awaitable
def IIapi_xaPrepare(pcb):
	py.IIapi_xaPrepare( pcb )


@_IIapi_awaitable
def IIapi_xaRollback(pcb):
	py.IIapi_xaRollback( pcb )
