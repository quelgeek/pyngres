# Pyngres

Pyngres is a Python wrapper for the Actian Ingres/Vector/X OpenAPI. It is
currently available for Windows, Linux, and Darwin.

## Installation

Pyngres requires Python 3.8 or higher. 

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install pyngres.

```bash
pip install pyngres
```

(Note for **Conda** and **Miniconda** users: there is an as-yet undiagnosed problem that prevents **pip** from properly resolving the dependency on **loguru**. As a workaround install loguru before installing pyngres.)

## Usage

The pyngres package contains three modules. The base **pyngres** module is a set of
Python bindings for the Actian OpenAPI. It is used exactly like the iilibapi 
library is used in a C program. The programmer is expected to use `IIapi_wait()` to 
complete asynchronous OpenAPI calls. Anyone familiar with the OpenAPI and
with Python programming will find their expertise using the OpenAPI in C is 
directly transferable. 

The second module is **pyngres.asyncio** which makes pyngres
usable with the Python asyncio package. It provides very clean support for
the asynchronous OpenAPI functions. The third package is **pyngres.blocking**, which
simply conceals the busy-wait loops that are required when using the OpenAPI 
in a synchronous application.

```python
import pyngres as py
```
or
```python
import pyngres.asyncio as py
```
or
```python
import pyngres.blocking as py
```

**Linux** and **Darwin:** initialize your Ingres environment by executing **~/.ing**XX**sh**, where XX
is your installation identifier (usually **II** or **AC**). Example:

```
. ~/.ingIIsh
```

**Windows:** your Ingres installation will usually be initialized already.

## Debugging and Diagnostics

Set the **IIAPI_DEV_MODE** environment variable to **ON** or call **loguru.enable('pyngres')**
in your application code to start **pyngres** tracing using 
[Loguru](https://loguru.readthedocs.io/en/stable/).

You can also enable Ingres OpenAPI tracing and Ingres GCA tracing. (We have generally found the latter most useful.) 

Ingres session tracing using **SET SERVER_TRACE** (or **SET TRACE POINT SC930**) is also extremely useful. Refer to the [Actian Ingres SQL Reference Guide](https://docs.actian.com/actianx/12.0/index.html#page/SQLRef/SERVER_TRACE.htm) for more information.

### Windows Debugging Environment Variables
```
set IIAPI_DEV_MODE=ON
set LOGURU_LEVEL=TRACE
set II_API_LOG=c:\temp\api.log
set II_API_TRACE=5
```
To enable optional GCA trace messages to also be written to the API log file:
```
set II_API_SET='printtrace'
```

### Server-Side Ingres Tracing (in **sql**, Windows)

```
SET SERVER_TRACE ON WITH DETAIL, DIRECTORY='C:\temp\session_logs'
```

### Linux/MacOs Debugging Environment Variables

```
export IIAPI_DEV_MODE=ON
export LOGURU_LEVEL=TRACE
export II_API_LOG=/tmp/api.log
export II_API_TRACE=5
```
To enable optional GCA trace messages to also be written to the API log file:
```
export II_API_SET='printtrace'
```

### Server-Side Ingres Tracing (in **sql**, Linux/MacOS)

```
SET SERVER_TRACE ON WITH DETAIL, DIRECTORY='/tmp/session_logs'
```

## pyngres.asyncio

The pyngres.asyncio package can be used in place of the bare-bones pyngres
package. The OpenAPI is intrinsically asynchronous. The OpenAPI sends a request to a 
server and then returns to the caller without waiting for the request to 
be completed. Ordinarily, using the bare-bones OpenAPI, that would mean
it is up to the programmer to insert calls to `IIapi_wait()` 
to pass control back to the OpenAPI so that it can set the gp_completed flag 
and invoke any callback routine that was requested. Using pyngres.asyncio 
instead with the Python asyncio features is much tidier.

Python asyncio provides infrastructure and well-known design patterns that
make asynchronous programming more convenient and supports
concurrent execution. The asyncio package facilitates the development of 
highly responsive event-driven applications, such as GUI applications.

**pyngres.asyncio** supports all the same entry points, but all the asynchronous 
OpenAPI functions are awaitable. They are executed in the asyncio event loop
concurrently with any other awaitables. (The one exception is the `IIapi_wait()`
function which does not exist in pyngres.asyncio; it would serve no purpose.)

```python
import pyngres.asyncio as py
```

## pyngres.blocking

OpenAPI applications that have no need to cooperate with other asyncio 
packages can use the pyngres.blocking package. It is functionally identical
to the base pyngres package except that all the of the OpenAPI functions 
block until completion in a platform independent way. This slightly reduces
the visual clutter of hard-coded loops calling `IIapi_wait()`, and eliminates
the risk of omitting a necessary wait loop. 

```python
import pyngres.blocking as py
```

## API

See [OpenAPI User Guide](https://docs.actian.com/ingres/11.2/#page/OpenAPIUser/OpenAPIUser_Title.htm) for details on the use the Ingres OpenAPI. The following API functions are supported by pyngres:

- `IIapi_abort()`
- `IIapi_autocommit()`
- `IIapi_batch()`
- `IIapi_cancel()`
- `IIapi_catchEvent()`
- `IIapi_close()`
- `IIapi_commit()`
- `IIapi_connect()`
- `IIapi_convertData()`
- `IIapi_disconnect()`
- `IIapi_formatData()`
- `IIapi_getColumnInfo()`
- `IIapi_getColumns()`
- `IIapi_getCopyMap()`
- `IIapi_getDescriptor()`
- `IIapi_getErrorInfo()`
- `IIapi_getEvent()`
- `IIapi_getQueryInfo()`
- `IIapi_initialize()`
- `IIapi_modifyConnect()`
- `IIapi_position()`
- `IIapi_prepareCommit()`
- `IIapi_putColumns()`
- `IIapi_putParms()`
- `IIapi_query()`
- `IIapi_registerXID()`
- `IIapi_releaseEnv()`
- `IIapi_releaseXID()`
- `IIapi_rollback()`
- `IIapi_savePoint()`
- `IIapi_scroll()`
- `IIapi_setConnectParam()`
- `IIapi_setDescriptor()`
- `IIapi_setEnvParam()`
- `IIapi_terminate()`
- `IIapi_wait()`
- `IIapi_xaCommit()`
- `IIapi_xaStart()`
- `IIapi_xaEnd()`
- `IIapi_xaPrepare()`
- `IIapi_xaRollback()`

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)

