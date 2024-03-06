# Pyngres

Pyngres is a Python wrapper for the Actian Ingres OpenAPI.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install pyngres.

```bash
pip install pyngres
```

(Note for **Conda** and **Miniconda** users: there is an as-yet undiagnosed problem that prevents **pip** from properly resolving the dependency on **loguru**. As a workaround install loguru before installing pyngres.)

## Usage

The pyngres package contains three modules. The pyngres module is a set of
Python bindings for the Actian OpenAPI. Anyone already familiar with the OpenAPI and
with Python programming will find their expertise using the OpenAPI in C is 
directly transferable.

```python
import pyngres as ii
```

**Linux:** initialize your Ingres environment by executing **~/.ing**XX**sh**, where XX
is your installation identifier (usually **II**). Example:

```
. ~/.ingIIsh
```

**Windows:** your Ingres installation will usually be initialized already.

Set the `IIAPI_DEV_MODE` environment variable or call `loguru.enable('pyngres')` in your application code to start pyngres API tracing using [Loguru](https://loguru.readthedocs.io/en/stable/).


### pyngres.asyncio

Actian OpenAPI is intrinsically asynchronous.
The standard Python **asyncio** package supports design patterns and a well-known
infrastructure that facilitates the 
development of highly responsive event-driven applications.  

The `pyngres.asyncio` package can be used in place of the bare-bones `pyngres`
package. It supports all the same entry points, but makes the asynchronous 
OpenAPI functions awaitable. They are executed in the **asyncio** event loop
concurrently with any other awaitables. 

```python
import pyngres.asyncio as ii
```

### pyngres.blocking

OpenAPI applications that have no need to cooperate with **asyncio** 
packages can use the `pyngres.blocking` package. It is functionally identical
to the base `pyngres` package except that all the of the OpenAPI functions 
block until completion, in a platform independent way. This slightly reduces
the visual clutter of hard-coded loops calling `IIapi_wait()`, and eliminates
the risk of omitting a necessary wait loop. 

The `pyngres.blocking` package includes a null
implementation of `IIapi_wait()` so that `pyngres.blocking` can simply be
substituted for `pyngres` in an existing application without requiring 
any changes. 

```python
import pyngres.blocking as ii
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
