# Pyngres

Pyngres is a Python wrapper for the Actian Ingres OpenAPI.

## Installation

Use [pip](https://pip.pypa.io/en/stable/) to install pyngres.

```bash
pip install pyngres
```

(Note for **Conda** and **Miniconda** users: there is an as-yet undiagnosed problem that prevents **pip** from properly resolving the dependency on **loguru**. As a workaround install loguru before installing pyngres.)

## Usage

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
