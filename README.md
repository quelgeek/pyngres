# Pyngres

Pyngres is a Python wrapper for the Actian OpenIngres API.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install pyngres.

```bash
pip install pyngres
```

## Usage

Linux: initialize your Ingres environment by running ~/.ingXXsh, where XX
is your installation identifier (usually II).

Windows: your Ingres installation will usually be initialized already.

Calling loguru.enable('pyngres') in your application, or setting the 
IIAPI_DEV_MODE environment variable, starts API tracing tracing.

```python
from pyngres import *
```

## API

IIapi_abort()
IIapi_autocommit()
IIapi_batch()
IIapi_cancel()
IIapi_catchEvent()
IIapi_close()
IIapi_commit()
IIapi_connect()
IIapi_convertData()
IIapi_disconnect()
IIapi_formatData()
IIapi_getColumnInfo()
IIapi_getColumns()
IIapi_getCopyMap()
IIapi_getDescriptor()
IIapi_getErrorInfo()
IIapi_getEvent()
IIapi_getQueryInfo()
IIapi_initialize()
IIapi_modifyConnect()
IIapi_position()
IIapi_prepareCommit()
IIapi_putColumns()
IIapi_putParms()
IIapi_query()
IIapi_registerXID()
IIapi_releaseEnv()
IIapi_releaseXID()
IIapi_rollback()
IIapi_savePoint()
IIapi_scroll()
IIapi_setConnectParam()
IIapi_setDescriptor()
IIapi_setEnvParam()
IIapi_terminate()
IIapi_wait()
IIapi_xaCommit()
IIapi_xaStart()
IIapi_xaEnd()
IIapi_xaPrepare()
IIapi_xaRollback()

See https://docs.actian.com/ingres/11.2/#page/OpenAPIUser/OpenAPIUser_Title.htm

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
