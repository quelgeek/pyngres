# pyngres

pyngres is a Python package that wraps the *Actian OpenIngres API*.

## Example Code

The examples run both on Linux and on Windows. Paths are shown here using
Linux syntax only for brevity.

An Ingres installation rooted at II_SYSTEM contains a directory called
ingres/demo/api in which there is a set of example C programs showing the
OpenAPI being used:

apis2ph1.c  apiscdel.c  apiscupd.c  apisname.c  apisproc.c  apisscrl.c
apis2ph2.c  apiscomm.c  apisdata.c  apisparm.c  apisprrp.c  apisselc.c
apisauto.c  apisconn.c  apiserr.c   apisprbr.c  apisrept.c  apissell.c
apisblob.c  apiscopy.c  apisinit.c  apisprgt.c  apisroll.c

Correspondingly, we provide the following Python examples:

apis2ph1.py  apiscdel.py  apiscupd.py  apisname.py  apisproc.py  apisscrl.py
apis2ph2.py  apiscomm.py  apisdata.py  apisparm.py  apisprrp.py  apisselc.py
apisauto.py  apisconn.py  apiserr.py   apisprbr.py  apisrept.py  apissell.py
apisblob.py  apiscopy.py  apisinit.py  apisprgt.py  apisroll.py

## Running Example Code

Our examples expect Python 3.6 or higher. However, if you prefer to use an
older version you should have no difficulty making the necessary changes to 
run the examples.

You will need to install pyngres into your environment.
```
pip install pyngres
```

The pyngres library has a dependency on loguru. **pip** will usually resolve
it automatically but there is a known problem when using conda/miniconda
which prevents it from installing. The workaround is to install loguru
first, before installing pyngres.

You will need to have a running Ingres installation available, your local
Ingres environment set up, and a local copy of the OpenAPI. You should have
a database available.

To run (for example) apisconn.py against a local Ingres database called
demobase:
```
python apisconn.py demobase
```

## Important Notes About the Example Code
Ingres expects queries in the form of C strings (i.e. NUL-terminated
arrays of bytes, not Python strings. e.g. ```qry = b'SELECT * FROM foo'```.

Some of the older C examples use the default level 1 API. It is not
obvious from those examples that later versions of the API return an
**envHandle** that has to be passed to the **IIapi_connect()** function using 
the **connHandle** member of the **IIAPI_CONNPARM** object. That is an 
important "gotcha" if you rely on the examples. We have adapted most of
our Python examples to show the way you would really connect today. We choose
**IIAPI_VERSION_11** explicitly. (**IIAPI_VERSION** would select the latest version.)

It is important to remember that Python objects created within a scope get 
garbage-collected when control passes out of that scope. That is why some of
the Python code explicitly returns a value where the original C code relied 
on side-effects. For example, functions that initiate a transaction 
will return the transaction handle as their value.

Because pyngres invokes the dynamic OpenAPI, standard OpenAPI and GCA tracing
work in the usual way.

The example Python code we supply here is a mostly slavish[^1] transliteration of the 
original C code. We have followed the original code as closely as possible
to make it relatively easy to do a side-by-side comparison with the original
examples. In multiple ways the resulting Python code is not Pythonic and should
not be taken as a recommendation of how to use pyngres in production code.
In fact there are several features of our example Python code that we would
actively discourage. Features that we discourage include:

```from pyngres import *```
where we would actually suggest 
```import pyngres as ii```

The examples rarely do any error handling unless the example is specifically 
illustrating error handling.

The examples initialize unused members to None, mirroring the NULLs used
in the C code, but that is not actually necessary or useful and is just
visual clutter.

We don't use context managers in these examples. (Obvious situations where
a context manager makes sense are starting and ending Ingres sessions;
starting and committing transactions, and changing and restoring lockmodes.)

[^1]: "mostly slavish" meaning: apart from the above-noted effort to illustrate
the use of the envHandle when calling IIapi_connect().
