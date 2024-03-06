# pyngres.asyncio

The `pyngres.asyncio` subpackage wraps the *Actian OpenIngres API*
to make the asynchronous OpenAPI functions awaitable. This not only
allows us to write code that is less cluttered and more Pythonic, it
is also necessary if we expect to use the Actian OpenAPI in event-driven
applications (e.g. GUIs) without degrading their responsiveness.

We have supplied only a couple of examples to illustrate the use of the OpenAPI 
with `asyncio`. Just two changes are required to the way an asynchronous
OpenAPI function is called; we `await` it, and we do not call 
`IIapi_wait()` to detect completion. (Do note that some OpenAPI functions are
in fact synchronous and cannot be awaited.) 

The examples here include one additional function, called `spin()`. This 
function displays an animated "spinner" concurrently with the OpenAPI 
operations. It is intended to show that database operations are 
automatically interleaved with other asynchronous operations.

All the other differences in the code are a consequence of how
`asyncio` has to be used and will make sense to anyone familiar with
`asyncio`. (Anyone not already fully familiar with `asyncio` should definitely
pause to study it before attempting to exploit `pyngres.asyncio`! There
is a learning curve to `asyncio`.)

