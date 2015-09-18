Logging in Galileo
==================

Galileo utilizes the Java Logging Framework.  To make things simple, we use the
following log levels:

* CONFIG: configuration info/issues
* INFO: debug messages.  Can be disabled during production execution.
* WARNING: recoverable failures/exceptions.  Logged during production.
* SEVERE: critical failures: bugs or termination conditions.

The FINE, FINER, FINEST levels, along with stdout and stderr, should be used for
internal development purposes only.  stdout and stderr can also be used for
output in client-side applications.

INFO can be used for debugging in production, otherwise WARNING or higher should
be logged.
