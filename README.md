# trac-plugin-feedrabbitmq
Trac plugin to feed Trac events to a RabbitMQ queue

An intermediate design was ICeleryTask, but instead we're already moving towards IAsyncTicketChangeListener.

ICeleryTask
-----------

One worker daemon per 'task' type. Uses a newly invented 'event' data
structure as the serialisation on the wire, and worker task has to
understand how to unpack this.

IAsyncTicketChangeListener
--------------------------

One worker daemon overall. Components are coded identically to
ITicketChangeListener, but declare implementation of
IAsyncTicketChangeListener instead. The framework would then
automatically execute them in the Celery worker, rather than
tracd/mod_wsgi side.

This design can be extended to IAsyncTicketChangeListener, and so on.

Each active IAsyncTicketChangeListener component would cause a message
on the celery queue, so that if one of the components fails, the
others will still get a message of their own.
