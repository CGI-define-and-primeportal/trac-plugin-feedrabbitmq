from trac.core import Component, implements
from trac.config import Option, BoolOption
from trac.ticket.api import ITicketChangeListener
from kombu import Connection, Exchange, Queue
import datetime
import os
import re

## If you want to see the messages, try:
# amqp-consume -u amqp://guest:guest@localhost/%2F -q ticket_event_feed cat
## or
# while : ; do amqp-get -u amqp://guest:guest@localhost/%2F -q ticket_event_feed && echo "------------" ; sleep 1; done


# TODO support IAttachmentChangeListener, IMilestoneChangeListener
# TODO invent IHoursListener (Trachours plugin)
# TODO admin ui to configure project_identifier and turn on active

class Listener(Component):
    implements(ITicketChangeListener)

    amqp = Option("amqp", "broker", default="amqp://guest:guest@localhost//")
    project_identifier = Option("amqp", "project_identifer")
    queue_name = Option("amqp", "queue", default="ticket_event_feed")
    active = BoolOption("amqp", "active", default=False)    

    def ticket_created(self, ticket):
        event = {k: self._transform_value(k, ticket[k]) for k in ticket.values}
        # TODO should we put a timezone mark on _time just in case, even though we say it'll be UTC?
        event.update({"_category": "created",
                      "_time": datetime.datetime.utcnow(),
                      "_ticket": ticket.id,
                      "_author": ticket['reporter']})
        self._send_events([event])
            
    def ticket_changed(self, ticket, comment, author, old_values):
        _time = datetime.datetime.utcnow()
        self._send_events(({"_category": "changed",
                            "_time": _time,
                            "_ticket": ticket.id,
                            "_author": author,
                            k: self._transform_value(k, ticket[k])}
                           for k, v in old_values.iteritems()))
    
    def ticket_deleted(self, ticket):
        event = {"_category": "deleted",
                 "_time": datetime.datetime.utcnow(),
                 "_ticket": ticket.id}
        self._send_events([event])

    def ticket_comment_modified(self, ticket, cdate, author, comment, old_comment):
        # we don't send comments to the queue anyway
        pass

    def ticket_change_deleted(self, ticket, cdate, changes):
        # we don't support this, as the authors of this plugin don't
        # support deleting changes in our downstream product
        pass

    def _transform_value(self, field, value):
        if field in ("cc", "keywords"):
            # note, Trac uses '[;,\s]+' (see trac/ticket/model.py)
            # but CGI's fork doesn't include the whitespace
            return [x.strip() for x in re.split(r'[;,]+', value)]
        # TODO deal with integer, date, float fields (CGI extensions)
        # TODO ensure that 'changetime' is in UTC?
        return value

    def _send_events(self, events):
        # TODO should we actually be creating Connection() and queue in __init__?
        if not self.active:
            return
        self.log.debug("Connecting to %s with queue %s", self.amqp, self.queue_name)
        with Connection(self.amqp) as conn:
            queue = conn.SimpleQueue(self.queue_name,
                                     queue_opts={'durable': True})
            for event in events:
                event['_project'] = self.project_identifier or os.path.basename(self.env.path)
                self.log.debug("Putting event %s", event)
                queue.put(event,
                          serializer="yaml")
        
