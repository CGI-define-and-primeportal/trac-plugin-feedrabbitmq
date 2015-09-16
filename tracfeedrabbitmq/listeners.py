from trac.core import Component, implements
from trac.config import Option, BoolOption, ListOption
from trac.ticket.api import ITicketChangeListener
from trac.attachment import IAttachmentChangeListener
from kombu import Connection, Exchange, Queue
import datetime
import os
import re
from itertools import chain

## If you want to see the messages, try:
# amqp-consume -u amqp://guest:guest@localhost/%2F -q ticket_event_feed cat
## or
# while : ; do amqp-get -u amqp://guest:guest@localhost/%2F -q ticket_event_feed && echo "------------" ; sleep 1; done


# TODO support IMilestoneChangeListener
# TODO invent IHoursListener (Trachours plugin)
# TODO admin ui to configure project_identifier and turn on active

class Listener(Component):
    implements(ITicketChangeListener,
               IAttachmentChangeListener)

    amqp = Option("amqp", "broker", default="amqp://guest:guest@localhost//")
    project_identifier = Option("amqp", "project_identifer")
    queue_names = ListOption("amqp", "queues", default="ticket_event_feed")
    active = BoolOption("amqp", "active", default=False)    

    def ticket_created(self, ticket):
        event = {k: self._transform_value(k, ticket[k]) for k in ticket.values}
        # TODO should we put a timezone mark on _time just in case, even though we say it'll be UTC?
        _time = datetime.datetime.utcnow()
        event.update({"_category": "created",
                      "_time": _time,
                      "_ticket": ticket.id,
                      "_author": ticket['reporter']})
        self._send_events([event])
            
    def ticket_changed(self, ticket, comment, author, old_values):
        _time = datetime.datetime.utcnow()
        if comment:
            comment_event = [{"_category": "comment",
                              "_time": _time,
                              "_ticket": ticket.id,
                              "_author": author,
                              "comment": comment}]
        else:
            comment_event = []
        # produces one event for each item in old_values, plus
        # possibly the comment
        self._send_events(chain(comment_event,
                                ({"_category": "changed",
                                  "_time": _time,
                                  "_ticket": ticket.id,
                                  "_author": author,
                                  k: self._transform_value(k, ticket[k])}
                                 for k, v in old_values.iteritems())))
    
    def ticket_deleted(self, ticket):
        _time = datetime.datetime.utcnow()        
        event = {"_category": "deleted",
                 "_time": _time,
                 "_ticket": ticket.id}
        self._send_events([event])

    def ticket_comment_modified(self, ticket, cdate, author, comment, old_comment):
        _time = datetime.datetime.utcnow()        
        event = {"_category": "changed",
                 "_time": _time,
                 "_ticket": ticket.id,
                 "_author": author,
                 "_cdate": cdate,                 
                 "comment": comment}
        self._send_events([event])

    def ticket_change_deleted(self, ticket, cdate, changes):
        # we don't support this, as the authors of this plugin don't
        # support deleting changes in our downstream product
        pass

    def attachment_added(self, attachment):
        _time = datetime.datetime.utcnow()
        if attachment.parent_realm != "ticket":
            return
        event = {"_category": "attachment-added",
                 "_time": _time,
                 "_ticket": attachment.parent_realm,
                 "_author": attachment.author,                 
                 "filename": attachment.filename,
                 "description": attachment.description,
                 "size": attachment.size}
        self._send_events([event])

    def attachment_deleted(self, attachment):
        _time = datetime.datetime.utcnow()
        if attachment.parent_realm != "ticket":
            return
        event = {"_category": "attachment-deleted",
                 "_time": _time,
                 "_ticket": attachment.parent_realm,
                 "_author": attachment.author,                 
                 "filename": attachment.filename}
        self._send_events([event])

    def attachment_version_deleted(self, attachment, old_version):
        """Called when a particular version of an attachment is deleted."""
        self.attachment_deleted(attachment)

    def attachment_reparented(self, attachment, old_parent_realm, old_parent_id):
        """Called when an attachment is reparented."""
        self.attachment_added(attachment)

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
        self.log.debug("Connecting to %s with queues %s", self.amqp, self.queue_names)
        with Connection(self.amqp) as conn:
            queues = [conn.SimpleQueue(queue_name, queue_opts={'durable': True})
                      for queue_name in self.queue_names]
            for event in events:
                event['_project'] = self.project_identifier or os.path.basename(self.env.path)
                for queue in queues:
                    self.log.debug("Putting event %s to queue %s", event, queue_name)
                    queue.put(event, serializer="yaml")
        
