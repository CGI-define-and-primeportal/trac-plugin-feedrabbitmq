from trac.core import Component, implements, ExtensionPoint
from trac.util.text import empty
from trac.config import Option, BoolOption, ListOption
from trac.ticket.api import ITicketChangeListener
from trac.attachment import IAttachmentChangeListener

import datetime
import os
import re
from itertools import chain

import pytz
import time
import proton

from api import ICeleryTask

# TODO support IMilestoneChangeListener
# TODO admin ui to configure project_identifier

class QueueFeeder(Component):
    tasks = ExtensionPoint(ICeleryTask)
    
    def send_events(self, events):
        for event in events:
            event['project'] = os.path.basename(self.env.path)
            for task in self.tasks:
                task.run(event)

class TicketListener(Component):
    implements(ITicketChangeListener,
               IAttachmentChangeListener)

    def ticket_created(self, ticket):
        _time = proton.timestamp(time.time() * 1000)
        event = {"category": "created",
                 "time": _time,
                 "ticket": ticket.id,
                 "author": ticket['reporter']}
        event['state'] = {k: self._transform_value(k, ticket[k]) for k in ticket.values}
        QueueFeeder(self.env).send_events([event])
            
    def ticket_changed(self, ticket, comment, author, old_values):
        _time = proton.timestamp(time.time() * 1000)
        event = {"category": "changed",
                 "time": _time,
                 "comment": comment,                 
                 "ticket": ticket.id,
                 "author": author}
        event['change'] = {k: self._transform_value(k, ticket[k]) for k in old_values}
        event['state'] = {k: self._transform_value(k, ticket[k]) for k in ticket.values}
        QueueFeeder(self.env).send_events([event])
    
    def ticket_deleted(self, ticket):
        _time = proton.timestamp(time.time() * 1000)
        event = {"category": "deleted",
                 "time": _time,
                 "ticket": ticket.id}
        QueueFeeder(self.env).send_events([event])

    def ticket_comment_modified(self, ticket, cdate, author, comment, old_comment):
        _time = proton.timestamp(time.time() * 1000)
        event = {"category": "changed",
                 "time": _time,
                 "ticket": ticket.id,
                 "author": author,
                 "cdate": cdate,
                 'change': {"comment": comment}}
        QueueFeeder(self.env).send_events([event])

    def ticket_change_deleted(self, ticket, cdate, changes):
        # we don't support this, as the authors of this plugin don't
        # support deleting changes in our downstream product
        pass

    def attachment_added(self, attachment):
        _time = proton.timestamp(time.time() * 1000)
        if attachment.parent_realm != "ticket":
            return
        event = {"category": "attachment-added",
                 "time": _time,
                 "ticket": attachment.parent_realm,
                 "author": attachment.author,                 
                 "filename": attachment.filename,
                 "description": attachment.description,
                 "size": attachment.size}
        QueueFeeder(self.env).send_events([event])

    def attachment_deleted(self, attachment):
        _time = time.time()
        if attachment.parent_realm != "ticket":
            return
        event = {"category": "attachment-deleted",
                 "time": _time,
                 "ticket": attachment.parent_realm,
                 "author": attachment.author,                 
                 "filename": attachment.filename}
        QueueFeeder(self.env).send_events([event])

    def attachment_version_deleted(self, attachment, old_version):
        """Called when a particular version of an attachment is deleted."""
        self.attachment_deleted(attachment)

    def attachment_reparented(self, attachment, old_parent_realm, old_parent_id):
        """Called when an attachment is reparented."""
        self.attachment_added(attachment)

    def _transform_value(self, field, value):
        if value is empty:
            return None
        if field in ("cc", "keywords"):
            # note, Trac uses '[;,\s]+' (see trac/ticket/model.py)
            # but CGI's fork doesn't include the whitespace
            return [x.strip() for x in re.split(r'[;,]+', value)]
        # TODO deal with integer, date, float fields (CGI extensions)
        # e.g., we have to convert value as string to value as float/integer, by looking
        # at the field datatype configuration
        # TODO ensure that 'changetime' is in UTC?
        if isinstance(value, datetime.datetime):
            # celery uses kombu, which uses pickle by default, which 
            # fails to unpickle trac.util.datefmt.FixedOffset
            #value = value.astimezone(pytz.utc)
            # but then also proton then fails to seralize for sending to Service Bus...
            # TODO http://stackoverflow.com/a/7852891 says this is a bad idea
            value = proton.timestamp(time.mktime(value.timetuple()) * 1000)
        return value
