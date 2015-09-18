from trac.core import Component, implements, ExtensionPoint
from trac.config import Option, BoolOption, ListOption
from trac.ticket.api import ITicketChangeListener
from trac.attachment import IAttachmentChangeListener

import datetime
import os
import re
from itertools import chain

from api import ICeleryTask

# TODO support IMilestoneChangeListener
# TODO admin ui to configure project_identifier

class QueueFeeder(Component):
    project_identifier = Option("celery", "project_identifer")
    tasks = ExtensionPoint(ICeleryTask)
    
    def send_events(self, events):
        for event in events:
            event['_environment'] = os.path.basename(self.env.path)
            event['_project'] = self.project_identifier or event['_environment']
            for task in self.tasks:
                task.run(event)

class TicketListener(Component):
    implements(ITicketChangeListener,
               IAttachmentChangeListener)

    def ticket_created(self, ticket):
        event = {k: self._transform_value(k, ticket[k]) for k in ticket.values}
        # TODO should we put a timezone mark on _time just in case, even though we say it'll be UTC?
        _time = datetime.datetime.utcnow()
        event.update({"_category": "created",
                      "_time": _time,
                      "_ticket": ticket.id,
                      "_author": ticket['reporter']})
        QueueFeeder(self.env).send_events([event])
            
    def ticket_changed(self, ticket, comment, author, old_values):
        _time = datetime.datetime.utcnow()
        event = {"_category": "changed",
                 "_time": _time,
                 "_comment": comment,                 
                 "_ticket": ticket.id,
                 "_author": author}
        for k, v in old_values.items():
            event[k] = self._transform_value(k, ticket[k])
        QueueFeeder(self.env).send_events([event])
    
    def ticket_deleted(self, ticket):
        _time = datetime.datetime.utcnow()        
        event = {"_category": "deleted",
                 "_time": _time,
                 "_ticket": ticket.id}
        QueueFeeder(self.env).send_events([event])

    def ticket_comment_modified(self, ticket, cdate, author, comment, old_comment):
        _time = datetime.datetime.utcnow()        
        event = {"_category": "changed",
                 "_time": _time,
                 "_ticket": ticket.id,
                 "_author": author,
                 "_cdate": cdate,                 
                 "comment": comment}
        QueueFeeder(self.env).send_events([event])

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
        QueueFeeder(self.env).send_events([event])

    def attachment_deleted(self, attachment):
        _time = datetime.datetime.utcnow()
        if attachment.parent_realm != "ticket":
            return
        event = {"_category": "attachment-deleted",
                 "_time": _time,
                 "_ticket": attachment.parent_realm,
                 "_author": attachment.author,                 
                 "filename": attachment.filename}
        QueueFeeder(self.env).send_events([event])

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
