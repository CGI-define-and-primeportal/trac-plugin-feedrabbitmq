from trac.env import open_environment
from trac.core import Component, ExtensionPoint, implements
from trac.ticket.api import ITicketChangeListener
from trac.ticket.model import Ticket
from trac.util.text import empty

from celery import Celery

from api import IAsyncTicketChangeListener

app = Celery('trac', backend='amqp', broker='amqp://')

@app.task
def worker(environmentpath, interface, function, args):
    env = open_environment(environmentpath, use_cache=True)
    ASyncBridgeReceive(env).receive(interface, function, args)

class ASyncBridgeReceive(Component):
    ticket_listeners = ExtensionPoint(IAsyncTicketChangeListener)

    def receive(self, interface, function, inbound_args):
        self.log.debug("Handling %s %s %s", interface, function, inbound_args)
        if interface == 'IAsyncTicketChangeListener':
            if function == 'ticket_created':
                ticket = Ticket(self.env, inbound_args)
                for listener in self.ticket_listeners:
                    listener.ticket_created(ticket)
            elif function == 'ticket_changed':
                ticket = Ticket(self.env, inbound_args[0])
                comment, author, serializable_old_values = inbound_args[1:4]
                old_values = {}
                for k, v in serializable_old_values.items():
                    if v == "__trac.util.text.empty__":
                        serializable_old_values[k] = empty
                    else:
                        serializable_old_values[k] = v
                for listener in self.ticket_listeners:
                    listener.ticket_changed(ticket, comment, author, old_values)
            elif function == 'ticket_deleted':
                ticket = Ticket(self.env, inbound_args)
                for listener in self.ticket_listeners:
                    listener.ticket_deleted(ticket)
            else:
                self.log.warning("Unknown function %s:%s", interface, function)
        else:
            self.log.warning("Unknown interface %s", interface)

class ASyncBridgeSend(Component):
    implements(ITicketChangeListener)
    ticket_listeners = ExtensionPoint(IAsyncTicketChangeListener)

    def ticket_created(self, ticket):
        return worker.delay(self.env.path,
                            'IAsyncTicketChangeListener', 'ticket_created', 
                            ticket.resource.id)

    def ticket_changed(self, ticket, comment, author, old_values):
        serializable_old_values = {}
        for k, v in old_values.items():
            #print k, v, type(v)
            if v == empty:
                serializable_old_values[k] = "__trac.util.text.Empty__"
            else:
                serializable_old_values[k] = v
        return worker.delay(self.env.path,
                            'IAsyncTicketChangeListener', 'ticket_changed', 
                            (ticket.resource.id, comment, author, old_values))

    def ticket_deleted(self, ticket):
        return worker.delay(self.env.path,
                            'IAsyncTicketChangeListener', 'ticket_deleted', 
                            ticket.resource.id)
