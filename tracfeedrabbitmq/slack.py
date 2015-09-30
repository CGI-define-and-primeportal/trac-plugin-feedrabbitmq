from celery import Celery
import requests
import json

from trac.core import Component, implements
from trac.config import Option
from trac.resource import Resource, get_resource_url

from api import ICeleryTask

# TODO, read the backend and broker from trac.ini
# TODO reorganise somehow so it's straight forward have our different tasks go into different queues
# TODO should the worker be loading just one app, for all our tasks?

app = Celery('slack', backend='amqp', broker='amqp://')

@app.task(name='post-to-slack')
def post_message(webhook_url, icon_url, channel, resource_url, event):
    if event['category'] == "changed":
        prefix = "Project {project} Ticket <{resource_url}|#{ticket}> changed by {author}".format(
            project=event['project'],
            author=event['author'],            
            resource_url=resource_url,
            ticket=event['ticket'])
        parts = []
        for k, v in event['change'].items():
            parts.append("{field} changed to {value}".format(field=k,
                                                             value=v))
        fields = []
        for k, v in event['change'].items():
            fields.append({
                    "title": k,
                    "value": v,
                    "short": len(v) < 10
                    })
            
        payload = {"channel": channel,
                   "username": "#define",
                   "text": prefix,
                   "icon_url": icon_url,
                   "attachments": [
                       {
                           "fallback": "%s: %s" % (prefix, ", ".join(parts)),
                           "color": "#36a64f",
                           "pretext": "Project {project} Ticket <{resource_url}|#{ticket}>".format(
                               project=event['project'],
                               resource_url=resource_url,
                               ticket=event['ticket']),
                           "author_name": event['author'],
                           "title": "#define ticket change",
                           "title_link": resource_url,
                           "text": event["comment"],
                           "fields": fields
                       }]
                   }
        #print payload
        r = requests.post(webhook_url, data=json.dumps(payload))
        #print r.text
        return r

class SlackEmitter(Component):
    implements(ICeleryTask)

    webhook = Option('slack', 'webhook')
    channel = Option('slack', 'channel')
    icon = Option('slack', 'icon')
    
    def run(self, event):
        resource_url = self.env.abs_href("ticket", event["ticket"])
        return post_message.delay(self.webhook,
                                  self.icon,
                                  self.channel,
                                  resource_url,
                                  event)
