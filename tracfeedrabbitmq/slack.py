from celery import Celery
import requests
import json

from trac.core import Component, implements
from trac.config import Option
from trac.resource import Resource, get_resource_url

from api import ICeleryTask

# TODO, read the backend and broker from trac.ini
app = Celery('slack', backend='amqp', broker='amqp://')

@app.task(name='post-to-slack')
def post_message(webhook_url, icon_url, channel, resource_url, event):
    if event['_category'] == "changed":
        prefix = "Project {project} Ticket <{resource_url}|#{ticket}> changed by {author}".format(
            project=event['_project'],
            author=event['_author'],            
            resource_url=resource_url,
            ticket=event['_ticket'])
        parts = []
        for k, v in event.items():
            if k.startswith("_"):
                continue
            parts.append("{field} changed to {value}".format(field=k,
                                                             value=v))
        fields = []
        for k, v in event.items():
            if not k.startswith("_"):
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
                               project=event['_project'],
                               resource_url=resource_url,
                               ticket=event['_ticket']),
                           "author_name": event['_author'],
                           "title": "#define ticket change",
                           "title_link": resource_url,
                           "text": event["_comment"],
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
        resource_url = self.env.abs_href("ticket", event["_ticket"])
        return post_message.delay(self.webhook,
                                  self.icon,
                                  self.channel,
                                  resource_url,
                                  event)
