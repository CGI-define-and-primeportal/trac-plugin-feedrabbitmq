from celery import Celery
import requests
import json
import pkg_resources

from trac.core import Component, implements
from trac.config import Option
from trac.resource import Resource, get_resource_url
from trac.admin import IAdminPanelProvider
from trac.web.chrome import ITemplateProvider, add_notice

from api import ICeleryTask

# TODO, read the backend and broker from trac.ini
# TODO reorganise somehow so it's straight forward have our different tasks go into different queues
# TODO should the worker be loading just one app, for all our tasks?

app = Celery('slack', backend='amqp', broker='amqp://')

@app.task(name='post-to-slack')
def post_message(webhook_url, icon_url, resource_url, event):
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
            
        payload = {"username": "#define",
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

# TODO, port this to IAsyncTicketChangeListener from ICeleryTask
class SlackEmitter(Component):
    implements(ICeleryTask, IAdminPanelProvider, ITemplateProvider)

    webhook = Option('slack', 'webhook')
    icon = Option('slack', 'icon')

    # ICeleryTask
    def run(self, event):
        if self.webhook:
            resource_url = self.env.abs_href("ticket", event["ticket"])
            return post_message.delay(self.webhook,
                                      self.icon,
                                      resource_url,
                                      event)

    # IAdminPanelProvider
    def get_admin_panels(self, req):
        if req.perm.has_permission('TRAC_ADMIN'):
            yield ('integrations', 'Integrations', 'slack', 'Slack')

    def render_admin_panel(self, req, cat, page, path_info):
        if req.method == 'POST':
            self.config.set('slack', 'webhook', req.args.get('webhook'))
            self.config.save()
            add_notice(req, "Saved Slack integration settings")
            req.redirect(req.href.admin(cat, page))
                
        return 'slack_admin.html', {'webhook': self.webhook}

    # ITemplateProvider
    def get_htdocs_dirs(self):
        yield 'slack', pkg_resources.resource_filename(__name__,
                                                       'htdocs')

    def get_templates_dirs(self):
        yield pkg_resources.resource_filename(__name__, 'templates')

