from setuptools import setup

setup(
    name = 'TracFeedRabbitMQ',
    version = '0.0',
    author = 'Nick Piper',
    author_email = 'nick.piper@cgi.com',
    license = 'Modified BSD License',
    packages = ['tracfeedrabbitmq'],
    package_data={
        'tracfeedrabbitmq': [
            'templates/*.html',
            'htdocs/js/*.js',
        ]
    },
    install_requires = ['pyyaml', 'celery', 'requests'],
    entry_points = {
        'trac.plugins': [
            'tracfeedrabbitmq = tracfeedrabbitmq',
            #'tracfeedrabbitmq.listeners = tracfeedrabbitmq.listeners',
            'tracfeedrabbitmq.slack = tracfeedrabbitmq.slack',
            #'tracfeedrabbitmq.servicebus = tracfeedrabbitmq.servicebus',
        ],
    },
)
