import logging
import uuid
import json
import asyncio

import requests

import sockjs

from aiohttp import web, ClientSession
from stompest.protocol import StompParser, StompFrame
import powershift.endpoints as endpoints

import requests.packages.urllib3

# Enable logging an INFO level so can see requests.

logging.basicConfig(level=logging.INFO)

# Disable all the noisy logging that request module outputs, including
# complaints about self signed certificates, which is what REST API for
# OpenShift when used internally has.

logging.getLogger('requests').setLevel(logging.CRITICAL)
requests.packages.urllib3.disable_warnings()

# Routines for interrogating the OpenShift REST API to look up list of
# backend services.

def project_name():
    client = endpoints.Client()

    projects = client.oapi.v1.projects.get()

    # If REST API endpoint access is not enabled the list of projects
    # will be empty and things will fail. Where is not empty, we assume
    # that the current project is the first in the list.

    if not projects.items:
        logging.fatal('OpenShift REST API access not enabled.')

    return projects.items[0].metadata.name

def get_services(namespace=None):
    if namespace is None:
        namespace = project_name()

    client = endpoints.Client()

    return client.api.v1.namespaces(namespace=namespace).services.get().items

def get_routes(namespace=None):
    if namespace is None:
        namespace = project_name()

    client = endpoints.Client()

    return client.oapi.v1.namespaces(namespace=namespace).routes.get().items

def public_address(route):
    # Assume that public HTTP port is always port 80 and HTTPS port is
    # always port 443 and these aren't mapped to something else.

    host = route.spec.host
    path = route.spec.path or '/'
    if route.spec.tls:
        return 'https://%s%s' % (host, path)
    return 'http://%s%s' % (host, path)

def get_backends():
    # We find backends by looking for a 'type' label on either services
    # or routes. If a service we use its internal address. If a route
    # we use the external address. Ensure we eliminate where label has
    # been applied to both as transition to use of routes.

    services = get_services()

    backends = []

    names = set()

    for service in services:
        if service.metadata.labels:
            if 'type' in service.metadata.labels:
                if service.metadata.labels['type'] == 'parksmap-backend':
                    port = service.spec.ports[0].port
                    name = service.metadata.name
                    url = 'http://%s:%s/' % (name, port)
                    backends.append((name, url))
                    names.add(name)

    routes = get_routes()

    for route in routes:
        if route.metadata.labels:
            if 'type' in route.metadata.labels:
                if route.metadata.labels['type'] == 'parksmap-backend':
                    name = route.metadata.name
                    url = public_address(route)
                    if name not in names:
                        backends.append((name, url))
                        names.add(name)

    return backends

def get_backend_info(name, url):
    url = url + 'ws/info/'

    response = requests.get(url)

    if response.status_code != requests.codes.ok:
        return None

    return response.json()

# Background task that periodically polls the list of backend services.
# The main task is a normal async task, but it executes calls to the
# backends in separate threads using loop.run_in_executor().

backend_details = {}

async def poll_services():
    global backend_details

    loop = asyncio.get_event_loop()

    while True:
        details = {}

        # Get the list of services with our label.

        try:
            endpoints = await loop.run_in_executor(None, get_backends)
        except Exception:
            logging.exception('Could not query backends.')
            continue

        # Query details for each backend service. The end point is
        # combination of service name and port.
        #
        # XXX This is currently polling the backend service each time
        # for details. This will cause Python backend to be kept alive
        # and will not restart due to inactivity. Even if details don't
        # change we aren't currently updating user interface anyway as
        # we aren't looking for any differences in details, only if a
        # backed was added or removed.

        for name, url in endpoints:
            try:
                info = await loop.run_in_executor(None, get_backend_info, name, url)
            except Exception:
                pass
            else:
                # We will get None if lookup of details failed for service.

                if info is None:
                    continue

                # We need to fill in some defaults for values if the
                # service doesn't define them as the user interface
                # expects all fields to be populated.

                info.setdefault('center', {"latitude":"0.0","longitude":"0.0"})
                info.setdefault('zoom', 1)
                info.setdefault('maxZoom', 1)
                info.setdefault('type', 'cluster')
                info.setdefault('visible', 'true')
                info.setdefault('scope', 'all')

                # If the service details didn't include an 'id' fill
                # it in with the name of the service.

                if 'id' not in info:
                    info['id'] = name

                details[info['id']] = (name, url, info)

        # Work out what services were added or removed since the last time
        # we ran this. Send notifications to the user interface about
        # whether services were added or removed.

        added = set()
        removed = set(backend_details.keys())

        for name in details:
            if name in removed:
                removed.remove(name)
            if name not in backend_details:
                added.add(name)

        manager = sockjs.get_manager('clients', app)

        def broadcast(topic, info):
            for session in manager.sessions:
                if not session.expired:
                    if hasattr(session, 'subscriptions'):
                        if topic in session.subscriptions:
                            subscription = session.subscriptions[topic]

                            headers = {}
                            headers['subscription'] = subscription
                            headers['content-type'] = 'application/json'
                            headers['message-id'] = str(uuid.uuid1())

                            body = json.dumps(info).encode('UTF-8')

                            frame = StompFrame(command='MESSAGE',
                                    headers=headers, body=body)

                            session.send(bytes(frame).decode('UTF-8'))

        for key in removed:
            name, url, info = backend_details[key]
            broadcast('/topic/remove', info)

        for key in added:
            name, url, info = details[key]
            broadcast('/topic/add', info)

        # Update our global record of what services we know about.

        backend_details = details

        # Wait a while and then update list again.

        await asyncio.sleep(15.0)

# The aiohttp application.

app = web.Application()

# The websocket endpoint. SockJS is used for basic transport over the
# web socket and then Stomp messaging is used on top. The Stomp module
# only provides message framing, so we need to implemented the
# handshakes ourself which the JS Stomp client is expecting.

def socks_backend(msg, session):
    parser = StompParser('1.1')

    if msg.data:
        parser.add(msg.data.encode('UTF-8'))

    frame = parser.get()

    manager = sockjs.get_manager('clients', app)

    if msg.tp == sockjs.MSG_OPEN:
        pass

    elif msg.tp == sockjs.MSG_MESSAGE:
        if frame.command == 'CONNECT':
            headers = {}
            headers['session'] = session.id

            msg = StompFrame(command='CONNECTED', headers=headers)

            session.send(bytes(msg).decode('UTF-8'))

            session.subscriptions = {}

        elif frame.command == 'SUBSCRIBE':
            subscription = frame.headers['id']
            session.subscriptions[frame.headers['destination']] = subscription

        elif frame.command == 'UNSUBSCRIBE':
            del session.subscriptions[frame.headers['destination']]

    elif msg.tp == sockjs.MSG_CLOSE:
        pass

    elif msg.tp == sockjs.MSG_CLOSED:
        pass

sockjs.add_endpoint(app, socks_backend, name='clients', prefix='/socks-backends/')

# Our REST API endpoints which the web interface uses.

async def backends_list(request):
    details = [info for name, url, info in backend_details.values()]
    return web.json_response(details)

app.router.add_get('/ws/backends/list', backends_list)

async def data_all(request):
    service = request.rel_url.query['service']

    name, url, info = backend_details[service]
    url =  url + 'ws/data/all'

    # XXX Need to find a better way of doing this. It currently reads
    # the whole data set into memory before returning it. Need to work
    # out how can stream the response from backend direct into response
    # to the web interface.

    async with ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()

    data = json.loads(data.decode('UTF-8'))

    return web.json_response(data, status=response.status)

app.router.add_get('/ws/data/all', data_all)

async def data_within(request):
    service = request.rel_url.query['service']

    name, url, info = backend_details[service]
    url = url + 'ws/data/within'

    # XXX Need to find a better way of doing this. It currently reads
    # the whole data set into memory before returning it. Need to work
    # out how can stream the response from backend direct into response
    # to the web interface.

    async with ClientSession() as session:
        async with session.get(url, params=request.rel_url.query) as response:
            data = await response.read()

    data = json.loads(data.decode('UTF-8'))

    return web.json_response(data, status=response.status)

app.router.add_get('/ws/data/within', data_within)

async def healthz(request):
    return web.json_response('OK')

app.router.add_get('/ws/healthz', healthz)

async def index(request):
    return web.HTTPFound('/index.html')

app.router.add_get('/', index)

app.router.add_static('/', 'static')

# Main application startup.

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Start up our background task to poll for backend services.

    asyncio.ensure_future(poll_services(), loop=loop)

    # Run the aiohttpd server.

    web.run_app(app)
