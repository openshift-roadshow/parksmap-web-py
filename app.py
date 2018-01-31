import logging
import uuid
import json
import asyncio
import os
import signal

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

async def project_name():
    # We still want to validate that the REST API access is also enabled.

    client = endpoints.AsyncClient()

    projects = await client.oapi.v1.projects.get()

    # If REST API access is not enabled the list of projects will be empty
    # as we should at least see our own project.

    if not projects.items:
        logging.fatal('OpenShift REST API access not enabled. To enable '
                'access, run the command "oc adm policy add-role-to-group '
                'view -k default')

    # We also need to check though that our project is in the list which is
    # returned because wrong permissions on other projects in the cluster
    # could expose them to us even if REST API access is enabled.

    with open('/run/secrets/kubernetes.io/serviceaccount/namespace') as fp:
        name = fp.read()

    for project in projects.items:
        if project.metadata.name == name:
            return name

    logging.fatal('OpenShift REST API access not enabled. To enable '
            'access, run the command "oc adm policy add-role-to-group '
            'view -k default')

    return None

async def get_services(namespace=None):
    if namespace is None:
        namespace = await project_name()

    if namespace is None:
        return []

    client = endpoints.AsyncClient()

    services = await client.api.v1.namespaces(namespace=namespace).services.get()

    return services.items

async def get_routes(namespace=None):
    if namespace is None:
        namespace = await project_name()

    if namespace is None:
        return []

    client = endpoints.AsyncClient()

    routes = await client.oapi.v1.namespaces(namespace=namespace).routes.get()
    
    return routes.items

async def get_pods(namespace=None):
    if namespace is None:
        namespace = await project_name()

    client = endpoints.AsyncClient()

    pods = await client.api.v1.namespaces(namespace=namespace).pods.get()
    
    return pods.items

async def get_pods_for_service(service, namespace=None):
    # Determine the list of pods which are associated with a service.

    selector = service.spec.selector

    pods = await get_pods(namespace)

    matches = []

    for pod in pods:
        if pod.metadata.labels:
            match = True

            for key in selector:
                if (key not in pod.metadata.labels or
                        pod.metadata.labels[key] != selector[key]):
                    match = False
                    break

            if match:
                matches.append(pod)

    return matches

async def get_service(name, namespace=None):
    services = await get_services(namespace)

    for service in services:
        if service.metadata.name == name:
            return service

async def get_services_for_route(route, namespace=None):
    services = []

    primary = route.spec.to

    async def verify_has_pods(backend):
        if backend.kind == 'Service' and backend.weight != 0:
            service = await get_service(backend.name, namespace)
            if service:
                if await get_pods_for_service(service, namespace):
                    services.append(service)

    await verify_has_pods(primary)

    if 'alternate_backends' in route.spec:
        for backend in route.spec.alternate_backends:
            await verify_has_pods(backend)

    return services

def public_address(route):
    # Assume that public HTTP port is always port 80 and HTTPS port is
    # always port 443 and these aren't mapped to something else.

    host = route.spec.host
    path = route.spec.path or '/'
    if route.spec.tls:
        return 'https://%s%s' % (host, path)
    return 'http://%s%s' % (host, path)

async def get_backends(namespace=None):
    # We find backends by looking for a 'type' label on either services
    # or routes. Only return backends that currently have active pods.
    # If a service we use its internal service address. If a route we
    # use the external address. Ensure we eliminate where label has been
    # applied to both as transition to use of routes.

    services = await get_services(namespace)

    backends = []

    names = set()

    for service in services:
        if service.metadata.labels:
            if 'type' in service.metadata.labels:
                if service.metadata.labels['type'] == 'parksmap-backend':
                    if await get_pods_for_service(service, namespace):
                        port = service.spec.ports[0].port
                        name = service.metadata.name
                        url = 'http://%s:%s/' % (name, port)
                        backends.append((name, url))
                        names.add(name)

    routes = await get_routes(namespace)

    for route in routes:
        if route.metadata.labels:
            if 'type' in route.metadata.labels:
                if route.metadata.labels['type'] == 'parksmap-backend':
                    if await get_services_for_route(route, namespace):
                        name = route.metadata.name
                        url = public_address(route)
                        if name not in names:
                            backends.append((name, url))
                            names.add(name)

    return backends

async def get_backend_info(url):
    url = url + 'ws/info/'

    async with ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()

    if response.status == 200:
        info = json.loads(data.decode('UTF-8'))

        # We need to fill in some defaults for values if the
        # service doesn't define them as the user interface
        # expects all fields to be populated.

        info.setdefault('center', {"latitude":"0.0","longitude":"0.0"})
        info.setdefault('zoom', 1)
        info.setdefault('maxZoom', 1)
        info.setdefault('type', 'cluster')
        info.setdefault('visible', 'true')
        info.setdefault('scope', 'all')

        return info

# Background task that periodically polls the list of backend services.
# The main task is a normal async task, but it executes calls to the
# backends in separate threads using loop.run_in_executor().

backend_details = {}

def broadcast_message(topic, info):
    manager = sockjs.get_manager('clients', app)

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

async def poll_backends():
    global backend_details

    loop = asyncio.get_event_loop()

    while True:
        details = {}

        # Get the list of services with our label.

        try:
            default_backend = os.environ.get('PARKSMAP_BACKEND')

            if default_backend:
                endpoints = [(default_backend, 'http://%s:8080/' % default_backend)]
            else:
                endpoints = await get_backends()

        except Exception:
            logging.exception('Could not query backends.')

            # Wait a while and then update list again.

            await asyncio.sleep(15.0)

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
                info = await get_backend_info(url)
            except Exception as e:
                pass
            else:
                # We will get None if lookup of details failed for service.

                if info is None:
                    continue

                # Ignore the backend if it doesn't provide an id field.

                if 'id' not in info:
                    continue

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

        for key in removed:
            name, url, info = backend_details[key]
            broadcast_message('/topic/remove', info)

        for key in added:
            name, url, info = details[key]
            broadcast_message('/topic/add', info)

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

    @asyncio.coroutine 
    def shutdown_application():
        logging.info('Application stopped')
        loop.stop()

    def schedule_shutdown():                          
        logging.info('Stopping application')
        for task in asyncio.Task.all_tasks():
            task.cancel()                    
        asyncio.ensure_future(shutdown_application()) 

    loop.add_signal_handler(signal.SIGTERM, schedule_shutdown)

    # Start up our background task to poll for backend services.

    asyncio.ensure_future(poll_backends(), loop=loop)

    # Run the aiohttpd server.

    web.run_app(app)
