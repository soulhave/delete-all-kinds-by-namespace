import endpoints

from protorpc import messages
from protorpc import remote
from google.appengine.api.taskqueue import Queue, Task
import logging
from google.appengine.ext import db, ndb
from google.appengine.api import namespace_manager
from google.appengine.ext.db import metadata
from google.appengine.datastore.datastore_query import Cursor


import webapp2

class DefaultResponse(messages.Message):
    """Greeting that stores a message."""
    message = messages.StringField(1)


class DefaultRequest(messages.Message):
    """Greeting that stores a message."""
    namespace = messages.StringField(1)


@endpoints.api(
    name='deleteKind',
    allowed_client_ids=[endpoints.API_EXPLORER_CLIENT_ID],
    version='v1')
class DatastoreManagerAPI(remote.Service):

    @endpoints.method(
        DefaultRequest,
        DefaultResponse,
        path='delete.kind',
        http_method='POST',
        name='post')
    def delete_kind(self, request):
        namespace = request.namespace

        send_message_to_queye(namespace)

        return DefaultResponse(message=namespace)


api = endpoints.api_server([DatastoreManagerAPI])


class DeleteKinds(webapp2.RequestHandler):
    """Delete kinds by namespace."""

    def post(self):
        required_header = 'X-Appengine-Queuename'
        if required_header not in self.request.headers:
            self.abort(403)

        namespace = self.request.get('namespace', None)
        kind = self.request.get('kind', None)

        if namespace and kind:

            cursor = Cursor(urlsafe=self.request.get('cursor', None))
            keys, cursor_next, more = ndb.Query(kind=kind).fetch_page(10000, keys_only=True, start_cursor=cursor)

            if more:
                send_message_to_queye(namespace, kind, cursor_next.urlsafe())
            else:
                logging.info("[Namespace %s][Kind %s] No more registers.", namespace, kind)

            ndb.delete_multi_async(keys)
            logging.info("[Namespace %s][Kind %s]Deleted keys --> %s", namespace, kind, keys)

        elif namespace:
            namespace_manager.set_namespace(namespace)

            for kind in metadata.get_kinds():
                if "__" not in kind:
                    send_message_to_queye(namespace, kind)
                    logging.info("Add kind %s on the queue...", kind)

        if namespace:
            logging.info("Deleted tables from %s...", namespace)


def send_message_to_queye(namespace=None, kind=None, cursor=None):
    queue_name = "delete-kinds-by-namespace"
    queue_url = "/task/delete_kinds"
    queue = Queue(name=queue_name)

    if not namespace:
        raise endpoints.BadRequestException("Please fill with namespace")

    params = {'namespace': namespace}

    if kind:
        params['kind'] = kind

    if cursor:
        params['cursor'] = cursor

    queue.add(Task(url=queue_url, params=params))

APP = webapp2.WSGIApplication([('/task/delete_kinds', DeleteKinds)], debug=True)

