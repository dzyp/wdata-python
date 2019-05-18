import argparse
import collections
import httplib
import json
import mimetypes
import sys
import time
import urllib
from urllib2 import HTTPError, Request, urlopen


AUTH_PATH = 'https://app.wdesk.com/iam/oauth2/v4.0/token'

PROTOCOL = 'https://'
WDATA_HOST = 'h.app.wdesk.com'
WDATA_API_PREFIX = '/s/wdata/prep/api/v1/'

FILE_IMPORT_PATH = PROTOCOL + WDATA_HOST + WDATA_API_PREFIX + 'table/%s/import'
FILE_POLL_PATH = PROTOCOL + WDATA_HOST + WDATA_API_PREFIX + 'file/%s'


class Args(collections.namedtuple('Args', ['table_id', 'client_id', 'client_secret', 'file_path'])):
    """Makes rst a bit easier to work with."""
    pass


class Authorization(collections.namedtuple('Authorization', ['access_token'])):
    def bearer_token(self):
        return 'Bearer ' + self.access_token


class File(collections.namedtuple('File', ['id', 'status'])):
    pass


def main(args):
    """Runs the application using the provided information to upload a file to wdata.

    :param Args args: user-provided arguments to the cli to upload a file
    """
    auth = _login(args.client_id, args.client_secret)
    print 'user authorized'

    file_id = _upload_file(auth, args.table_id, args.file_path)
    print 'file uploaded'

    _import_file(auth, args.table_id, file_id)
    print 'file import started'

    _block_on_import(auth, file_id)
    print 'file successfully imported'


def _make_request(request):
    """
    To help with errors

    :param Request request: request to make
    :return: the response body if successful else prints friendly error and exits
    :rtype: str
    """
    try:
        return urlopen(request).read()
    except HTTPError as e:
        print 'Error encountered making request: %s' % e.read()
        sys.exit(1)


def _login(client_id, client_secret):
    """
    Sends the client id and secret to wdesk auth and returns authentication.

    :param str client_id: id of the oauth cred
    :param str client_secret: secret of the oauth cred
    :return: returns the bearer token
    :rtype: Authorization
    """
    form_dct = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    form_data = urllib.urlencode(form_dct)
    req = Request(
        AUTH_PATH,
        data=form_data,
    )
    return json.loads(_make_request(req), object_hook=lambda b: Authorization(b['access_token']))


def _upload_file(auth, table_id, path):
    """
    Uploads a file to Wdata prep to the provided table id.

    :param Authorization auth: for authentication the request
    :param str table_id: id of the table to upload to
    :param str path: the local path of the file to upload
    :return: the id of the file uploaded
    :rtype: str
    """
    try:
        f = open(path)
    except IOError as e:
        print 'unable to open file %s: %s' % (path, str(e))
        sys.exit(1)

    content_type, body = encode_multipart_formdata(
        [('tableId', table_id)],
        [('file', f.name, f.read())]
    )

    h = httplib.HTTPSConnection(WDATA_HOST)
    h.request('POST', WDATA_API_PREFIX + 'file', body=body, headers={
        'Content-Type': content_type,
        'Authorization': auth.bearer_token()
    })

    response = h.getresponse()
    if response.status != 201:
        print 'Unable to upload file: %s' % response.read()
        sys.exit(1)

    return json.loads(response.read())['body']['id']


def _import_file(auth, table_id, file_id):
    """
    Imports the provided file id and blocks until upload is complete.

    :param Authorization auth: for utilizing a bearer token
    :param str file_id: the id of the file to import
    :return: nothing or prints a friendly error message if an error is encountered
    """
    req = Request(FILE_IMPORT_PATH % table_id)
    req.add_header('Content-Type', 'application/json')
    req.add_header('Authorization', auth.bearer_token())
    req.add_data(json.dumps({'fileId': file_id}))

    _make_request(req)


def _block_on_import(auth, file_id):
    """
    Blocks while polling on the file import. If the file isn't done importing in 3 minutes this times out and exits.

    :param Authorization auth: for utilizing a bearer token
    :param file_id: the id of the file being imported
    :return: nothing but prints and exits on an error
    """
    for _ in xrange(90):
        time.sleep(2)  # wait 2 seconds on every iteration

        req = Request(FILE_POLL_PATH % file_id)
        req.add_header('Authorization', auth.bearer_token())

        res = json.loads(_make_request(req))
        f = File(res['body']['id'], res['body']['status'])
        if f.status.upper() == 'IMPORTING':  # continue to next iteration
            continue

        if f.status.upper() == 'IMPORTED':  # we are done here, nothing else to do
            return

        # any other case is a problem
        print 'error importing file: %s' % res
        sys.exit(1)

    print 'timed out importing file'
    sys.exit(1)


def encode_multipart_formdata(fields, files):
    """
    Encodes data for a multipart request. I could make this much easier using requests but this ensures we only use
    the std lib.

    :param list of (str, str) fields: fields to add to the form
    :param list of (str, str, str) files: files to add to the form
    :return: a tuple of content type and body
    :rtype: (str, str)
    """
    LIMIT = '----------lImIt_of_THE_fIle_eW_$'
    CRLF = '\r\n'
    L = []
    for (key, value) in fields:
        L.append('--' + LIMIT)
        L.append('Content-Disposition: form-data; name="%s"' % key)
        L.append('')
        L.append(value)

    for (key, filename, value) in files:
        L.append('--' + LIMIT)
        L.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
        L.append('Content-Type: %s' % get_content_type(filename))
        L.append('')
        L.append(value)
    L.append('--' + LIMIT + '--')
    L.append('')
    body = CRLF.join(L)
    content_type = 'multipart/form-data; boundary=%s' % LIMIT
    return content_type, body


def get_content_type(filename):
    """
    Uses mimetype's guess functionality to take a shot at guessing the provided filename's mimetype.

    :param str filename: name of the file, should include extension
    :return: the guessed value or `application/octet-stream` by default
    :rtype: str
    """
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'


def _parse_args():
    """
    Parses args from the command line and returns an args object.

    :return: parsed args
    :rtype: Args
    """
    parser = argparse.ArgumentParser(description='Upload a file to Wdata.')
    parser.add_argument('table_id', help='id of the table to upload a file to')
    parser.add_argument('client_id', help='the id of the oauth user')
    parser.add_argument('client_secret', help='oauth secret')
    parser.add_argument('file_path', help='path of the file to upload, file should include extension and should be CSV')

    return parser.parse_args()


if __name__ == '__main__':
    main(_parse_args())
