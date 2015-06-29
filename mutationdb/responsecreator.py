__author__ = 'lverbeke'

from flask import Response
from mutationdb import responsetype
import json


def createResponse(items, responseType):
    if responseType == responsetype.PLAINTEXT:
        return plainTextResponse(items)
    elif responseType == responsetype.JSON:
        return jsonResponse(items)
    else:
        raise ValueError('invalid response type')


def jsonResponse(t):
    return json.dumps(t)


def plainTextResponse(t):
    if t is None:
        output = 'nothing to return'
    elif type(t) is list:
        output = '\n'.join(set(t))
    elif type(t) is set:
        output = '\n'.join(t)
    else:
        output = ''

    return Response(output, content_type='text/plain; charset=utf-8')
