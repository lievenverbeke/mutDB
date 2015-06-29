__author__ = 'lverbeke'

from flask import Response
from mutationdb import responsetype


def createResponse(items, responseType):
    if responseType == responsetype.PLAINTEXT:
        return plainTextResponse(items)

def plainTextResponse(t):
    return Response(t, content_type='text/plain; charset=utf-8')
