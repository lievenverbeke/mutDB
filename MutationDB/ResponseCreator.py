__author__ = 'lverbeke'

from flask import Response
from MutationDB.ResponseType import ResponseType


def createResponse(items, responseType):
    if responseType == ResponseType.PLAINTEXT:
        return plainTextResponse(items)

def plainTextResponse(t):
    return Response(t, content_type='text/plain; charset=utf-8')
