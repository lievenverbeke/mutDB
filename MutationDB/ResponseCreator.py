__author__ = 'lverbeke'

from flask import Response
from MutationDB import ResponseType


class ResponseCreator:
    def __init__(self):
        pass

    @staticmethod
    def bla():
        pass

    @staticmethod
    def createResponse(items, responseType):
        if responseType == ResponseType.PLAINTEXT:
            return ResponseCreator.plainTextResponse(items)

    @staticmethod
    def plainTextResponse(t):
        return Response(t, content_type='text/plain; charset=utf-8')
