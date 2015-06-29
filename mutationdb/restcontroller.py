__author__ = 'lverbeke'
from mutationdb.mdb import MDB
from mutationdb import responsecreator
import logging


class RESTController:
    _nInstances = 0

    def __init__(self, pathToReader):
        if RESTController._nInstances > 0:
            raise RuntimeError('you should only have one instance of RESTController')

        self._reader = MDB(pathToReader)
        RESTController._nInstances += 1

    def findMutationsInRegion(self, chromosome, fromPos, toPos, responseType):
        if chromosome is None:
            return responsecreator.createResponse('no chromosome provided', responseType)
        if fromPos is None:
            return responsecreator.createResponse('no fromPos provided', responseType)
        if toPos is None:
            return responsecreator.createResponse('no toPos provided', responseType)

        fromPos=int(fromPos)
        toPos=int(toPos)
        if fromPos > toPos:
            t = fromPos
            fromPos = toPos
            toPos = t

        logging.debug('finding mutations on chromosome {:s} between {:d} and {:d}'.format(chromosome, fromPos, toPos))
        result = self._reader.findMutationsInRegion(chromosome, fromPos, toPos)
        return responsecreator.createResponse(result, responseType)

    def findMutationsForGene(self, geneID, annotation, responseType):
        if not annotation in {'all', 'coding', 'promotor'}:
            return responsecreator.createResponse('invalid gene annotation: {:s}'.format(annotation), responseType)

        if annotation is None or annotation == '':
            annotation = 'all'

        logging.debug('finding {:s} mutations in {:s}'.format(annotation, geneID))
        if annotation == 'all':
            result = self._reader.findMutationsInAnnotatedAreaOfGene(geneID, 'coding') \
                     + self._reader.findMutationsInAnnotatedAreaOfGene(geneID, 'promotor')
        else:
            result = self._reader.findMutationsInAnnotatedAreaOfGene(geneID, annotation)

        return responsecreator.createResponse(result, responseType)
