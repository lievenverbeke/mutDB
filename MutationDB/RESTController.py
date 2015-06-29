__author__ = 'lverbeke'
from MutationDB import MDB
from MutationDB import ResponseCreator
import logging


class RESTController:
    _nInstances = 0

    def __init__(self, pathToReader):
        if RESTController._nInstances > 0:
            raise RuntimeError('you should only have one instance of RESTController')

        self._reader = MDB(pathToReader)
        RESTController._nInstances += 1

    def findMutationsInRegion(self, chromosome, fromPos, toPos, responseType):
        pass

    def findMutationsForGene(self, geneID, annotation, responseType):
        ResponseCreator.bla()

        if not annotation in {'all', 'coding', 'promotor'}:
            return ResponseCreator.createResponse('invalid gene annotation', responseType)

        if annotation is None or annotation == '':
            annotation = 'all'

        logging.debug('finding {:s} mutations in {:s}'.format(annotation, geneID))
        if annotation == 'all':
            result = self._reader.findMutationsInAnnotatedAreaOfGene(geneID, 'coding') \
                     + self._reader.findMutationsInAnnotatedAreaOfGene(geneID, 'promotor')
        else:
            result = self._reader.findMutationsInAnnotatedAreaOfGene(geneID, annotation)


        if result is not None:
            result = set(result)
            return ResponseCreator.createResponse('\n'.join(result), responseType)
        else:
            return ResponseCreator.createResponse('no results found', responseType)
