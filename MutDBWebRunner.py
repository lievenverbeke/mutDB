from flask import Flask
from flask import Response
from MutationDB import MDB
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)

app = Flask(__name__)
reader = MDB('/home/share/data/annotated_mafs')


@app.route("/")
def hello():
    return "Hello World!"

@app.route('/mutations/<annotation>/<geneID>')
def findMutationsForGene(geneID, annotation):
    if not annotation in {'all', 'coding', 'promotor'}:
        return plainTextResponse('invalid gene annotation')

    if annotation is None or annotation == '':
        annotation = 'all'

    logging.debug('finding {:s} mutations in {:s}'.format(annotation, geneID))
    if annotation == 'all':
        result = reader.findMutationsInAnnotatedAreaOfGene(geneID, 'coding') \
                 + reader.findMutationsInAnnotatedAreaOfGene(geneID, 'promotor')
    else:
        result = reader.findMutationsInAnnotatedAreaOfGene(geneID, annotation)

    result = set(result)

    if result is not None:
        return plainTextResponse('\n'.join(result))
    else:
        return plainTextResponse('no results found')


def plainTextResponse(t):
    return Response(t, content_type='text/plain; charset=utf-8')


if __name__ == "__main__":
    app.run()
