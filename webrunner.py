from flask import Flask
from flask import request
from mutationdb import RESTController
from mutationdb import responsetype
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)

app = Flask(__name__)
controller = RESTController('/home/share/data/annotated_mafs')


@app.route("/")
def hello():
    return "Hello World!"


@app.route('/mutations')
def findMutationsInRegions():
    chromosome = request.args.get('chrom')
    fromPos = request.args.get('from')
    toPos = request.args.get('to')
    return controller.findMutationsInRegion(chromosome, fromPos, toPos, getResponseType())


@app.route('/mutations/<annotation>/<geneID>')
def findMutationsForGene(geneID, annotation):
    return controller.findMutationsForGene(geneID, annotation, getResponseType())

def getResponseType():
    if request.args.get('type') is None:
        return responsetype.PLAINTEXT
    elif request.args.get('type')=='text':
        return responsetype.PLAINTEXT
    elif request.args.get('type')=='json':
        return responsetype.JSON
    elif request.args.get('type')=='image':
        return responsetype.IMAGE
    else:
        return responsetype.PLAINTEXT


if __name__ == "__main__":
    app.run()
