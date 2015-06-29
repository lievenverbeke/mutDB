from flask import Flask
from MutationDB import RESTController
from MutationDB import ResponseType
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)

app = Flask(__name__)
controller = RESTController('/home/share/data/annotated_mafs')


@app.route("/")
def hello():
    return "Hello World!"

@app.route('/mutations/<annotation>/<geneID>')
def findMutationsForGene(geneID, annotation):
    return controller.findMutationsForGene(geneID,annotation,ResponseType.PLAINTEXT)


if __name__ == "__main__":
    app.run()
