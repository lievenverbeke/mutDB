from MutationDB import MDB

import logging
import pickle

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)


# fileName='/home/share/data/annotated_mafs/Sanger.PCAWG_train2.lite.snv_mnv.maf'
fileName = '/home/share/data/annotated_mafs/small_lite_snv_mnv.txt'
dbFolder='/home/share/data/annotated_mafs/'

mutationDB=MDB(dbFolder)
mutationDB.clear()
mutationDB.importFile(fileName)
mutationDB.printMutationDB()

mutationDB.closeDatabases()
