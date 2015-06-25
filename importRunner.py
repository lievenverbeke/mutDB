from MutationDB import MDB

import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)


# fileName='/home/share/data/annotated_mafs/Sanger.PCAWG_train2.lite.snv_mnv.maf'
# fileName = '/home/share/data/annotated_mafs/small_lite_snv_mnv.txt'
tertFileName = '/home/share/data/annotated_mafs/Santa_Cruz_Pilot_2015_05_04.tert.maflite'
dbFolder='/home/share/data/annotated_mafs/'

mutationDB=MDB(dbFolder)
#mutationDB.findSamplesWithMutationInRegion('1',50000,100000)
#mutationDB.findMutationsForSample('f82d213f-bc99-5b1d-e040-11ac0c486880')
# mutationDB.clear()
mutationDB.importFile(tertFileName)
# mutationDB.printMutationDB()
# mutationDB.printSampleDB()

mutationDB.closeDatabases()
