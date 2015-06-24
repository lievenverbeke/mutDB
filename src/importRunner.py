import importData
import logging
import plyvel
import pickle

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)


# fileName='/home/share/data/annotated_mafs/Sanger.PCAWG_train2.lite.snv_mnv.maf'
fileName = '/home/share/data/annotated_mafs/small_lite_snv_mnv.txt'

plyvel.destroy_db('/home/share/data/annotated_mafs/mdbSM')
plyvel.destroy_db('/home/share/data/annotated_mafs/mdbMS')
dbMS = plyvel.DB('/home/share/data/annotated_mafs/mdbMS', create_if_missing=True)
dbSM = plyvel.DB('/home/share/data/annotated_mafs/mdbSM', create_if_missing=True)

importData.importDataFromFile(fileName, dbMS)

for key, value in dbMS:
    keyS=key.decode("utf-8")
    valueS=' '.join(pickle.loads(value))
    logging.debug('ḱey = {:s}, value = {:s}'.format(keyS,valueS))

dbMS.close()
dbSM.close()
