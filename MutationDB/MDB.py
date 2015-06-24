import plyvel
import logging
import pickle


class MDB:
    def __init__(self, databasePath):
        self.databasePath = databasePath
        self.dbMS = None
        self.dbSM = None
        self.openDatabases()

    def importFile(self, fileName):
        logging.info('reading data from %s', fileName)
        index = 0
        samplesToAdd = list()
        previousKey = ''

        with open(fileName) as iFile:
            for line in iFile:
                index += 1

                parts = line.rstrip('\n').split('\t')

                key = MDB.positionToString(parts[0], parts[1])
                value = MDB.sampleToString(parts[5])

                if index > 1 and previousKey != key:
                    MDB.addListToKey(self.dbMS, previousKey, samplesToAdd)
                    samplesToAdd.clear()

                samplesToAdd.append(value)
                previousKey = key

                if index % 10000 == 0:
                    logging.info("{:<10d} lines processed, chromosome {:s}".format(index, parts[0]))

        MDB.addListToKey(self.dbMS, previousKey, samplesToAdd)
        return

    @staticmethod
    def addListToKey(db, key, listToAdd):
        key = key.encode('utf-8')
        value = db.get(key)
        if value is not None:
            listToAdd = listToAdd + pickle.loads(value)

        db.put(key, pickle.dumps(listToAdd))
        return

    @staticmethod
    def sampleToString(sampleID):
        return 'sampleID:{:s}'.format(sampleID)

    @staticmethod
    def positionToString(chromosome, position):
        key = ('chrom:{:s}:pos:{:0>8s}'.format(chromosome, position))
        return key

    def clear(self):
        logging.info('clearing all data')
        self.closeDatabases()
        plyvel.destroy_db(self.databasePath + '/mdbSM')
        plyvel.destroy_db(self.databasePath + '/mdbMS')
        self.openDatabases()

    def openDatabases(self):
        logging.info('opening databases in folder '+ self.databasePath)
        self.dbMS = plyvel.DB(self.databasePath + '/mdbSM', create_if_missing=True)
        self.dbSM = plyvel.DB(self.databasePath + '/mdbMS', create_if_missing=True)

    def closeDatabases(self):
        logging.info('closing databases')
        if self.dbMS is not None:
            self.dbMS.close()
        if self.dbSM is not None:
            self.dbSM.close()

    def printMutationDB(self):
        for key, value in self.dbMS:
            keyS = key.decode("utf-8")
            valueS = ' '.join(pickle.loads(value))
            print('ḱey = {:s}, value = {:s}'.format(keyS, valueS))

    def printSampleDB(self):
        for key, value in self.dbSM:
            keyS = key.decode("utf-8")
            valueS = ' '.join(pickle.loads(value))
            print('ḱey = {:s}, value = {:s}'.format(keyS, valueS))