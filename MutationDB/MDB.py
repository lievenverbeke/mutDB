import plyvel
import logging
import pickle


class MDB:
    def __init__(self, databasePath):
        self.databasePath = databasePath
        self.dbMS = None
        self.dbSM = None
        self.dbAnnotation = None
        self.openDatabases()

    def clearAnnotationDB(self):
        logging.info('clearing annotation database')
        if self.dbAnnotation is not None:
            self.dbAnnotation.close()
        plyvel.destroy_db(self.databasePath + '/mdbAnnotation')
        self.dbAnnotation = plyvel.DB(self.databasePath + '/mdbAnnotation', create_if_missing=True)

    def importCodingAreaOfGenes(self, fileName, idColumn=0, chromosomeColumn=1, startColumn=2, endColumn=3,
                                skipFirst=True):
        logging.info('reading coding annotation data from %s', fileName)
        index = 0

        with open(fileName) as iFile:
            if skipFirst:
                iFile.readline()
            for line in iFile:
                index += 1

                parts = line.rstrip('\n').split('\t')
                geneID = MDB.removeDotFromGeneName(parts[idColumn])
                chromosome = parts[chromosomeColumn]
                startOfGene = int(parts[startColumn])
                endOfGene = int(parts[endColumn])
                mutationsInThisGene = self.findMutationsInRegion(chromosome, startOfGene, endOfGene)

                if mutationsInThisGene is not None:
                    key = MDB.geneIDToCodingKey(geneID)
                    value = pickle.dumps(mutationsInThisGene)
                    self.dbAnnotation.put(key.encode('utf-8'), value)

                if index % 1000 == 0:
                    logging.info('processed line {:d}'.format(index))


    @staticmethod
    def removeDotFromGeneName(geneName):
        posOfDot = geneName.find('.')
        if posOfDot > 0:
            return geneName[0:posOfDot]
        else:
            return geneName

    def findMutationsForSample(self, sampleID):
        if self.dbSM is None:
            logging.info('sample database is not open')

        key = MDB.sampleToString(sampleID).encode('utf-8')
        value = self.dbSM.get(key)
        result = pickle.loads(value)
        return result

    def findMutationsInCodingAreaOfGene(self, geneID):
        if self.dbAnnotation is None:
            logging.info('annotation database is not open')

        key = MDB.geneIDToCodingKey(geneID).encode('utf-8')
        value = self.dbAnnotation.get(key)
        result = pickle.loads(value)
        return result

    def findSamplesWithMutation(self, chromosome, pos):
        if self.dbMS is None:
            logging.info('sample database is not open')

        posS = '{:d}'.format(pos)

        key = MDB.positionToString(chromosome, posS).encode('utf-8')
        value = self.dbMS.get(key)
        result = pickle.loads(value)
        return result

    def findSamplesWithMutationInRegion(self, chromosome, fromPos, toPos):
        if self.dbMS is None:
            logging.info('mutation database is not open')

        fromPosS = '{:d}'.format(fromPos - 1)
        toPosS = '{:d}'.format(toPos + 1)

        fromKey = MDB.positionToString(chromosome, fromPosS).encode('utf-8')
        toKey = MDB.positionToString(chromosome, toPosS).encode('utf-8')
        result = dict()
        for key, value in self.dbMS.iterator(start=fromKey, stop=toKey):
            result[key.decode('utf-8')] = pickle.loads(value)
        return result

    def findMutationsInRegion(self, chromosome, fromPos, toPos):
        if self.dbMS is None:
            logging.info('mutation database is not open')

        fromPosS = '{:d}'.format(fromPos - 1)
        toPosS = '{:d}'.format(toPos + 1)

        fromKey = MDB.positionToString(chromosome, fromPosS).encode('utf-8')
        toKey = MDB.positionToString(chromosome, toPosS).encode('utf-8')
        result = list(self.dbMS.iterator(start=fromKey, stop=toKey, include_value=False))
        return result

    def importMutationFile(self, fileName):
        logging.info('reading data from %s', fileName)
        index = 0
        samplesToAdd = dict()
        mutationsToAdd = dict()
        batchSize = 1000000

        with open(fileName) as iFile:
            for line in iFile:
                index += 1

                parts = line.rstrip('\n').split('\t')

                positionS = MDB.positionToString(parts[0], parts[1])
                sampleS = MDB.sampleToString(parts[5])

                if positionS not in samplesToAdd:
                    samplesToAdd[positionS] = list()
                samplesToAdd[positionS].append(sampleS)

                if sampleS not in mutationsToAdd:
                    mutationsToAdd[sampleS] = list()
                mutationsToAdd[sampleS].append(positionS)

                if index % batchSize == 0:
                    MDB.appendMapToKeyList(self.dbSM, mutationsToAdd)
                    MDB.appendMapToKeyList(self.dbMS, samplesToAdd)
                    mutationsToAdd.clear()
                    samplesToAdd.clear()
                    logging.info("{:<10d} lines processed, chromosome {:s}".format(index, parts[0]))

        MDB.appendMapToKeyList(self.dbSM, mutationsToAdd)
        MDB.appendMapToKeyList(self.dbMS, samplesToAdd)

    @staticmethod
    def appendMapToKeyList(db, mapToAppend):
        for key in mapToAppend:
            value = mapToAppend[key]
            MDB.appendListToKeyList(db, key, value)
        return

    @staticmethod
    def appendListToKeyList(db, key, listToAppend):
        key = key.encode('utf-8')
        value = db.get(key)
        if value is not None:
            listToAppend = listToAppend + pickle.loads(value)

        db.put(key, pickle.dumps(listToAppend))
        return

    @staticmethod
    def appendValueToKeyList(db, key, valueToAppend):
        key = key.encode('utf-8')
        value = db.get(key)
        if value is not None:
            listToAppendTo = pickle.loads(value)
        else:
            listToAppendTo = list()
        listToAppendTo.append(valueToAppend)
        db.put(key, pickle.dumps(listToAppendTo))
        return

    @staticmethod
    def geneIDToCodingKey(geneID):
        return 'coding:{:s}'.format(MDB.removeDotFromGeneName(geneID))

    @staticmethod
    def geneIDToPromotorKey(geneID):
        return 'promotor:{:s}'.format(MDB.removeDotFromGeneName(geneID))

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
        plyvel.destroy_db(self.databasePath + '/mdbAnnotation')
        self.openDatabases()

    def openDatabases(self):
        logging.info('opening databases in folder ' + self.databasePath)
        self.dbMS = plyvel.DB(self.databasePath + '/mdbSM', create_if_missing=True)
        self.dbSM = plyvel.DB(self.databasePath + '/mdbMS', create_if_missing=True)
        self.dbAnnotation = plyvel.DB(self.databasePath + '/mdbAnnotation', create_if_missing=True)

    def closeDatabases(self):
        logging.info('closing databases')
        if self.dbMS is not None:
            self.dbMS.close()
        if self.dbSM is not None:
            self.dbSM.close()
        if self.dbAnnotation is not None:
            self.dbAnnotation.close()

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
