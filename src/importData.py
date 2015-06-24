import logging
import pickle


def importDataFromFile(fileName, db):
    logging.info('reading data from %s', fileName)
    index = 0
    samplesToAdd = list()
    previousKey = ''

    with open(fileName) as iFile:
        for line in iFile:
            index += 1

            parts = line.rstrip('\n').split('\t')

            key = positionToString(parts[0], parts[1])
            value = sampleToString(parts[5])

            if index > 1 and previousKey != key:
                addListToKey(db, previousKey, samplesToAdd)
                samplesToAdd.clear()

            samplesToAdd.append(value)
            previousKey = key

            if index % 10000 == 0:
                logging.info("{:<10d} lines processed, chromosome {:s}".format(index, parts[0]))

    addListToKey(db, previousKey, samplesToAdd)
    return


def addListToKey(db, key, listToAdd):
    key=key.encode('utf-8')
    value = db.get(key)
    if value is not None:
        listToAdd = listToAdd + pickle.loads(value)

    db.put(key, pickle.dumps(listToAdd))
    return


def sampleToString(sampleID):
    return 'sampleID:{:s}'.format(sampleID)


def positionToString(chromosome, position):
    key = ('chrom:{:s}:pos:{:0>8s}'.format(chromosome, position))
    return key
