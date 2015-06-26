from MutationDB import MDB
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)

# fileName = '/home/share/data/annotated_mafs/small_lite_snv_mnv.txt'

# mutationFileName='/home/share/data/annotated_mafs/Sanger.PCAWG_train2.lite.snv_mnv.maf'
# tertFileName = '/home/share/data/annotated_mafs/Santa_Cruz_Pilot_2015_05_04.tert.maflite'

codingAnnotationFileName='/home/share/data/IDConversion.txt'
promotorAnnotationFileName='/home/share/data/annotated_mafs/expanded_promotors.txt'


dbFolder='/home/share/data/annotated_mafs/'

mutationDB=MDB(dbFolder)

# mutationDB.importMutationFile(mutationFileName)
# mutationDB.importMutationFile(tertFileName)
# mutationDB.printMutationDB()
# mutationDB.printSampleDB()
# mutationDB.clearAnnotationDB()
# mutationDB.importAnnotatedRegionOfGenes(codingAnnotationFileName,'coding',idColumn=0,chromosomeColumn=1,startColumn=2,endColumn=3,skipFirst=True)
# mutationDB.importAnnotatedRegionOfGenes(promotorAnnotationFileName,'promotor',idColumn=4,chromosomeColumn=0,startColumn=1,endColumn=2,skipFirst=True)

# print(mutationDB.findSamplesWithMutationInRegion('5',1295000,1296000))
# print(mutationDB.findSamplesWithMutation('5',1295228))
# print(mutationDB.findMutationsForSample('add0b0f6-b4a8-4b73-b634-c3600dc567d5'))

print(mutationDB.findMutationsInAnnotatedAreaOfGene('ENSG00000141510','coding'))    #TP53
print(mutationDB.findMutationsInAnnotatedAreaOfGene('ENSG00000164362','promotor'))  #TERT


mutationDB.closeDatabases()
