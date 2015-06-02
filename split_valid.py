import csv, os
import pickle

class Data:
  def __init__ (self, runDir):
    self.dataDir = runDir + '/original_data/'
    self.pickleDir = runDir + '/pickles/'

    if not os.path.exists(self.dataDir):
      os.makedirs(self.dataDir)

    self.confirmed = {}
    self.deleted   = {}
    #self.usages    = {}
    self.aids = []


  def readValidSolution(self, csvFile   ='ValidSolution.csv' \
                            , pickleFile='validsolution.dat'):

    if os.path.isfile(self.pickleDir + pickleFile):
      with open(self.pickleDir + pickleFile) as f:
        self.confirmed = pickle.load(f)
        #self.usages    = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        aid   = int(row[0])
        pids  = row[1].split()
        #usage = row[2]

        if aid not in self.aids:
          self.aids.append(aid)

        if aid not in self.confirmed:
          self.confirmed[aid] = []

        for pid in pids:
          if pid not in self.confirmed[aid]:
            self.confirmed[aid].append(pid)

        #self.usages[aid] = usage

    with open(self.pickleDir + 'validsolution.dat', 'wb') as f:
      pickle.dump(self.confirmed, f)
      #pickle.dump(self.usages, f)


  def readValid(self, csvFile='Valid.csv', pickleFile='valid.dat'):

    if os.path.isfile(self.pickleDir + 'valid.dat'):
      with open(self.pickleDir + 'valid.dat') as f:
        self.deleted = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + 'Valid.csv', 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()

      for row in reader:
        aid  = int(row[0])
        pids = row[1].split()

        if aid not in self.aids:
          self.aids.append(aid)

        if aid not in self.deleted:
          self.deleted[aid] = []

        for pid in pids:
          if aid not in self.confirmed \
             or pid not in self.confirmed[aid] and pid not in self.deleted[aid]:
            self.deleted[aid].append(pid)

    with open(self.pickleDir + 'valid.dat', 'wb') as f:
      pickle.dump(self.deleted, f)


  def writeValidToTrain(self, csvFile='ValidToTrain.csv'):
    with open(self.dataDir + csvFile, 'wb') as csvOut:
      writer = csv.writer(csvOut, delimiter=',')
      writer.writerow(['AuthorId','ConfirmedPaperIds','DeletedPaperIds'])

      for aid in self.aids:
        if aid not in self.confirmed:
          writeConfirm = []
        else:
          writeConfirm = [" ".join(self.confirmed[aid])]

        if aid not in self.deleted:
          writeDelete = []
        else:
          writeDelete = [" ".join(self.deleted[aid])]

        writer.writerow([aid,] + writeConfirm + writeDelete)

def main():
  data = Data(os.getcwd())

  print '[*] Start to read ValidSolution.csv'
  data.readValidSolution()

  print '[*] Start to read Valid.csv'
  data.readValid()

  print '[*] Start to create ValidToTrain.csv'
  data.writeValidToTrain()

if __name__ == "__main__":
  main()
