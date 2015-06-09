import csv, os, datetime
import re, math, operator
import collections, pickle

import jellyfish
from nltk.corpus import stopwords

import time
def runtime(function):
  def wrap(*arg):
    start = time.time()
    r = function(*arg)
    end = time.time()
    print "%s (%0.3f ms)" % (function.func_name, (end-start)*1000)
    return r
  return wrap


def charFilter(string):
  """
  Ignore all non-alphabet except space
  """
  return re.sub('[^a-z ]', '', string)


def euclidean_distance(p, q):
  """
  Return distance between two points
  """
  distance = 0
  for d in p:
    if d in q:
      distance += (p[d] - q[d]) ** 2  # Both
    else:
      distance += p[d] ** 2           # p only

  for d in q:
    if d not in p:
      distance += q[d] ** 2           # q only

  return math.sqrt(distance)


def authorCmp(author1, author2):
  """
  Return author similairty. Called by coauthorCmp
    Publication Count : euclidean
  """
  # Publish Count Distance
  countDistance = euclidean_distance(author1[0], author2[0])

  return (countDistance,)


def coauthorCmp(author, coauthors):
  """
  Return mean of distance between each co-author
  """
  num = len(author)
  mean = [0.0 for i in range(0, num)]
  cnt = len(coauthors)

  for coauthor in coauthors:
    ret = authorCmp(author, coauthor)
    mean = map(operator.add, mean, ret)

  if cnt > 0:
    return [mean[i] / cnt for i in range(0, num)]
  else:
    return mean


def stringDistance(str1, str2):
  """
  Return distance between two strings
    String distance : jaro + levenshtein + damerau
  """
  distance = 0
  if len(str1) > 0 and len(str2) > 0:
    str1 = str1.decode('utf-8')
    str2 = str2.decode('utf-8')

    jaro = jellyfish.jaro_distance(str1, str2)
    leven = jellyfish.levenshtein_distance(str1, str2)
    damerau = jellyfish.damerau_levenshtein_distance(str1, str2)

    norm = max(len(str1), len(str2))
    distance = 0.5 * jaro + 0.25 * (1 - leven / norm)   \
                          + 0.25 * (1 - damerau / norm)

  return distance


def paperCmp(paper1, paper2):
  """
  Return paper similarity. Called by publicationCmp
  """

  titleDistance   = stringDistance(paper1[0], paper2[0])
  publishDistance = stringDistance(paper1[1], paper2[1])

  return (titleDistance, publishDistance)


def publicationCmp(paper, publications):
  """
  Return mean of distance between each co-author
  """
  num = len(paper)
  mean = [0.0 for i in range(0, num)]
  cnt = len(publications)

  for publication in publications:
    ret = paperCmp(paper, publication)
    mean = map(operator.add, mean, ret)

  if cnt > 0:
    return [mean[i] / cnt for i in range(0, num)]
  else:
    return mean

def ddInt():
  return collections.defaultdict(int)

class Data:
  def __init__ (self, runDir):
    self.dataDir = runDir + '/original_data/'
    self.pickleDir = runDir + '/pickles/'
    self.resultDir = runDir + '/preprocess/'
    self.stopword = stopwords.words('english')  # remove high-frequency words
    self.currentTime = str(datetime.datetime.now())

    if not os.path.exists(self.dataDir):
      os.makedirs(self.dataDir)
    if not os.path.exists(self.pickleDir):
      os.makedirs(self.pickleDir)
    if not os.path.exists(self.resultDir):
      os.makedirs(self.resultDir)

    # Author.csv
    self.authorAffiliation = collections.defaultdict(str)  # authorId -> authorAffiliation

    # Paper.csv
    self.paperTitle   = collections.defaultdict(str)  # paperId -> paperTitle
    self.paperYear    = collections.defaultdict(int)  # paperId -> paperYear
    self.paperPublish = {}                            # paperId -> conferenceId/journalId

    # Conferene.csv, Journal.csv
    self.publishName = collections.defaultdict(str)  # conferenceId/journalId -> conferenceName/journalName
    self.journalPad  = 0   # max(conferenceId)

    # PaperAuthor.csv
    self.paperCoAuthors     = collections.defaultdict(dict)  # paperId -> authorIds
    self.authorPublications = collections.defaultdict(dict)  # authorId -> paperIds
    self.authorPublishCount = collections.defaultdict(ddInt)  # authorId, conferenceId/journalId -> # papers

    # Train.csv
    self.confirmed  = collections.defaultdict(list)  # authorId -> confirmedPaperIds
    self.deleted    = collections.defaultdict(list)  # authorId -> deletedPaperIds
    self.trainYear  = collections.defaultdict(int)    # authorId -> mean of confirmed paperYear
    self.trainCount = collections.defaultdict(ddInt)  # authorId, conferenceId/journalId -> # confirmed papers

    # Test.csv
    self.unknown = collections.defaultdict(list)  # authorId -> unknown paperIds

    self.minYear = 1900
    self.maxYear = 2013


  @runtime
  def readAuthor(self, csvFile='Author.csv', pickleFile='author.dat'):
    """
    AuthorId, Affiliation
    """
    if os.path.isfile(self.pickleDir + pickleFile):
      with open(self.pickleDir + pickleFile, 'rb') as f:
        pickleAffiliation = pickle.load(f)

        for k,v in pickleAffiliation.iteritems():
          self.authorAffiliation[k] = v

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        aid = int(row[0])
        aff = row[2].lower()

        if len(aff) > 0:
          aff = charFilter(aff)
          aff = " ".join([w for w in aff.split() if not w in self.stopword])
          self.authorAffiliation[aid] = aff

    with open(self.pickleDir + pickleFile, 'wb') as f:
      pickle.dump(self.authorAffiliation, f)


  @runtime
  def readPaper(self, csvFile='Paper.csv', pickleFile='paper.dat'):
    """
    PaperId, Title, Year, ConferenceId, JournalId, Keywords
    """
    if os.path.isfile(self.pickleDir + pickleFile):
      with open(self.pickleDir + pickleFile, 'rb') as f:
        pickleTitle       = pickle.load(f)
        pickleYear        = pickle.load(f)
        self.paperPublish = pickle.load(f)

        for k,v in pickleTitle.iteritems():
          self.paperTitle[k] = v
        for k,v in pickleYear.iteritems():
          self.paperYear[k] = v

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        pid   = int(row[0])
        title = row[1].lower()
        year  = int(row[2])
        cid   = int(row[3])
        jid   = int(row[4])

        # Remove error
        if year < self.minYear:
          self.paperYear[pid] = 0
        elif year > self.maxYear:
          self.paperYear[pid] = 1
        else:
          self.paperYear[pid] = (year - self.minYear) / float(self.maxYear - self.minYear)

        # Merge conference and journal
        if cid > 0:
          self.paperPublish[pid] = cid
        elif jid > 0:
          self.paperPublish[pid] = jid + self.journalPad

        # Remove high-frequency words
        if len(title) > 0:
          title = charFilter(title)
          title = " ".join([w for w in title.split() if not w in self.stopword])
          self.paperTitle[pid] = title

    with open(self.pickleDir + pickleFile, 'wb') as f:
      pickle.dump(self.paperTitle, f)
      pickle.dump(self.paperYear, f)
      pickle.dump(self.paperPublish, f)

  @runtime
  def readConference(self, csvFile='Conference.csv', pickleFile='publish.dat'):
    """
    ConferenceId, FullName
    """
    if os.path.isfile(self.pickleDir + pickleFile):
      with open(self.pickleDir + pickleFile, 'rb') as f:
        pickeName = pickle.load(f)
        self.journalPad = pickle.load(f)
        self.journalPad = int(self.journalPad)

        for k,v in pickeName.iteritems():
          self.publishName[k] = v

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    stopwordConference = ['conference', 'international', 'workshop', \
                           'systems', 'ieee', 'symposium']
    stopwordConference += self.stopword

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        cid  = int(row[0])
        full = row[2].lower()

        if cid > self.journalPad:
          self.journalPad = cid

        # Remove high-frequency words
        if len(full) > 0:
          full = charFilter(full)
          full = " ".join([w for w in full.split() if not w in stopwordConference])
          self.publishName[cid] = full


  @runtime
  def readJournal(self, csvFile='Journal.csv', pickleFile='publish.dat'):
    """
    JournalId, FullName
    """
    if os.path.isfile(self.pickleDir + pickleFile):
      print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
      return

    stopwordJournal = ['journal', 'international', 'research', \
                        'science', 'review', 'engineering']
    stopwordJournal += self.stopword

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        jid  = int(row[0]) + self.journalPad
        full = row[2].lower()

        # Remove high-frequency words
        if len(full) > 0:
          full = charFilter(full)
          full = " ".join([w for w in full.split() if not w in stopwordJournal])
          self.publishName[jid] = full

    with open(self.pickleDir + pickleFile, 'wb') as f:
      pickle.dump(self.publishName, f)
      pickle.dump(self.journalPad , f)


  @runtime
  def readPaperAuthor(self, csvFile='PaperAuthor.csv', pickleFile='paperauthor.dat'):
    """
    PaperId, AuthorId
    """
    if os.path.isfile(self.pickleDir + pickleFile):
      with open(self.pickleDir + pickleFile, 'rb') as f:
        pickleCoAuthors    = pickle.load(f)
        picklePublications = pickle.load(f)
        picklePublishCount = pickle.load(f)

        for k,v in pickleCoAuthors.iteritems():
          self.paperCoAuthors[k] = v
        for k,v in picklePublications.iteritems():
          self.authorPublications[k] = v
        for k,v in picklePublishCount.iteritems():
          for kk,vv in v.iteritems():
            self.authorPublishCount[k][kk] = vv

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        pid = int(row[0])
        aid = int(row[1])
        aff = row[3].lower()

        # Affiliation
        if len(aff) > 0:
          aff = charFilter(aff)
          aff = " ".join([w for w in aff.split() if not w in self.stopword])
          if aid not in self.authorAffiliation:
            self.authorAffiliation[aid] = aff
          elif len(self.authorAffiliation[aid]) > len(aff):
            pass
          else:
            self.authorAffiliation[aid] = aff

        # Co-authors
        self.paperCoAuthors[pid][aid] = 1

        # Author's publications
        self.authorPublications[aid][pid] = 1

        # Count the number of papers published to conference / journal
        for pid in self.authorPublications[aid]:
          if pid in self.paperPublish:
            cid = self.paperPublish[pid]
            self.authorPublishCount[aid][cid] += 1

        # Normalize
        if len(self.authorPublishCount[aid]) > 0:
          maxCount = max(self.authorPublishCount[aid].iteritems(), key=operator.itemgetter(1))[1]
          minCount = min(self.authorPublishCount[aid].iteritems(), key=operator.itemgetter(1))[1]
          if maxCount == minCount:
            for cid in self.authorPublishCount[aid]:
              self.authorPublishCount[aid][cid] = 1
          else:
            for cid in self.authorPublishCount[aid]:
              self.authorPublishCount[aid][cid] -= minCount
              self.authorPublishCount[aid][cid] /= float(maxCount - minCount)

    with open(self.pickleDir + pickleFile, 'wb') as f:
      pickle.dump(self.paperCoAuthors, f)
      pickle.dump(self.authorPublications, f)
      pickle.dump(self.authorPublishCount, f)


  @runtime
  def readTrain(self, csvFile='Train.csv', pickleFile='train.dat', outFile='preprocess.csv', refresh=0):
    if os.path.isfile(self.pickleDir + pickleFile) and refresh is 0:
      with open(self.pickleDir + pickleFile, 'rb') as f:
        self.confirmed = pickle.load(f)
        self.deleted   = pickle.load(f)
        self.trainYear  = pickle.load(f)
        self.trainCount = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      with open(self.resultDir + outFile + '.' + self.currentTime, 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','PaperYear','PublishCount', \
                         'PaperTitle','Publish','mark'])

        for row in reader:
          if len(row) < 3:
            continue
          aid       = int(row[0])
          confirmed = map(int, set(row[1].split()))
          deleted   = map(int, set(row[2].split()))

          authorInfo = self.getAuthorInfo(aid)
          publicationInfo = self.getPublicationsInfo(aid)

          for pid in confirmed:
            # for testing
            if pid in self.paperYear:
              yearNorm = self.paperYear[pid]  # prevent auto-creation of pid
            else:
              yearNorm = 0

            if pid in self.paperPublish:
              cid = self.paperPublish[pid]
              self.trainCount[aid][cid] += 1

            self.trainYear[aid] += yearNorm

            # for training
            self.confirmed[aid].append(pid)

            coauthorInfo = self.getCoAuthorsInfo(aid, pid)
            authorSimilarity = coauthorCmp(authorInfo, coauthorInfo)

            paperInfo = self.getPaperInfo(pid)
            paperSimilarity = publicationCmp(paperInfo, publicationInfo)

            writer.writerow([aid, pid, yearNorm] + authorSimilarity + paperSimilarity + [1,])

          self.trainYear[aid] /= float(len(confirmed))

          for pid in deleted:
            self.deleted[aid].append(pid)

            coauthorInfo = self.getCoAuthorsInfo(aid, pid)
            authorSimilairty = coauthorCmp(authorInfo, coauthorInfo)

            paperInfo = self.getPaperInfo(pid)
            paperSimilarity = publicationCmp(paperInfo, publicationInfo)

            if pid in self.paperYear:
              yearNorm = self.paperYear[pid]  # prevent auto-creation of pid
            else:
              yearNorm = 0
              
            writer.writerow([aid, pid, yearNorm] + authorSimilarity + paperSimilarity + [-1,])

    with open(self.pickleDir + pickleFile, 'wb') as f:
      pickle.dump(self.confirmed, f)
      pickle.dump(self.deleted, f)
      pickle.dump(self.trainYear, f)
      pickle.dump(self.trainCount, f)


  @runtime
  def readTest(self, csvFile='Test.csv', pickleFile='test.dat', outFile='preprocess_test.csv', refresh=1):
    print csvFile, pickleFile, outFile, refresh
    if os.path.isfile(self.pickleDir + pickleFile) and refresh is 0:
      with open(self.pickleDir + pickleFile, 'rb') as f:
        self.unknown = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      with open(self.resultDir + outFile + '.' + self.currentTime, 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','PaperYear','PublishCount', \
                         'PaperTitle','Publish','mark'])

        for row in reader:
          aid     = int(row[0])
          unknown = map(int, set(row[1].split()))

          authorInfo = self.getAuthorInfo(aid)
          publicationInfo = self.getPublicationsInfo(aid)

          for pid in unknown:
            self.unknown[aid].append(pid)

            coauthorInfo = self.getCoAuthorsInfo(aid, pid)
            authorSimilarity = coauthorCmp(authorInfo, coauthorInfo)

            paperInfo = self.getPaperInfo(pid)
            paperSimilarity = publicationCmp(paperInfo, publicationInfo)

            if pid in self.paperYear:
              yearNorm = self.paperYear[pid]
            else:
              yearNorm = 0
              
            writer.writerow([aid, pid, yearNorm] + authorSimilarity + paperSimilarity + [0,])

    with open(self.pickleDir + pickleFile, 'wb') as f:
      pickle.dump(self.unknown, f)


  @runtime
  def readTestFull(self, csvFile='Test.csv', pickleFile='testfull.dat', outFile='preprocess_testfull.csv'):
    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()

      with open(self.resultDir + outFile + '.' + self.currentTime, 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','KnonwAuthorId','Similarity'])

        # Localize for performance
        paperYear = self.paperYear
        learned = self.confirmed
        trainYear = self.trainYear
        trainCount = self.trainCount
        paperPublish = self.paperPublish

        neighbority   = {}
        neighborYear  = {}
        neighborCount = {}

        for row in reader:
          aid  = int(row[0])
          pids = map(int, set(row[1].split()))

          # unknown
          naff = self.authorAffiliation[aid]
          nyear = 0
          ncount = collections.defaultdict(int)
          for pid in pids:
            if pid in paperPublish:
              cid = paperPublish[pid]
              ncount[cid] += 1
            nyear += paperYear[pid]
          nyear /= float(len(pids))

          # learned
          for caid in learned:
            caff = self.authorAffiliation[caid]
            yearDistance = math.sqrt((trainYear[caid] - nyear) ** 2)
            countDistance = 0
            for cid in ncount:
              if cid in trainCount[caid]:
                countDistance += (ncount[cid] - trainCount[caid][cid]) ** 2
              else:
                countDistance += ncount[cid] ** 2
            for cid in trainCount[caid]:
              if cid not in ncount:
                countDistance += trainCount[caid][cid] ** 2
            countDistance = math.sqrt(countDistance)

            neighbority[caid]   = 0.33 * stringDistance(naff, caff)
            neighborYear[caid]  = yearDistance
            neighborCount[caid] = countDistance

          # Find 5 nearest neighbors
          max_year  = max(neighborYear.iteritems(),  key=operator.itemgetter(1))[1]
          max_count = max(neighborCount.iteritems(), key=operator.itemgetter(1))[1]
          for caid in neighborYear:
            neighbority[caid] += 0.33 * neighborYear[caid] / max_year + 0.33 * neighborCount[caid] / max_count

          nearestList = []
          for i in range(5):
            try:
              nearest = max(neighbority.iteritems(), key=operator.itemgetter(1))
              nearestList.extend([nearest[0], nearest[1]])
              neighbority.pop(nearest[0])
            except ValueError:
              break

          writer.writerow([aid,] + nearestList)


  def getAuthorInfo(self, aid):
    """
    Return co-author information. Called by getCoAuthorInfo
    """
    return (self.authorPublishCount[aid],)


  def getCoAuthorsInfo(self, author, paper):
    """
    Return list of co-author information
    """
    coauthorsInfo = []

    for coauthor in self.paperCoAuthors[paper]:
      if author != coauthor:
        coauthorsInfo.append(self.getAuthorInfo(coauthor))

    return coauthorsInfo
    

  def getPaperInfo(self, pid):
    """
    Return author's publication information. Called by getPublicationsInfo
    """
    publish = ""
    title = self.paperTitle[pid]

    if pid in self.paperPublish:
      cid = self.paperPublish[pid]
      publish = self.publishName[cid]

    return (title, publish)


  def getPublicationsInfo(self, author):
    """
    Return list of author's publications information
    """
    publicationInfo = []

    if author in self.authorPublications:
      for publication in self.authorPublications[author]:
        publicationInfo.append(self.getPaperInfo(publication))

    return publicationInfo


  def TrainTestClear(self):
    self.confirmed  = collections.defaultdict(list)
    self.deleted    = collections.defaultdict(list)
    self.trainYear  = collections.defaultdict(int)
    self.trainCount = collections.defaultdict(ddInt)
    self.unknown    = collections.defaultdict(list)
# --- class Data


def main():
  data = Data(os.getcwd())

  # Parse given data
  print '[*] Start to read Author.csv'
  data.readAuthor()

  print '[*] Start to read Conference.csv'
  data.readConference()
  
  print '[*] Start to read Journal.csv'
  data.readJournal()

  print '[*] Start to read Paper.csv'
  data.readPaper()

  print '[*] Start to read PaperAuthor.csv'
  data.readPaperAuthor()

  # Preprocessing
  print '[*] Start to read Train.csv'
  data.readTrain()

  #print '[*] Start to read Valid.csv'
  #data.readTestFull('Valid.csv','valid.dat','preprocess_valid.csv')

  #data.TrainTestClear()

  #print '[*] Start to read Train+Valid.csv'
  #data.readTrain('Train+Valid.csv','train_valid.dat','preprocess_train+valid.csv')

  print '[*] Start to read Test.csv'
  data.readTestFull()

if __name__ == "__main__":
  main()
