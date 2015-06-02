import csv, os, datetime
import re, math, operator, jellyfish
from nltk.corpus import stopwords
import threading, pickle

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
  Ignore unmatched dimension
  """
  distance = 0
  for d in p:
    if d in q:
      distance += (p[d] - q[d]) ** 2
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
  cnt  = [0 for i in range(0, num)]

  for coauthor in coauthors:
    ret = authorCmp(author, coauthor)
    mean = map(operator.add, mean, ret)

    for i in range(0, num):
      if ret[i] > 0:
        cnt[i] += 1

  return [mean[i] / cnt[i] if cnt[i] > 0 else 0 for i in range(0, num)]


def paperCmp(paper1, paper2):
  """
  Return paper similarity. Called by publicationCmp
    Title    : jaro + levenshtein + damerau
    Publish  : jaro + levenshtein + damerau
  """
  titleDistance = 0
  publishDistance = 0

  # Title Distance
  if len(paper1[0]) > 0 and len(paper2[0]) > 0:
    title1 = paper1[0].decode('utf-8')
    title2 = paper2[0].decode('utf-8')

    jaro = jellyfish.jaro_distance(title1, title2)
    leven = jellyfish.levenshtein_distance(title1, title2)
    damerau = jellyfish.damerau_levenshtein_distance(title1, title2)

    norm = max(len(title1), len(title2))
    titleDistance = 0.5 * jaro + 0.25 * (1 - leven / norm)   \
                               + 0.25 * (1 - damerau / norm)

  # publish Distance
  if len(paper1[1]) > 0 and len(paper2[1]) > 0:
    publish1 = paper1[1].decode('utf-8')
    publish2 = paper2[1].decode('utf-8')

    jaro = jellyfish.jaro_distance(publish1, publish2)
    leven = jellyfish.levenshtein_distance(publish1, publish2)
    damerau = jellyfish.damerau_levenshtein_distance(publish1, publish2)

    norm = max(len(publish1), len(publish2))
    publishDistance = 0.5 * jaro + 0.25 * (1 - leven / norm)   \
                                 + 0.25 * (1 - damerau / norm)

  return (titleDistance, publishDistance)

def publicationCmp(paper, publications):
  """
  Return mean of distance between each co-author
  """
  num = len(paper)
  mean = [0.0 for i in range(0, num)]
  cnt  = [0 for i in range(0, num)]

  for publication in publications:
    ret = paperCmp(paper, publication)
    mean = map(operator.add, mean, ret)

    for i in range(0, num):
      if ret[i] > 0:
        cnt[i] += 1

  return [mean[i] / cnt[i] if cnt[i] > 0 else 0 for i in range(0, num)]


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

    # Paper.csv
    self.paperTitle   = {}  # paperId -> paperTitle
    self.paperYear    = {}  # paperId -> paperYear
    self.paperPublish = {}  # paperId -> conferenceId/journalId

    # Conferene.csv, Journal.csv
    self.publishName = {}  # conferenceId/journalId -> conferenceName/journalName
    self.journalPad  = 0   # max(conferenceId)

    # PaperAuthor.csv
    self.paperCoAuthors     = {}  # paperId -> authorIds
    self.authorPublications = {}  # authorId -> paperIds
    self.authorPublishCount = {}  # authorId, conferenceId/journalId -> # papers

    # Train.csv
    self.confirmed = {}
    self.deleted  = {}

    self.minYear = 1900
    self.maxYear = 2013

  @runtime
  def readPaper(self, csvFile='Paper.csv', pickleFile='paper.dat'):
    """
    PaperId, Title, Year, ConferenceId, JournalId, Keywords
    """
    if os.path.isfile(self.pickleDir + pickleFile):
      with open(self.pickleDir + pickleFile, 'rb') as f:
        self.paperTitle      = pickle.load(f)
        self.paperYear       = pickle.load(f)
        self.paperPublish    = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        pid      = int(row[0])
        title    = row[1].lower()
        year     = int(row[2])
        cid      = int(row[3])
        jid      = int(row[4])


        if year < self.minYear:
          self.paperYear[pid] = 0
        elif year > self.maxYear:
          self.paperYear[pid] = 1
        else:
          self.paperYear[pid] = (year - self.minYear) / float(self.maxYear - self.minYear)

        if cid > 0:
          self.paperPublish[pid] = cid
        elif jid > 0:
          self.paperPublish[pid] = jid + self.journalPad

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
        self.publishName = pickle.load(f)
        self.journalPad = pickle.load(f)
        self.journalPad = int(self.journalPad)

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

        if len(full) > 0:
          full = charFilter(full)
          full = " ".join([w for w in full.split() if not w in stopwordConference])
          self.publishName[cid] = full


  # FIXME: Can be handled as a sing field with conference
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
        self.paperCoAuthors     = pickle.load(f)
        self.authorPublications = pickle.load(f)
        self.authorPublishCount = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        pid = int(row[0])
        aid = int(row[1])

        # Co-authors
        if pid not in self.paperCoAuthors:
          self.paperCoAuthors[pid] = []
        if aid not in self.paperCoAuthors[pid]:
          self.paperCoAuthors[pid].append(aid)

        # Author's publications
        if aid not in self.authorPublications:
          self.authorPublications[aid] = []
        if pid not in self.authorPublications[aid]:
          self.authorPublications[aid].append(pid)

        # Count the number of papers published to conference / journal
        if aid not in self.authorPublishCount:
          self.authorPublishCount[aid] = {}

        # Count
        for pid in self.authorPublications[aid]:
          if pid in self.paperPublish:
            cid = self.paperPublish[pid]
            if cid not in self.authorPublishCount[aid]:
              self.authorPublishCount[aid][cid] = 0
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
  #def readTrain(self, csvFile='Test.csv', pickleFile='test.dat', refresh=0):
  #def readTrain(self, csvFile='Train+Valid.csv', pickleFile='train_valid.dat', refresh=0):
  def readTrain(self, csvFile='Train.csv', pickleFile='train.dat', refresh=0):
    if os.path.isfile(self.pickleDir + pickleFile) and refresh is 0:
      with open(self.pickleDir + pickleFile, 'rb') as f:
        self.confirmed = pickle.load(f)
        self.deleted   = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      with open(self.resultDir + 'preprocess.csv.' + self.currentTime, 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','PaperYear','PublishCount', \
                         'PaperTitle','Publish','mark'])

        for row in reader:
          aid = int(row[0])
          confirmed = row[1].split()
          deleted  = row[2].split()

          if aid not in self.confirmed:
            self.confirmed[aid] = []
          if aid not in self.deleted:
            self.deleted[aid] = []

          authorInfo = self.getAuthorInfo(aid)
          publicationInfo = self.getPublicationsInfo(aid)

          for pid in confirmed:
            pid = int(pid)
            self.confirmed[aid].append(pid)

            coauthorInfo = self.getCoAuthorsInfo(aid, pid)
            authorSimilarity = coauthorCmp(authorInfo, coauthorInfo)

            paperInfo = self.getPaperInfo(pid)
            paperSimilarity = publicationCmp(paperInfo, publicationInfo)

            if pid in self.paperYear:
              yearNorm = self.paperYear[pid]
            else:
              yearNorm = 0
              
            writer.writerow([aid, pid, yearNorm] + authorSimilarity + paperSimilarity + [1,])

          for pid in deleted:
            pid = int(pid)
            self.deleted[aid].append(pid)

            coauthorInfo = self.getCoAuthorsInfo(aid, pid)
            authorSimilairty = coauthorCmp(authorInfo, coauthorInfo)

            paperInfo = self.getPaperInfo(pid)
            paperSimilarity = publicationCmp(paperInfo, publicationInfo)

            if pid in self.paperYear:
              yearNorm = self.paperYear[pid]
            else:
              yearNorm = 0
              
            writer.writerow([aid, pid, yearNorm] + authorSimilarity + paperSimilarity + [-1,])

    with open(self.pickleDir + pickleFile, 'wb') as f:
      pickle.dump(self.confirmed, f)
      pickle.dump(self.deleted, f)


  @runtime
  def readTest(self, csvFile='Test.csv', pickleFile='test.dat', refresh=0):
    if os.path.isfile(self.pickleDir + pickleFile) and refresh is 0:
      with open(self.pickleDir + pickleFile, 'rb') as f:
        self.unknown = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      with open(self.resultDir + 'preprocess_test.csv.' + self.currentTime, 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','PaperYear','PublishCount', \
                         'PaperTitle','Publish','mark'])

        for row in reader:
          aid = int(row[0])
          unknown = row[1].split()

          if aid not in self.unknown:
            self.unknown[aid] = []

          authorInfo = self.getAuthorInfo(aid)
          publicationInfo = self.getPublicationsInfo(aid)

          for pid in unknown:
            pid = int(pid)
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
  def readTest_(self, csvFile='Test.csv', pickleFile='test.dat', outFile='test_preprocess.csv'):
    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()

      learned = [i for i in self.confirmed]
      tmp     = [i for i in self.deleted]

      for tid in tmp:
        if tid not in learned:
          learned.append(tid)

      with open(self.resultDir + outFile, 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','KnonwAuthorId','Similarity'])

        for row in reader:
          aid  = int(row[0])
          pids = row[1].split()

          writemap = {}

          authorInfo = self.getAuthorInfo(aid)
          #if aid not in self.authorYearNorm:
          #  self.authorYearNorm[aid] = {}

          #for caid in self.confirmed:

          for caid in learned:
            caid = int(caid)

            coauthorInfo = [self.getAuthorInfo(caid),]
            authorSimilarity = coauthorCmp(authorInfo, coauthorInfo)

            publicationInfo = self.getPublicationsInfo(caid) # Use error data set also

            for pid in pids:
              pid = int(pid)

              if pid not in writemap:
                writemap[pid] = []

              paperInfo = self.getPaperInfo(pid)
              if pid in self.paperYear:
                yearNorm = self.paperYear[pid]
              else:
                yearNorm = 0
                
              paperSimilarity = publicationCmp(paperInfo, publicationInfo)

              #comparePool.append([aid, year] + authorSimilarity + paperSimilarity)
              tmp = [caid, yearNorm] + authorSimilarity + paperSimilarity
              writemap[pid].append(tmp)
              #writemap[pid].append([caid, yearNorm] + authorSimilarity + paperSimilarity)

          for pid in writemap:
            writer.writerow([aid, pid] + writemap[pid])

          print aid, 'IS DONE. GO TO NEXT'


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

    if paper in self.paperCoAuthors:
      for coauthor in self.paperCoAuthors[paper]:
        if author != coauthor:
          coauthorsInfo.append(self.getAuthorInfo(coauthor))

    return coauthorsInfo
    

  def getPaperInfo(self, pid):
    """
    Return author's publication information. Called by getPublicationsInfo
    """
    title   = ""
    publish = ""

    if pid in self.paperTitle:
      title = self.paperTitle[pid]

    if pid in self.paperPublish:
      cid = self.paperPublish[pid]
      if cid in self.publishName:
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


def main():
  data = Data(os.getcwd())

  print '[*] Start to read Conference.csv'
  data.readConference()
  
  print '[*] Start to read Journal.csv'
  data.readJournal()

  print '[*] Start to read Paper.csv'
  data.readPaper()

  print '[*] Start to read PaperAuthor.csv'
  data.readPaperAuthor()

  print '[*] Start to read Train.csv'
  data.readTrain()

  #print '[*] Start to read Test.csv'
  #data.readTest()

if __name__ == "__main__":
  main()
