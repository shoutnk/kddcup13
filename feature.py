import csv, re, os, math, operator, jellyfish
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


stopword = stopwords.words('english')  # remove high-frequency words

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
    Affiliation       : jaro + levenshtein + damerau,
    Publication Count : euclidean
  """
  # Affiliation Distanace
  if len(author1[0]) > 0 and len(author2[0]) > 0:
    aff1 = author1[0].decode('utf-8')
    aff2 = author2[0].decode('utf-8')

    jaro = jellyfish.jaro_distance(aff1, aff2)
    leven = jellyfish.levenshtein_distance(aff1, aff2)
    damerau = jellyfish.damerau_levenshtein_distance(aff1, aff2)

    try:
      affiliationDistance = 0.5 * jaro + 0.5 * (1.0 / leven + 1.0 / damerau)
    except ZeroDivisionError:
      print '[*] aff1: %s, aff2: %s - jaro: %f, leven: %f, damerau: %f' % (aff1, aff2, jaro, leven, damerau)
  else:
    affiliationDistance = 0

  # Publish Count Distance
  conferenceDistance = euclidean_distance(author1[1], author2[1])
  journalDistance = euclidean_distance(author1[2], author2[2])

  return (affiliationDistance, conferenceDistance, journalDistance)

def coauthorCmp(author, coauthors):
  """
  Return mean of distance between each co-author
  """
  mean = [0.0, 0.0, 0.0]
  cnt = 0
  for coauthor in coauthors:
    ret = authorCmp(author, coauthor)
    mean = map(operator.add, mean, ret)
    cnt += 1

  if cnt > 0:
    return [i / cnt for i in mean]
  else:
    return mean


def paperCmp(paper1, paper2):
  """
  Return paper similarity. Called by publicationCmp
    Title    : jaro + levenshtein + damerau
    Publish  : jaro + levenshtein + damerau
    Keywords : jaro + levenshtein + damerau
  """
  # Title Distance
  if len(paper1[0]) > 0 and len(paper2[0]) > 0:
    title1 = paper1[0].decode('utf-8')
    title2 = paper2[0].decode('utf-8')

    jaro = jellyfish.jaro_distance(title1, title2)
    leven = jellyfish.levenshtein_distance(title1, title2)
    damerau = jellyfish.damerau_levenshtein_distance(title1, title2)

    try:
      titleDistance = 0.5 * jaro + 0.5 * (1.0 / leven + 1.0 / damerau)
    except ZeroDivisionError:
      print '[*] paper1: ', paper1, 'paper2: ', paper2, 'jaro: %f, leven: %f, damerau: %f' % (jaro, leven, damerau)
      
  else:
    titleDistance = 0

  # publish Distance
  if len(paper1[1]) > 0 and len(paper2[1]) > 0:
    publish1 = paper1[1].decode('utf-8')
    publish2 = paper2[1].decode('utf-8')

    jaro = jellyfish.jaro_distance(publish1, publish2)
    leven = jellyfish.levenshtein_distance(publish1, publish2)
    damerau = jellyfish.damerau_levenshtein_distance(publish1, publish2)

    if leven == 0 or damerau == 0:
      publishDistance = 0.5 * jaro + 1
    else:
      publishDistance = 0.5 * jaro + 0.5 * (1.0 / leven + 1.0 / damerau)
  else:
    publishDistance = 0

  # keywords Distance
  if len(paper1[2]) > 0 and len(paper2[2]) > 0:
    keywords1 = paper1[2].decode('utf-8')
    keywords2 = paper2[2].decode('utf-8')

    jaro = jellyfish.jaro_distance(keywords1, keywords2)
    leven = jellyfish.levenshtein_distance(keywords1, keywords2)
    damerau = jellyfish.damerau_levenshtein_distance(keywords1, keywords2)

    if leven == 0 or damerau == 0:
      keywordsDistance = 0.5 * jaro + 1
    else:
      keywordsDistance = 0.5 * jaro + 0.5 * (1.0 / leven + 1.0 / damerau)
  else:
    keywordsDistance = 0
  
  return (titleDistance, publishDistance, keywordsDistance)

def publicationCmp(paper, publications):
  """
  Return mean of distance between each co-author
  """
  mean = [0.0, 0.0, 0.0]
  cnt = 0
  for publication in publications:
    if paper[0] != publication[0]:
      ret = paperCmp(paper, publication)
      mean = map(operator.add, mean, ret)
      cnt += 1

  if cnt > 0:
    return [i / cnt for i in mean]
  else:
    return mean


class Data:
  def __init__ (self, run_dir):
    self.dataDir = run_dir + '/original_data/'

    # Author.csv
    self.authorAffiliation = {}  # authorId -> authorAffiliation

    # Paper.csv
    self.paperTitle      = {}  # paperId -> paperTitle
    self.paperYear       = {}  # paperId -> paperYear
    self.paperConference = {}  # paperId -> paperConferenceId
    self.paperJournal    = {}  # paperId -> paperJournalId
    self.paperKeywords   = {}  # paperId -> paperKeywords

    # Conferene.csv, Journal.csv
    self.conferenceName = {}  # conferenceId -> conferenceName
    self.journalName    = {}  # journalId -> journalname

    # PaperAuthor.csv
    self.paperCoAuthors     = {}  # paperId -> authorIds
    self.authorPublications = {}  # authorId -> paperIds

    # Self similartiy (pids in aid)
    self.authorConfCount    = {}  # authorId -> # conferenceIds
    self.authorJourCount    = {}  # authorId -> # journalIds
    #self.titleDistance      = {}  # authorId -> || paperTitles ||
    #self.keywordsDistance   = {}  # authorId -> || paperKeywords ||
    #self.conferenceDistance = {}  # authorId -> || conferenceNames ||
    #self.journalDistance    = {}  # authorId -> || journalNames ||


  @runtime
  def readAuthor(self, csvFile='Author.csv', pickleFile='._author.dat'):
    """
    AuthorId, Affiliation
    """
    if os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "rb") as f:
        self.authorAffiliation = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    global stopword

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        aid         = int(row[0])
        affiliation = row[2].lower()

        if len(affiliation) > 0:
          affiliation = charFilter(affiliation)
          affiliation = " ".join([w for w in affiliation.split() if not w in stopword])
          self.authorAffiliation[aid] = affiliation

    if not os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "wb") as f:
        pickle.dump(self.authorAffiliation, f)


  @runtime
  def readPaper(self, csvFile='Paper.csv', pickleFile='._paper.dat'):
    """
    PaperId, Title, Year, ConferenceId, JournalId, Keywords
    """
    if os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "rb") as f:
        self.paperTitle      = pickle.load(f)
        self.paperYear       = pickle.load(f)
        self.paperConference = pickle.load(f)
        self.paperJournal    = pickle.load(f)
        self.paperKeywords   = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    global stopword

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        pid      = int(row[0])
        title    = row[1].lower()
        year     = int(row[2])
        cid      = int(row[3])
        jid      = int(row[4])
        keywords = row[5].lower()


        if year >= 1600 and year <= 2013:
          self.paperYear[pid] = year

        if cid > 0:
          self.paperConference[pid] = cid

        # FIXME: 'cid' and 'jid' can be handled as a single field
        if jid > 0:
          self.paperJournal[pid] = jid

        if len(title) > 0:
          title = charFilter(title)
          title = " ".join([w for w in title.split() if not w in stopword])
          self.paperTitle[pid] = title

        if len(keywords) > 0:
          keywords = charFilter(keywords)
          keywords = re.sub('key.*?word', '', keywords)
          keywords = re.sub('key.*?term', '', keywords)
          keywords = re.sub('index.*?term', '', keywords)
          keywords = " ".join([w for w in keywords.split() if not w in stopword])
          self.paperKeywords[pid] = keywords

    if not os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "wb") as f:
        pickle.dump(self.paperTitle, f)
        pickle.dump(self.paperYear, f)
        pickle.dump(self.paperConference, f)
        pickle.dump(self.paperJournal, f)
        pickle.dump(self.paperKeywords, f)

  @runtime
  def readConference(self, csvFile='Conference.csv', pickleFile='._conference.dat'):
    """
    ConferenceId, FullName
    """
    if os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "rb") as f:
        self.conferenceName = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    global stopword
    stopwordConference = ['conference', 'international', 'workshop', \
                           'systems', 'ieee', 'symposium']
    stopwordConference = stopword + stopwordConference

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        cid   = int(row[0])
        full  = row[2].lower()

        if len(full) > 0:
          full = charFilter(full)
          full = " ".join([w for w in full.split() if not w in stopwordConference])
          self.conferenceName[cid] = full

    if not os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "wb") as f:
        pickle.dump(self.conferenceName, f)


  # FIXME: Can be handled as a sing field with conference
  @runtime
  def readJournal(self, csvFile='Journal.csv', pickleFile='._journal.dat'):
    """
    JournalId, FullName
    """
    if os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "rb") as f:
        self.journalName = pickle.load(f)

        print ' # Load %s instead of parsing %s' % (pickleFile, csvFile)
        return

    global stopword
    stopwordJournal = ['journal', 'international', 'research', \
                        'science', 'review', 'engineering']
    stopwordJournal = stopword + stopwordJournal

    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      for row in reader:
        jid   = int(row[0])
        full  = row[2].lower()

        if len(full) > 0:
          full = charFilter(full)
          full = " ".join([w for w in full.split() if not w in stopwordJournal])
          self.journalName[jid] = full

    if not os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "wb") as f:
        pickle.dump(self.journalName, f)


  @runtime
  def readPaperAuthor(self, csvFile='PaperAuthor.csv', pickleFile='._paperauthor.dat'):
    """
    PaperId, AuthorId
    """
    if os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "rb") as f:
        self.paperCoAuthors     = pickle.load(f)
        self.authorPublications = pickle.load(f)
        self.authorConfCount    = pickle.load(f)
        self.authorJourCount    = pickle.load(f)

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
        self.paperCoAuthors[pid].append(aid)

        # Author's publications
        if aid not in self.authorPublications:
          self.authorPublications[aid] = []
        self.authorPublications[aid].append(pid)

        # Count the number of papers published to conference / journal
        if aid not in self.authorConfCount:
          self.authorConfCount[aid] = {}

        if aid not in self.authorJourCount:
          self.authorJourCount[aid] = {}

        for pid in self.authorPublications[aid]:
          if pid in self.paperConference:
            cid = self.paperConference[pid]
            if cid not in self.authorConfCount[aid]:
              self.authorConfCount[aid][cid] = 1
            else:
              self.authorConfCount[aid][cid] += 1

          elif pid in self.paperJournal:
            jid = self.paperJournal[pid]
            if jid not in self.authorJourCount[aid]:
              self.authorJourCount[aid][jid] = 1
            else:
              self.authorJourCount[aid][jid] += 1

    if not os.path.isfile(self.dataDir + pickleFile):
      with open(self.dataDir + pickleFile, "wb") as f:
        pickle.dump(self.paperCoAuthors, f)
        pickle.dump(self.authorPublications, f)
        pickle.dump(self.authorConfCount, f)
        pickle.dump(self.authorJourCount, f)


  @runtime
  def readTrain(self, csvFile='Train.csv', pickleFile='._train.dat'):
    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      with open(self.dataDir + 'preprocess.csv', 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','AuthorSimilarity','PublicationSimilarity','mark'])

        for row in reader:
          aid = int(row[0])
          confirmed = row[1].split()
          deleted  = row[2].split()

          authorInfo = self.getAuthorInfo(aid)
          publicationInfo = self.getPublicationsInfo(aid)

          for pid in confirmed:
            pid = int(pid)
            print '[+]', aid, pid, '+'
            coauthorInfo = self.getCoAuthorsInfo(aid, pid)
            authorSimilarity = coauthorCmp(authorInfo, coauthorInfo)

            paperInfo = self.getPaperInfo(pid)
            paperSimilarity = publicationCmp(paperInfo, publicationInfo)

            writer.writerow([aid, pid] + authorSimilarity + paperSimilarity + [1,])
            
          for pid in deleted:
            pid = int(pid)
            print '[+]', aid, pid, '-'
            coauthorInfo = self.getCoAuthorsInfo(aid, pid)
            authorSimilairty = coauthorCmp(authorInfo, coauthorInfo)

            paperInfo = self.getPaperInfo(pid)
            paperSimilarity = publicationCmp(paperInfo, publicationInfo)

            writer.writerow([aid, pid] + authorSimilarity + paperSimilarity + [-1,])


  @runtime
  def readValid(self, csvFile='Valid.csv', pickleFile='._valid.dat'):
    with open(self.dataDir + csvFile, 'rb') as csvFile:
      reader = csv.reader(csvFile)
      reader.next()  # pass column name

      with open(self.dataDir + 'valid_preprocess.csv', 'wb') as csvOut:
        writer = csv.writer(csvOut, delimiter=',')
        writer.writerow(['AuthorId','PaperId','AffiliationSimilarity','ConferenceCountSimilairty', \
                         'JournalCountSimilairty','TitleSimilairty','PublishSimilarity','KeywordSimilairty','mark'])

        for row in reader:
          aid = int(row[0])
          confirmed = row[1].split()
          deleted  = row[2].split()

          authorInfo = self.getAuthorInfo(aid)
          publicationInfo = self.getPublicationsInfo(aid)

          pid = int(pid)
          print '[+]', aid, pid, '?'
          coauthorInfo = self.getCoAuthorsInfo(aid, pid)
          authorSimilarity = coauthorCmp(authorInfo, coauthorInfo)

          paperInfo = self.getPaperInfo(pid)
          paperSimilarity = publicationCmp(paperInfo, publicationInfo)

          writer.writerow([aid, pid] + authorSimilarity + paperSimilarity + [0,])


  def getAuthorInfo(self, aid):
    """
    Return co-author information. Called by getCoAuthorInfo
    """
    (affiliation, conferenceCount, journalCount) = ("", {}, {})

    if aid in self.authorAffiliation:
      affiliation = self.authorAffiliation[aid]

    if aid in self.authorConfCount:
      conferenceCount = self.authorConfCount[aid]

    if aid in self.authorJourCount:
      journalCount = self.authorJourCount[aid]

    return (affiliation, conferenceCount, journalCount)

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
    (title, publish, keywords) = ("", "", "")

    if pid in self.paperTitle:
      title = self.paperTitle[pid]

    if pid in self.paperConference:
      cid = self.paperConference[pid]
      if cid in self.conferenceName:
        publish = self.conferenceName[cid]
    elif pid in self.paperJournal:
      jid = self.paperJournal[pid]
      if jid in self.journalName:
        publish = self.journalName[jid]

    if pid in self.paperKeywords:
      keywords = self.paperKeywords[pid]

    return (title, publish, keywords)

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
  runDir = os.getcwd()
  data = Data(runDir)

  threads = []

  print '[*] Start to read Paper.csv'
  thread = threading.Thread(target=data.readPaper)
  thread.start()
  threads.append(thread)

  print '[*] Start to read Conference.csv'
  thread = threading.Thread(target=data.readConference)
  thread.start()
  threads.append(thread)
  
  print '[*] Start to read Journal.csv'
  thread = threading.Thread(target=data.readJournal)
  thread.start()
  threads.append(thread)

  for thread in threads:
    thread.join()

  print '[*] Start to read PaperAuthor.csv'
  data.readPaperAuthor()

  print '[*] Start to read Train.csv'
  data.readTrain()

  print '[*] Start to read Valid.csv'
  data.readValid()

if __name__ == "__main__":
  main()
