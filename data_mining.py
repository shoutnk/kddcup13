import csv, os, pickle
import threading, Queue
import collections

#
paperId = {}
paperTitle = {}
paperYear = {}
paperCid = {}
paperJid = {}

#
confName = {}
jourName = {}

#
paperAuthor = {}
authorPaper = {}

#
print '[*] Read Paper.csv'
if os.path.isfile('compressed_data/dm_pickle/paper.dat'):
  with open('compressed_data/dm_pickle/paper.dat', 'rb') as f:
    paperId    = pickle.load(f)
    paperTitle = pickle.load(f)
    paperYear  = pickle.load(f)
    paperCid   = pickle.load(f)
    paperJid   = pickle.load(f)

    print ' # Load pickle instead'
else:
  with open('original_data/Paper.csv', 'rb') as csvFile:
    reader = csv.reader(csvFile)

    with open('compressed_data/Paper.csv', 'wb') as csvOut:
      writer = csv.writer(csvOut, delimiter=',')
      writer.writerow(reader.next())

      for row in reader:
        pid   = int(row[0])
        title = row[1]
        year  = int(row[2])
        cid  = int(row[3])
        jid  = int(row[4])

        if len(title) > 0 and year >= 1900 and year <= 2013 and (cid > 0 or jid > 0):
          paperId[pid] = 1
          paperTitle[pid] = title
          paperYear[pid]  = year
          paperCid[pid]   = cid
          paperJid[pid]   = jid

          writer.writerow([pid, title, year, cid, jid, ''])

  with open('compressed_data/dm_pickle/paper.dat', 'wb') as f:
    pickle.dump(paperId, f)
    pickle.dump(paperTitle, f)
    pickle.dump(paperYear, f)
    pickle.dump(paperCid, f)
    pickle.dump(paperJid, f)

    print ' # Dump pickle'

#
print '[*] Read Conference.csv'
if os.path.isfile('compressed_data/dm_pickle/conference.dat'):
  with open('compressed_data/dm_pickle/conference.dat', 'rb') as f:
    confName = pickle.load(f)

    print ' # Load pickle instead'
else:
  with open('original_data/Conference.csv', 'rb') as f:
    reader = csv.reader(f)

    with open('compressed_data/Conference.csv', 'wb') as csvOut:
      writer = csv.writer(csvOut, delimiter=',')
      writer.writerow(reader.next())

      for row in reader:
        cid  = int(row[0])
        full = row[2]

        if len(full) > 0:
          if cid in paperCid.values():
            confName[cid] = full

            writer.writerow([cid, '', full, ''])
          #else:
          #  print cid, 'is not in Paper.csv'

  with open('compressed_data/dm_pickle/conference.dat', 'wb') as f:
    pickle.dump(confName, f)
    print ' # Dump pickle'

#
print '[*] Read Journal.csv'
if os.path.isfile('compressed_data/dm_pickle/journal.dat'):
  with open('compressed_data/dm_pickle/journal.dat', 'rb') as f:
    jourName = pickle.load(f)

    print ' # Load pickle instead'
else:
  with open('original_data/Journal.csv', 'rb') as csvFile:
    reader = csv.reader(csvFile)

    with open('compressed_data/Journal.csv', 'wb') as csvOut:
      writer = csv.writer(csvOut, delimiter=',')
      writer.writerow(reader.next())

      for row in reader:
        jid  = int(row[0])
        full = row[2]

        if len(full) > 0:
          if jid in paperJid.values():
            jourName[cid] = full

            writer.writerow([jid, '', full, ''])
          #else:
          #  print jid, 'is not in Paper.csv'

  with open('compressed_data/dm_pickle/journal.dat', 'wb') as f:
    pickle.dump(jourName, f)
    print ' # Dump pickle'

#
print '[*] Read PaperAuthor.csv'
if os.path.isfile('compressed_data/dm_pickle/paperauthor.dat'):
  with open('compressed_data/dm_pickle/paperauthor.dat', 'rb') as f:
    paperAuthor = pickle.load(f)
    authorPaper = pickle.load(f)
    print ' # Load pickle instead'
else:
  with open('original_data/PaperAuthor.csv', 'rb') as csvFile:
    reader = csv.reader(csvFile)

    with open('compressed_data/PaperAuthor.csv', 'wb') as csvOut:
      writer = csv.writer(csvOut, delimiter=',')
      writer.writerow(reader.next())

      p2amap = collections.defaultdict(list)
      a2pmap = collections.defaultdict(list)

      for row in reader:
        pid = int(row[0])
        aid = int(row[1])

        if pid in paperId:
          # key = pid, value = aid
          p2amap[pid].append(aid)

          # key = aid, value = pid
          a2pmap[aid].append(pid)

          writer.writerow([pid, aid, '', ''])
        #else:
        #  print pid, 'is not in Paper.csv'
      paperAuthor = p2amap
      authorPaper = a2pmap

  with open('compressed_data/dm_pickle/paperauthor.dat', 'wb') as f:
    pickle.dump(paperAuthor,f)
    pickle.dump(authorPaper,f)

    print ' # Dump pickle'

#
"""
print '[*] Read Train.csv'
with open('original_data/Train.csv', 'rb') as csvFile:
  reader = csv.reader(csvFile)

  with open('compressed_data/Train.csv', 'wb') as csvOut:
    writer = csv.writer(csvOut, delimiter=',')
    writer.writerow(reader.next())

    for row in reader:
      aid       = int(row[0])
      confirmed = map(int, row[1].split())
      deleted   = map(int, row[2].split())

      if aid in authorPaper:
        dconfirmed = []
        ddeleted   = []

        for pid in confirmed:
          if pid in paperId:
            dconfirmed.append(pid)

        for pid in deleted:
          if pid in paperId:
            ddeleted.append(pid)

        if len(dconfirmed) > 0 and len(ddeleted) > 0:
          dconfirmed = [str(i) for i in dconfirmed]
          dconfirmed = " ".join(dconfirmed)
          ddeleted   = [str(i) for i in ddeleted]
          ddeleted   = " ".join(ddeleted)
          writer.writerow([aid, dconfirmed, ddeleted])
        else:
          print 'Lack of data of %d: len(Confirm) = %d, len(Delete) = %d' % \
                  (aid, len(dconfirmed), len(ddeleted))

      else:
        print aid, 'is not in PaperAuthor.csv'
"""

print '[*] Read Valid.csv'
with open('original_data/Valid.csv', 'rb') as csvFile:
  reader = csv.reader(csvFile)

  with open('compressed_data/Valid.csv', 'wb') as csvOut:
    writer = csv.writer(csvOut, delimiter=',')
    writer.writerow(reader.next())

    for row in reader:
      aid     = int(row[0])
      unknown = map(int, row[1].split())

      if aid in authorPaper:
        dunknown = []

        for pid in unknown:
          if pid in paperId:
            dunknown.append(pid)

        if len(dunknown) > 0:
          dunknown = [str(i) for i in dunknown]
          dunknown = " ".join(dunknown)
          writer.writerow([aid, dunknown])
        else:
          print 'Lack of data of %d: len(Unknown) = 0' % aid

      else:
        print aid, 'is not in PaperAuthor.csv'

print '[*] Read ValidSolution.csv'
with open('original_data/ValidSolution.csv', 'rb') as csvFile:
  reader = csv.reader(csvFile)

  with open('compressed_data/ValidSolution.csv', 'wb') as csvOut:
    writer = csv.writer(csvOut, delimiter=',')
    writer.writerow(reader.next())

    for row in reader:
      aid     = int(row[0])
      confirmed = map(int, row[1].split())

      if aid in authorPaper:
        dconfirmed = []

        for pid in confirmed:
          if pid in paperId:
            dconfirmed.append(pid)

        if len(dconfirmed) > 0:
          dconfirmed = [str(i) for i in dconfirmed]
          dconfirmed = " ".join(dconfirmed)
          writer.writerow([aid, dconfirmed])
        else:
          print 'Lack of data of %d: len(Confirm) = 0' % aid

      else:
        print aid, 'is not in PaperAuthor.csv'

print '[*] Read Test.csv'
with open('original_data/Test.csv', 'rb') as csvFile:
  reader = csv.reader(csvFile)

  with open('compressed_data/Test.csv', 'wb') as csvOut:
    writer = csv.writer(csvOut, delimiter=',')
    writer.writerow(reader.next())

    for row in reader:
      aid     = int(row[0])
      unknown = map(int, row[1].split())

      if aid in authorPaper:
        dunknown = []

        for pid in unknown:
          if pid in paperId:
            dunknown.append(pid)

        if len(dunknown) > 0:
          dunknown = [str(i) for i in dunknown]
          dunknown = " ".join(dunknown)
          writer.writerow([aid, dunknown])
        else:
          print 'Lack of data of %d: len(Unknown) = 0' % aid

      else:
        print aid, 'is not in PaperAuthor.csv'

print '[*] Done'
