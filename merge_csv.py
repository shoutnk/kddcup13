import csv, os, sys

title = 0

def mergeCsv(dest, target):
  global title

  with open(dest, 'ab') as csvFile:
    writer = csv.writer(csvFile)
    
    with open(target, 'rb') as csvFile:
      reader = csv.reader(csvFile)

      if title > 0:
        reader.next()
      else:
        title += 1

      for row in reader:
        writer.writerow(row)


def main():
  argc = len(sys.argv)

  if argc is 2:
    print 'Usage: csv1 csv2 merged'
    return

  mergedCsv = sys.argv[argc - 1]

  for i in range(1, argc - 1):
    mergeCsv(mergedCsv, sys.argv[i])

  print '[*] Done'

if __name__ == "__main__":
  main()
