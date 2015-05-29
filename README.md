# kddcup13

## Setting
* create 'original\_data' directory in the same directory with code
* Copy given csv files into the 'original\_data' directory
* Remove pickles to update given data ('original\_data/.\_\*.dat' files)


## Parsing
#### Author.csv
AuthorId -> Affiliation

#### Paper.csv
PaperId -> Title
PaperId -> Conference/Journal
PaperId -> Keywords

#### Conference.csv
ConferenceId -> Full Name

#### Journal.csv
JournalId -> Full Name

#### PaperAuthor.csv
PaperId -> Co-AuthorIds
AuthorId -> PublicationPaperIds


## Feature Engineering
Distance of string := 0.5 * Jaro + 0.5 * (1 / Levenshtein + 1 / Damerau\_Levenshtein)
  * NOTE: All non-alphabet is removed except space
  * NOTE: Some common words are removed
  * NOTE: Exclude empty string

Distance of counter := Euclidean distance
  * NOTE: Exclude non-common dimension (publisher)

AuthorVector := [Distance of affiliation, Distance of # of papers per conference, Distance of # of papers per journal]

PaperVector := [Distance of title, Distance of publisher name, Distance of keywords]

AuthorSimilarity = mean of AuthorVector between co-authors of given paper

PaperSimilarity = mean of PaperVector between publications of given author


## Training
#### Train.csv -> preprocess.csv
row = [AuthorId, PaperId, AuthorSimilarity, PaperSimilarity, Mark]
  * NOTE: Mark := +1 / -1

#### Train.csv + Valid.csv

## Testing
#### Train.csv  vs.  Valid.csv
#### Train.csv + Valid.csv  vs.  Test.csv
