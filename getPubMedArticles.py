# -*- coding: utf-8 -*-

"""
download abstracts from PubMed and save to tsv
warning: you can get 10,000 records once
Trick: convert generator to list could prevent unexpeted error (e.g. disconnection)
"""

from Bio import Entrez, Medline
import csv
from time import time
from math import ceil


def getArticles(id_list, csv_writer):
    # get medline records based on ids
    # record is a generator
    # now we can not close this handle, or it return none
    handle = Entrez.efetch(db="pubmed", id=id_list, rettype="medline", retmode="text")
    record = list(Medline.parse(handle))
    i = 0
    for row in record:
        i += 1
        csv_writer.writerow([row.get("PMID", None), row.get("AB", None)])
    return i


# main function
start = time()
partition = 5000

# Always tell NCBI who you are
Entrez.email = "hill103@foxmail.com" 
query_term = "Congenital Abnormalities[Majr] AND (Case Reports[PT] OR case report[All Fields])"

# first count the number of related articles
handle = Entrez.egquery(term=query_term)
record = Entrez.read(handle)
handle.close()
for row in record["eGQueryResult"]:
    if row["DbName"] == "pubmed":
        count = int(row["Count"])
        print "records find in %s: %d." % (row["DbName"], count)
        break

# second get id (default of the number is set to 20, so we need first to get the count)
handle = Entrez.esearch(db="pubmed", term=query_term, retmax=count)
record = Entrez.read(handle)
handle.close()
id_list = record["IdList"]

# last get medline records based on ids
# split id_list into 10,000 within a subset
print "starting to fetch articles..."
parts = int(ceil(count / float(partition)))
record_count = 0
# save to a tsv file
with open("articles.tsv", "wb") as f:
    writer = csv.writer(f, delimiter="\t")
    writer.writerow(["pubmed_id", "abstract"])
    for i in range(parts):
        record_count += getArticles(id_list[i*partition:(i+1)*partition], writer)
        print "Progress: %.0f%%. %d records have been written to file." % ((i+1)/float(parts)*100, record_count)

print "articles fetching finished. Elapsed time: %.2f hours." % ((time()-start)/3600.0)
