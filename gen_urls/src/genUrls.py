import itertools
import sys
import numpy as np
import time

from google.cloud import bigquery

URL_PREFIX="https://www.bcn-advisors.com/propiedad?"

class UrlFacet:
    def __init__(self, field, keywordValues, nullable=True):
        self.field = field
        self.keywordValues = keywordValues
        if nullable:
            self.keywordValues[""] = ""
    def genUrlParameters(self):
        for value in self.keywordValues.values():
            yield self.field + "=" + value
    def genKeywords(self):
        for keyword in self.keywordValues:
            yield keyword
    def genUrlParametersAndKeywords(self):
        for keyword, value in self.keywordValues.items():
            yield (self.field + "=" + value if value else "", keyword)

def urlFromFacets(facets):
    params = []
    keywords = []
    for param, keyword in facets:
        params.append(param)
        keywords.append(keyword)
    # clean null params
    params = [p for p in params if p]
    keywords = [k for k in keywords if k]
    # sort to get always the same url for a given set of params
    params.sort()
    return (" ".join(keywords), URL_PREFIX + "&".join(params))

def main():
    #### bquery inputs
    # Instantiates a client
    client = bigquery.Client()

    # The name for the new dataset
    dataset_id = "adwords_project_data_input"

    # Prepares a reference to the new dataset
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)

    table_ref = dataset.table("facets_input_federated")
    table = bigquery.Table(table_ref)
    input_rows = client.list_rows(client.get_table(table))

    input_rows_map = {}
    input_rows_list = []
    for r in input_rows:
        input_rows_list.append(r)

        if not input_rows_map.has_key(r["facet"]):
            # new facet with a tuple "(field_map, is_nullable)"
            field_map = { r["field"]: r["url_value"] }
            is_nullable = r["is_nullable"]
            input_rows_map[r["facet"]] = (field_map, is_nullable)
        else:
            field_map = input_rows_map[r["facet"]][0]
            field_map[r["field"]] = r["url_value"]
    ####

    facets = []
    for k in input_rows_map.keys():
        facet_name = k
        fields = input_rows_map[k][0]
        is_nullable = input_rows_map[k][1]
        facets.append(
            UrlFacet(facet_name, fields, is_nullable)
        )

    #facets = [
    #    UrlFacet("zone", {"esplugas" : "65","pedralbes" : "66","sarria" : "66","sant gervasi" : "66","tres torres" : "66","gracia" : "66","eixample" : "67","eixample derecho" : "67","eixample izquierdo" : "67","ciutat vella" : "68","born" : "68","gotico" : "68","raval" : "68","poble nou" : "69","diagonal mar" : "69","les corts" : "70","sants" : "70","maresme" : "72","badalona" : "72","castelldefels" : "73","sitges" : "73","sant cugat" : "74","valles " : "74","mallorca" : "2852"}, False),
    #    UrlFacet("type", {"vivienda" : "housing","obra nueva" : "new buildings","edificios" : "buildings","oficina" : "offices"}),
    #    UrlFacet("operation", {"venta" : "sale","alquiler" : "rent"}, False),
    #    UrlFacet("field_icons_value", {"sin muebles" : "no","con muebles" : "yes"}),
    #    UrlFacet("bedroom_number", {"1 habitacion" : "1", "2 habitaciones" : "2","3 habitaciones" : "3","4 habitaciones" : "4","5 o mas habitaciones" : "5"})]

    kws_with_url = []
    for urlTup in itertools.product(*map(lambda facet: facet.genUrlParametersAndKeywords(), facets)):
        for perm in itertools.permutations(urlTup):
            kws_with_url.append(urlFromFacets(perm))

    # multiply per match type
    kws_with_url_and_match_type = []
    for kwd in kws_with_url:
      kws_with_url_and_match_type.append((kwd[0], kwd[1], "broad"))
      kws_with_url_and_match_type.append((kwd[0], kwd[1], "phrase"))
      kws_with_url_and_match_type.append((kwd[0], kwd[1], "exact"))


    #### Insert to bquery
    table_ref = dataset.table("keywords")
    table = bigquery.Table(table_ref)
    table = client.get_table(table)
    num_kws_to_upload = len(kws_with_url_and_match_type)
    chunk_size = 10000
    chunks_to_upload = (kws_with_url_and_match_type[i : i + chunk_size] for i in range(0, num_kws_to_upload, chunk_size))
    num_chunks = int((len(kws_with_url_and_match_type) + chunk_size - 1) / chunk_size)
    num_chunks_uploaded = 0
    print("About to upload " + str(num_kws_to_upload) + " kws in " + str(num_chunks) + " chunks")
    for chunk_to_upload in chunks_to_upload:
        print "Uploading chunk " + str(num_chunks_uploaded + 1) + "/" + str(num_chunks),
        iter_start_time = time.time()
        client.insert_rows(table, chunk_to_upload)
        iter_end_time = time.time()
        num_chunks_uploaded += 1
        print "[ "+ str(iter_end_time - iter_start_time) + " sec ]"
    ####

if __name__ == "__main__":
    main()
