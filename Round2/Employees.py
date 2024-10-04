import pysolr
import requests
import csv

def createCollection(collection_name):
    url = f'http://localhost:8983/solr/admin/collections?action=CREATE&name={collection_name}&numShards=1&replicationFactor=1'
    
    response = requests.get(url)
    
    if response.status_code == 200:
        print(f"Collection '{collection_name}' created successfully.")
    else:
        print(f"Failed to create collection '{collection_name}'. Response: {response.content}")

def indexData(collection_name, exclude_column):
    url = f'http://localhost:8983/solr/{collection_name}'
    solr = pysolr.Solr(url, always_commit=True)
    encodings_to_try = ['utf-8', 'ISO-8859-1', 'windows-1252']
    documents = []

    for encoding in encodings_to_try:
        try:
            with open('EmployeeData.csv', mode='r', encoding=encoding, errors='replace') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    documents.append(row)
            break
        except UnicodeDecodeError as e:
            print(f"Failed to read with encoding {encoding}: {e}")
        except FileNotFoundError:
            print("CSV file not found. Please check the file path.")
            return
        except Exception as e:
            print(f"An error occurred: {e}")
    
    if documents:
        try:
            solr.add(documents)
            print(f"Indexed {len(documents)} documents into {collection_name}.")
        except pysolr.SolrError as e:
            print(f"Failed to index documents in Solr: {e}")
        except Exception as e:
            print(f"Unexpected error while indexing: {e}")
    else:
        print("No documents were indexed.")

def searchByColumn(collection_name, column_name, column_value):
    url = f'http://localhost:8983/solr/{collection_name}'
    solr = pysolr.Solr(url, always_commit=True)
    query = f'{column_name}:"{column_value}"'
    
    try:
        results = solr.search(query)
        return results
    except pysolr.SolrError as e:
        print(f"Solr Error: {e}")

def getEmpCount(collection_name):
    url = f'http://localhost:8983/solr/{collection_name}'
    solr = pysolr.Solr(url, always_commit=True)
    results = solr.search('*:*', **{'rows': 0})
    count = results.hits
    print(f"Total employees in '{collection_name}': {count}")
    return count

def delEmpById(collection_name, employee_id):
    url = f'http://localhost:8983/solr/{collection_name}'
    solr = pysolr.Solr(url, always_commit=True)
    solr.delete(id=employee_id)
    print(f"Deleted employee with ID: {employee_id}")

def getDepFacet(collection_name):
    url = f'http://localhost:8983/solr/{collection_name}'
    solr = pysolr.Solr(url, always_commit=True)
    try:
        facet_query = solr.search('*:*', **{
            'facet': 'true',
            'facet.field': 'Department',
            'rows': 0
        })
        print(f"Facet response: {facet_query}")
    except pysolr.SolrError as e:
        print(f"Solr responded with an error (HTTP 400): {e}")
    except Exception as e:
        print(f"Unexpected error while getting facets: {e}")

v_nameCollection = 'Hash_AshokKumar'
v_phoneCollection = 'Hash_8843'
print("\n\nCollection Creation")
createCollection(v_nameCollection)
createCollection(v_phoneCollection)
print("\n\nIndexing Data into collections")
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
print("\n\nEmployee Count of Hash_AshokKumar")
getEmpCount(v_nameCollection)
print("\n\nDelete E02003 from Hash_AshokKumar")
delEmpById(v_nameCollection, 'E02003')


print("\n\nSearch Department column in Hash_AshokKumar")

results = searchByColumn(v_nameCollection, 'Department', 'IT')
if results:
    print(f"Search results in '{v_nameCollection}' for Department 'IT': {len(results)} found.")
    for result in results:
        print(result)

print("\n\nSearch Gender column in Hash_AshokKumar")

results = searchByColumn(v_nameCollection, 'Gender', 'Male')
if results:
    print(f"Search results in '{v_nameCollection}' for Gender 'Male': {len(results)} found.")
    for result in results:
        print(result)

print("\n\nSearch Department column in Hash_8843")

results = searchByColumn(v_phoneCollection, 'Department', 'IT')
if results:
    print(f"Search results in '{v_phoneCollection}' for Department 'IT': {len(results)} found.")
    for result in results:
        print(result)

print("\n\nGetting Department Facet of collections")

getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
