# Athena scanner via JDBC

### Why the need to create an Athena Scanner

the generic JDBC scanner uses standard JDBC calls to get lists of:-
  * Catalogs (database)
  * Schemas (in athena terms database & schema is the same thing)
  * Tables
  * Columns
  
the problem with the athena jdbc implementation is that the GetDatabaseMetadata.getTables() call returns nothing. so the generic JDBC scanner will not import anything.

in Athena - the way to get a list of tables is to use the following:-

`show tables in <database>;`

this scanner will use standard JDBC calls for db/schema/columns - but also use the show tables in <database> and show views in <database>
to extract the metadata  for an Each athena database.


## Disclaimer

* because this is a 'custom' scanner - no profiling will be possible
* exhaustive tests against all types of athena structures have not been completed - errors may be raised during the scan (once found, these can be fixed)
* view metadata will be extracted, but view sql will not be parsed - lineage will not be available for views (need to validate whether views are even used)
* complex types (arrays/structs) are un-tested - the should be imported but not flattened
* lineage to S3 folder(files) is not yet implemented (via custom lineage & connection assignment)


# EDC configuration

Note:  no new model needs to be created - the standard relational model will be used

Create a custom resource type
-----------------------------

* In the Catalog Admin UI, go to Manage > Custom Resource Types
* On the left panel, click on the + sign
* Enter "Athena" as Name, select `com.infa.ldm.relational` as the model 
* select `com.infa.ldm.relational.Database` and `com.infa.ldm.relational.Schema` as Connection Types


Generate CSV files from Athena
------------------------------------

execute the following command: 

java -jar <PATH_TO_JAR>\athenaScan.jar <athena.properties>

athena.propeties contains the following:-
* jdbc.class
* jdbc.url
* aws.key
* aws.secret_key - leave empty to be prompted

this will generate athena.zip, containg:
* athenaTablesViews.csv - table & view metadata - including view sql for both (for tables it will show the s3 "location"
* columns.csv          - 1 row per column, with datatype/length/position
* otherobjects.csv     - 1 row per db/schema (they share attributes so can be in a single file)
* links.csv            - parent-child relationships between objects


Create a resource to load the metadata
--------------------------------------

* In the Catalog Admin UI, go to New > Resource
* Enter name and select resource Type as "Athena"
* Upload the zip file where prompted
* Click on next, then Save and Run.




