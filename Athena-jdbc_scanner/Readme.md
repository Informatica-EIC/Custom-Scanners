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


Install and Run the Athena Scanner
----------------------------------

* download `edcAthena_Scanner_v<n_n>.zip` and unzip contents
* edit athena.properties - configuring 
    * jdbc.class
    * jdbc.url
    * user (aws access key)
    * pwd (aws secret_key) - leave empty to be prompted

To run the scanner

on linux
* `./athena.sh`  optionally pass the name of the .properties file to control the scanner default athena.properties
  - athena.sh will zip the 4 csv files into `athena_edc_custom.zip`

on windows
* `java -cp "athenaScan.jar:lib/*" com.informatica.edc.custom.AthenaScanner <athena.properties>`

CSV output files, for import into EDC:
* athenaTablesViews.csv - table & view metadata - including view sql for both (for tables it will show the s3 "location"
* columns.csv           - 1 row per column, with datatype/length/position
* otherobjects.csv      - 1 row per db/schema (they share attributes so can be in a single file)
* links.csv             - parent-child relationships between objects


Create a resource to load the metadata
--------------------------------------

* In the Catalog Admin UI, go to New > Resource
* Enter name and select resource Type as "Athena"
* Upload the zip file where prompted
* Click on next, then Save and Run.

### Sample output
-----------------

```
AthenaScanner: athena.properties currentTimeMillis=1544461478757
AthenaScanner 0.1 initializing properties from: athena.properties
password set to <prompt> for user *************** - waiting for user input...
User password:
   jdbc driver=com.simba.athena.jdbc.Driver
      jdbc url=jdbc:awsathena://AwsRegion=us-west-2;S3OutputLocation=s3://aws-athena-query-results-595425154981-us-west-2
          user=####################
           pwd=****************************************
Include/Exclude settings
        schemas include filters=none - all (not excluded) will be extracted
        schemas exclude filters=none - all will be excluded
        tables include  filters=none - all tables be extracted (in not excluded)
        tables exclude  filters=none - no tables will be excluded (except for table.include.filter settings)
Initializing output files
Initializing jdbc driver class: com.simba.athena.jdbc.Driver
establishing connection to: jdbc:awsathena://AwsRegion=us-west-2;S3OutputLocation=s3://aws-athena-query-results-595425154981-us-west-2
log4j:WARN No appenders could be found for logger (com.simba.athena.amazonaws.AmazonWebServiceClient).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Connected!
getting database metadata object (con.getMetaData())
getting catalogs:  DatabaseMetaData.getCatalogs()
catalog: AwsDataCatalog
        creating database: AwsDataCatalog
        getting schemas - using 'show databases' command
        schema=athena_db
                object: athena_db included:true
                getting view list using: 'show views in athena_db' command
                views found: [order_view]
                getting table list using: 'show tables in athena_db' command
                customer_export
                object: customer_export included:true
                        columns extracted: 10
                order_view
                object: order_view included:true
                        order_view is a view
                        columns extracted: 2
                pos_orders
                object: pos_orders included:true
                        columns extracted: 25
finished tables
        schema=default
                object: default included:true
                getting view list using: 'show views in default' command
                views found: []
                getting table list using: 'show tables in default' command
finished tables
        schema=sampledb
                object: sampledb included:true
                getting view list using: 'show views in sampledb' command
                views found: []
                getting table list using: 'show tables in sampledb' command
                elb_logs
                object: elb_logs included:true
                        columns extracted: 19
finished tables
finished schemas
closing...
closing output files
Finished

```


