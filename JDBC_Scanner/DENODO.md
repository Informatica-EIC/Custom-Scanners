# DENODO VDP custom scanner for Enterprise Data Catalog

This document describes how to setup/use the Denodo Virtual Data Port Scanner.

The scanner is based on the standard relational database model `com.infa.ldm.relational`

Denodo is a data virtualization product and can be accessed via a JDBC Driver.  Since denodo acts a little differently to standard relational databases, we cannot use the generic JDBC scanner for Denodo.

In Denodo, there is no concept of a schema - only Databases, Tables and Columns.  To represent these properly in the catalog, *AND* provide the right structure for linking, we need to store both database and schema objects.
 

<img src="denodo_model.svg">

Since Denodo is a virtual layer - we also need to generate the lineage links back to the orignial data sources.

<img src="denodo_scanner_overview.png">



### Capabilities

* can scan multiple denodo databases (these are actually more like schemas for other databsae types)
* will extract lineage between views within denodo, at both the view & column level
* expression fields are processed - lineage should be created to all referenced source fields
    * the expression logic is stored in the "View Statement" system attribute `com.infa.ldm.relational.ViewStatement`
* for relational sources - custom lineage is generated to link source tables/views to the actual dbms tables/views via connection assignment




### Configuring the Denodo Scanner

1 - (one time) create a resource-type for Denodo
  * from ldmadmin ui - select Manage | Custom Resource Types
  * click + to create a new Custom Resource type (if not already created)
  * Name=DenodoScanner  (changing the name is ok - this name)
  * Model=com.infa.ldm.relational  (browse to select only this model)
  * Connection Types=Database (for linking, no need to select schema)

2 - edit (or copy/clone) denodo.properties - used to control the scanner process

	```properties
	driverClass=com.denodo.vdp.jdbc.Driver
	URL=jdbc:vdb://[denodo host]:[denodo vdp port]/[denodo database]
	user=<user id>
	pwd=<password>
	catalog=<list of databases to extract - comma seperated>
	
	# environment settings
	# customMetadta.folder  - location/folder where custom scanner output (&.zip) file(s) are created
	customMetadata.folder=denodo_custom_metadata_out
	
	# denodo specific settings - name of the database object to create
	denodo.databaseName=denodo_vdp
	```   

3 - run the scanner 

  * `scanDenodo.sh <propertyFile>`
  * output will be written to the folder referenced in denodo.properties (setting: `customMetadata.folder`) and will be named `denodoScanner.zip`
  * if the folder does not exist, it will be created (assuming the user has permissions to do so)
  * custom lineage will be exported to folder referenced in denodo.properties (setting: `<customMetadata.folder>_lineage/denodo_lineage.csv`)
 
  
 
4 - create a denodo resource
  * create a Denodo resource, using the resource type from Step 1
  * select the denodoScanner.zip file created by the denodo scanner (step 3 above)
  * save and run




### Running the Denodo Scanner

`scanDenodo.sh <propertyFile>`

scanner log/output is written to stdout (no specific file logging currently) - to pipe results to file

`scanDenodo.sh <propertyFile> > denodo_scan.log`

Note:  when starting the scanner - the following disclaimer will be displayed:

```
************************************ Disclaimer *************************************
By using this custom scanner, you are agreeing to the following:-
- this custom scanner is not officially supported by Informatica
  it was created for situations where the generic JDBC scanner
  does not produce the correct results of fails to extract any metadata.
- It has only been tested with limited test/cases, and may report exceptions or fail.
- Issues can be created on githib:- 
  https://github.com/Informatica-EIC/Custom-Scanners  (JDBC_Scanner folder)
*************************************************************************************
```

the user will be prompted to agree to this disclaimer each time the scanner is run, or by passing "agreeToDisclaimer" as the 2nd command-line parameter.  (not case sensitive)



## Design Notes

<img src="denodo_model.svg">

* extends the Generic JDBC Custom scanner for denodo specific functions
   * reads denodo.databaseName setting from the .properties file - default value if not provided is "denodo_vdp"
   * initFiles() - creates a custom lineage file in `<customMetadata.folder>_lineage` 
   * getCatalogs() - uses `denodo.databaseName` as the Database object in the catalog (shifts the actual catalog name as the Schema name)
   * getSchemas() - calls `dbMetaData.getCatalogs()` as Denodo has no concept of schema - but the catalog requires it
   * getTables() - for each table, also generates custom lineage (extracts the connection name from the table 'wrapper')
   * getViews() - calls the `GET_VIEWS()` procedure (does not use standard jdbc get tables)
   * other calls made
      * for each source table (to get the source connection name for lineage)  `DESC VQL WRAPPER JDBC <database>.<table>`
      * extracts column dependencies for views using the procedure - `COLUMN_DEPENDENCIES()`  



# Disclaimer/Limitations

* profiling will not be possible (yet, until it is supported with custom scans)
* custom lineage for tables is only possible for JDBC connections (need test cases for other types)
* calculated fields are not currently processed (for lineage) - this is a work-in-progress, to parse the expression logic and link to the correct fields
* this is not an official informatica supported product - but any issues can be logged on github
* custom lineage to non-relational resources has not been attempted


# Example

The following example shows how objects stored in Denodo is represented after scanning into the catalog.

this example reads data from mySQL, joins several tables and calculates a single value (concatenating to fields), using a denodo database named "sakila_vdb" - this is imported into the catalog as a schema with the same name.

<img src="denodo_sakila_vdb_tree.png">

The view `v_customer_list` is represented in denodo like this (either lineage or tree view)

<img src="denodo_view_v_customer_list_diag.png">

and after scanning into the catalog - the lineage looks like this (internal to denodo only, without scanning the mysql database)

Note:  the schema name is not included in these lineage diagrams

<img src="edc_lineage_v_customer_list_summary.png">

the System Attribute:  Location `com.infa.ldm.relational.Location` is used to store the folder location for the object in denodo.  in this example the folder `/3 app views` is used as the location for `v_customer_list`

the `full_name` field for the `v_customer_list` view is calculated using the following expression:-

<img src="denodo_full_name_calculated_field.png">

after scanning into EDC - lineage for full_name shows the 2 fields that are used for the expression/calculation.

<img src="edc_full_name_lineage.png">

and - the expression used is stored in the View Statement attribute `com.infa.ldm.relational.ViewStatement`

<img src="edc_full_name_system_attributes.png">

an extract of the custom lineage file generated by the scanner - shows that connection assignments are used - reflecting the denoeo environment & objects referenced

<img src="edc_denodo_custom_lineage.png">

after importing (either using automatic or manual connection assignment - and ensuring the connections point to the right place) the lineage should be complete.


<img src="denodo_custom_lineage_cnx_assignment.png">

and the lineage now shows a connection back to the mysql database structures - note the customer table in mysql was re-labeled to sakila_customer.

<img src="edc_lineage_v_customer_list_with_customlineage.png">


Example 2:  multiple steps in Denodo


<img src="denodo_client_with_bills.png">

and after importing into EDC - also showing that `amount_due_by_client` is also connected downstream

<img src="edc_client_with_bills.png">













