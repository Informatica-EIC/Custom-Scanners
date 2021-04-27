Version History/Changes for Denodo Scanner
------------------------------------------
2021/04/27 - v0.9.8.8
- bugfix - connection assignment for SAP Hana calculation views was wrong
           see issue #36 https://github.com/Informatica-EIC/Custom-Scanners/issues/36
           when a schema _SYS_BIC is used - it will be removed & the package references will have any . package seperators replaced with a /
- experimental feature
  the.properties you can add a  `view_select_filter` with a single expression, to filter tables/views when querying denodo
  example:-
  view_select_filter=%claim%


2021/04/22 - v0.9.8.7
- new feature - table/view include and exclude filters
    include.datasets=comma seperate list of wildcards for objects to include (<databsae>.<table|view>)
    exclude.datasets=comma seperate list of wildcards for objects to exclude (<databsae>.<table|view>)

    exclude takes precedence (e.g. if an object is both included and excluded, it will not be scanned)

    default (no values set) - all objects are included, no objects are excluded

    example:-
    # - include only datasets with acct anywhere in the name, for all databases or start with cust (in any database)
    include.datasets=*.*acct,*.cust*
    # - exclude any datasets with test in the name
    exclude.datasets=*.*test*

    expressions are converted to regex and are compared when extracting tables and views

    output folder will have 2 new files:-
    - excluded_objects.csv - list of all excluded objects & the filter type (excluded|not included)
    - missing_objects.csv - list of the objects that are referenced by scanned objects, but were excluded|not included

    setting ep.skipobjects is now mostly obsolete, but still included for backwards compatibility



2021/04/21 - v0.9.8.6
- bugfix ep.skipobjects was case sensitive, switched to case insensitive
- changed csv writer to use BufferedWriter class
- new feature - ep.skipobjects now applies to table/view extractor (was only for lineage processing)
- new feature - encryptedPwd setting added to .properties file
                to encrypt a password:-
                scanDenodo.cmd|sh password
                you will be prompted to enter a password & the encrpted value is printed
                set encryptedPwd=<encryped password>
- default jdbc driver version for Denodo is now v8  (was v7)
  remove/replace lib/denodo-vdp-jdbcdriver-full-8.0.0-SNAPSHOT.jar, with the version that matches your denodo installation
- added placeholders in scanner start scripts (.cmd and .sh) for SSL connections to denodo

2019/11/29 - v0.9.8.5
- bugfix java.lang.NullPointerException when wrapper/datasource objects fails (permission related)
  Note:  this fixes the NullPointerException - but there will be no external lineage for any object that we cannot execute a desc vql statement on
         see here:  https://community.denodo.com/docs/html/browse/7.0/vdp/administration/databases_users_and_access_rights_in_virtual_dataport/user_and_access_right_in_virtual_dataport/user_and_access_right_in_virtual_dataport#write-privilege


2019/10/25 - v0.9.8.4
- bugfix #32 NullPointerException after error in extractWrapper
- location property is now written for Tables with the denodo folder location (missing before), was only stored for views before

2019/10/18 - v0.9.8.3
- bugfix #28 - gracefully capture the following error:
					java.sql.SQLException: The user does not have WRITE privileges on ...
               modified the code to catch these errors and print an error summary (no stack trace) to the console (& debug log)

               Note 1:  this should not affect any relational (JDBC/ODBC) datasources.  however for datafile (DF) datasources
                        and
                        it means that lineage back to the filesystem objects will be missing
               Note 2:  this is a restriction with Denodo (so not a scanner bug)
                        where WRITE privileges are required to view the VQL for
                        Data sources, derived views, base views, stored procedures, JMS listeners, web services and widgets
                        https://community.denodo.com/docs/html/browse/7.0/vdp/administration/databases_users_and_access_rights_in_virtual_dataport/user_and_access_right_in_virtual_dataport/user_and_access_right_in_virtual_dataport


2019/10/10 - v0.9.8.2
- bugfix #26 - views with a + character in the name were not processed properly
- bugfix #25 - datasource and wrapper objects with spaces in the name were not stored properly, lineage would be missing for views based on these objects
               regex patterns are used to extract the datasource & wrapper names
- bugfix #24 - extracting the vql (create statement) for some views failed to find the end of a view definitions ";" character.  previous versions were looking for );\n
               the fixed version only looks for ;\n
- new feature - denodo database to database (schemas in the EDC relational model) are now linked via core.DataSetDataFlow relationship
                Note:  recursive relationships (where views are based on other views/tables in the same database) are not stored (can clutter the schema lineage diagram)


2019/08/08 - v0.9.8.1
- bugfix - lineage.csv had single quotes for columns in some situations

2019/08/05 - v0.9.8
- custom for delimited files (wrapper type=DF) created
- sql over-rides for base views (tables) is captured using com.infa.ldm.relational.ViewStatement
  lineage is not possible for these tables (see known limitations)
  the scanner log (console) will print the count of "tables with sql statements:<n>"
  and list each entry <db>.<basetable>
  to extract lineage a sql parser would be required
- datasource metadata is now collected during a 1st pass for each denodo database
  this is used to extract custom lineage for non JDBC/ODBC sources
- custom lineage for jdbc - now using correct column name from wrapper definition
  (was assuming column names were the same as what is used in the denodo base view)
- view sql statements were including all dependent views and datasource/wrappers
  only the definition for the view is stored now - reducing any truncation messages during import


Known Limitations:
1.	custom lineage is not possible for base views using SQL statements


2019/07/22 - v0.9.7
- all object .csv files are now prefixed objects-*.csv (needed for 10.2.2hf1+)
- changes for v10.2.2hf1 and later (for older versions, processing remains the same)
  - added new flag in .properties file "include.custlineage" - with a default of false
    when set to true - lineage will be generated in a seperate folder, lineage.csv will
    be included in the scanner output
  - when importing into EDC, using v10.2.2hf1+ - set the following:-
    ETL Resource=Yes  (etl really means the scanner has external lineage in this case)
    Auto Assign Connections - can be on or off, same as for any custom lineage resource
  - connections naming convention <denodo_database>.<denodo_connection_name>:<wrappertype>

2019/07/08 - v0.9.6
- re-factored view lineage processing column level
	- previous versions processed each column individually,
	  for some larger databases that hava a lot of joins, these
	  querys could take up to 4 seconds for each column
	- by re-factoring, we can get the same result by extracting
	  lineage for all columns in a view (vs 1 at a time).
	  For a large database, this reduced the scan time from 1h:20 to 9 mins
	- console output is more compact
- re-factored view lineage processing view level
	- older versions created some wrong links for interface views
	  (links to the objects linked from the source of the interface)
- fixed Issues(github)
  https://github.com/Informatica-EIC/Custom-Scanners/issues
	- #8  - denodo: tables/views with $ character in the name fail for jdbc getColumns
	- #9  - denodo: tables with mixed case names - extract wrapper fails
	- #10 - denodo: GET_SOURCE_TABLE [STORED_PROCEDURE] [ERROR] for non jdbc wrappers
	- #12 - denodo: scanner raises errors when databases have mixed case names

- known issues / enhancements requested
	- #13 - denodo: custom lineage for flatfiles is not generated
			for delimited files, we should be able to generate lineage
			all other datasource types (Except for JDBC) will not have any
			custom lineage generated
			for any non-supported datasource types - you will see a message in the
			scanner console/log
				wrapper type:{type} not yet supported



