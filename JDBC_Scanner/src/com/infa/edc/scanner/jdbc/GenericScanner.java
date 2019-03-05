/**
 * 
 */
package com.infa.edc.scanner.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import com.opencsv.CSVWriter;



/**
 * @author Administrator
 *
 */
public class GenericScanner implements IJdbcScanner {
	public static final String version="1.0";
	public String propertyFileName;
	public String driverClass;
	public String dbURL;
	public String userName;
	public String pwd;
	public String catalogFilter;

	public Connection connection;
	public DatabaseMetaData dbMetaData;

	protected String customMetadataFolder;


	// constants
	protected String DB_TYPE="com.infa.ldm.relational.Database";
	protected String SCH_TYPE="com.infa.ldm.relational.Schema";
	protected String TAB_TYPE="com.infa.ldm.relational.Table";
	protected String COL_TYPE="com.infa.ldm.relational.Column";
	protected String VIEW_TYPE="com.infa.ldm.relational.View";
	protected String VIEWCOL_TYPE="com.infa.ldm.relational.ViewColumn";

	protected String CATALOG_SCHEMA_FILENAME="catalogAndSchemas.csv";
	protected String TABLEVIEWS_FILENAME="tablesViews.csv";
	protected String VIEWS_FILENAME="views.csv";
	protected String COLUMN_FILENAME="columns.csv";
	protected String VCOLUMN_FILENAME="viewColumns.csv";
	protected String LINKS_FILENAME="links.csv";


	// file variables
	protected CSVWriter otherObjWriter = null; 
	protected CSVWriter tableWriter = null; 
	protected CSVWriter viewWriter = null; 
	protected CSVWriter columnWriter = null; 
	protected CSVWriter viewColumnWriter = null; 
	protected CSVWriter linksWriter = null; 




	/**
	 * read the property file to get db connection settings
	 */
	public GenericScanner(String propertyFile) {
		System.out.println(this.getClass().getSimpleName() + " " + version +  " initializing properties from: " + propertyFile);

		// store the property file
		propertyFileName = propertyFile;

		try {
			File file = new File(propertyFile);
			FileInputStream fileInput = new FileInputStream(file);
			Properties prop;
			prop = new Properties();
			prop.load(fileInput);
			fileInput.close();

			driverClass = prop.getProperty("driverClass");
			dbURL = prop.getProperty("URL");

			userName = prop.getProperty("user");
			pwd = prop.getProperty("pwd");
			if (pwd.equals("<prompt>")) {
				System.out.println("password set to <prompt> for user " + userName  + " - waiting for user input...");
				//				pwd = APIUtils.getPassword();
				//				System.out.println("pwd chars entered (debug):  " + pwd.length());
			}

			customMetadataFolder = prop.getProperty("customMetadata.folder", "custom_metadata_out");
			if (customMetadataFolder == null || customMetadataFolder.equals("")) {
				System.out.println("empty value set for custom metadata output folder: using 'custom_metadata_out'");
				customMetadataFolder = "custom_metadata_out";
			}

			catalogFilter = prop.getProperty("catalog", "");

			System.out.println("scanner settings from:" + propertyFile);
			System.out.println("\tdriver=" + driverClass);
			System.out.println("\turl=" + dbURL);
			System.out.println("\tuser=" + userName);
			System.out.println("\tpwd=" + pwd.replaceAll(".", "*"));
			System.out.println("\tout folder=" + customMetadataFolder);
			System.out.println("\tcatalog filter=" + catalogFilter);



		} catch(Exception e) {
			System.out.println("error reading properties file: " + propertyFile);
			e.printStackTrace();
		}

	}

	/* (non-Javadoc)
	 * @see com.infa.edc.scanner.jdbc.IJdbcScanner#getConnection(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Connection getConnection(String classType, String url, String user, String pwd) {
		// TODO Auto-generated method stub
		System.out.println("Step 1: validating jdbc driver class: " + classType + " using:" + this.getClass().getName());
		try {
			Class.forName(classType);
		} catch (ClassNotFoundException e) {
			System.out.println("\tunable to find class: " + classType + " " + e.getClass().getName() + " exiting...");
			return null;
		}  
		System.out.println("\tjdbc deiver class validated successfully!");

		// valid driver class - now try the actual connection
		System.out.println("Step 2: Attempting to connect to database using url=" + url + " using: "  + this.getClass().getName());
		try {
			Connection con=DriverManager.getConnection(  
					url, user, pwd);
			// connection successful - return the connection object
			System.out.println("\tconnection successful!");
			return con;
		} catch (SQLException e) {
			System.out.println("connection failed for url=" + url + " " + e.getClass().getName() + "");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  

		// something is wrong return null connection
		return null;
	}

	/**
	 * start the scan process
	 */
	public void run() {
		System.out.println(this.getClass().getName() + ".run() starting");
		connection = getConnection(driverClass, dbURL, userName, pwd);
		if (connection == null) {
			System.out.println("\t"+ this.getClass().getName() +  " - no connection - exiting...");
			return;
		} else {
			// we have a connection - continue...
			initFiles();

			System.out.println("\t" + this.getClass().getName() + " ready to start extracting databse metadata!");

			System.out.println("Step 4: getting databaseMetadata object from connection");
			try {
				dbMetaData = connection.getMetaData();
				String allV = dbMetaData.getDatabaseProductVersion();
				System.out.println("\tgetDatabaseProductVersion()="+allV);

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("\terror getting DatabaseMetaData object from connection - exiting");
				e.printStackTrace();
				return;
			}

			getCatalogs();
			
			
			extraProcessing();


			// after all proccess are finished- close the csv files
			closeFiles();
		}

	}

	/**
	 * iterate over all catalogs (databases) there may be multiple
	 * need to determine whether to default to extracting all, or only a subset
	 */
	public void getCatalogs() {
		System.out.println("Step 5: getting catalogs:  DatabaseMetaData.getCatalogs()");

		ResultSet catalogs;
		try {
			catalogs = dbMetaData.getCatalogs();
			String catalogName;
			while (catalogs.next()) {
				catalogName = catalogs.getString(1);  //"TABLE_CATALOG"
				System.out.println("\tcatalog: " + catalogName);

				// create the catalog object
				if (exportCatalog(catalogName)) {
					this.createDatabase(catalogName);

					// get schemas
					getSchemas(catalogName);
				} else {
					// message for catalog is not exported...
					System.out.println("\tcatalog=" + catalogName + " skipped - not included in catalog filter: " + catalogFilter);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * determine if the catalog should be exported or not - depends on filtering conditions
	 * e.g. if catalog=<name>,<name> - then filter in from this list
	 * e.g. if catalog=<name>,!<name> - then filter out any with !
	 * 
	 * @param catalogName
	 * @return
	 */
	protected boolean exportCatalog(String catalogName) {
		// default to all
		if (catalogFilter.equals("")) {
			// no filtering - extract them all...
			return true;
		}
		if (catalogFilter.contains(catalogName)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * get the schemas for a catalog
	 * @param catalogName
	 */
	public void getSchemas(String catalogName) {
		try {
			System.out.println("Step 5: extracting schemas for catalog: " + catalogName);
			ResultSet schemas = dbMetaData.getSchemas(catalogName, null);
			int schemaCount=0;
			while(schemas.next()) {
				schemaCount++;
				String schemaName = schemas.getString("TABLE_SCHEM");
				System.out.println("\tschema is: " + schemaName);

				createSchema(catalogName, schemaName);

				// process tables
				getTables(catalogName, schemaName);


				// process views
				getViews(catalogName, schemaName);
			}
			System.out.println("\tSchemas extracted: " + schemaCount);


		} catch (Exception ex) {
			System.out.println("Error getting list of databases using: getSchemas. " + ex.getMessage());
		}
	}

	/** 
	 * find all table objects
	 * @param catalogName
	 * @param schemaName
	 */
	protected void getTables(String catalogName, String schemaName) {
		try {
			ResultSet rsTables = dbMetaData.getTables(catalogName, schemaName, null, new String[] { "TABLE" });
			int tableCount = 0;
			while (rsTables.next()) {
				// Print
				tableCount++;

				// System.out.println("found one...");
				System.out.println("\t" + " catalog=" + rsTables.getString("TABLE_CAT") + " schema="
						+ rsTables.getString("TABLE_SCHEM") + " tablename=" + rsTables.getString("TABLE_NAME")
						+ " TABLE_TYPE=" + rsTables.getString("TABLE_TYPE")
						+ " comments=" + rsTables.getClob("REMARKS")
						);
				//				System.out.println(rsTables.getMetaData().getColumnTypeName(5));
				this.createTable(catalogName, schemaName, rsTables.getString("TABLE_NAME"), rsTables.getString("REMARKS"));

				getColumnsForTable(catalogName, schemaName, rsTables.getString("TABLE_NAME"), false);
			}
			
			System.out.println("\tTables extracted: " + tableCount);
			

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	/** 
	 * find all table objects
	 * @param catalogName
	 * @param schemaName
	 */
	protected void getViews(String catalogName, String schemaName) {
		try {
			ResultSet rsViews = dbMetaData.getTables(catalogName, schemaName, null, new String[] { "VIEW" });
			int viewCount = 0;
			while (rsViews.next()) {
				// Print
				viewCount++;

				// System.out.println("found one...");
				System.out.println("\t" + " catalog=" + rsViews.getString("TABLE_CAT") + " schema="
						+ rsViews.getString("TABLE_SCHEM") + " tablename=" + rsViews.getString("TABLE_NAME")
						+ " TABLE_TYPE=" + rsViews.getString("TABLE_TYPE")
						+ " comments=" + rsViews.getClob("REMARKS")
						);
				//				System.out.println(rsTables.getMetaData().getColumnTypeName(5));
				this.createView(catalogName, schemaName, rsViews.getString("TABLE_NAME"), rsViews.getString("REMARKS"), "");

				getColumnsForTable(catalogName, schemaName, rsViews.getString("TABLE_NAME"), true);
			}
			System.out.println("\tViews extracted: " + viewCount);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}



	protected void getColumnsForTable(String catalogName, String schemaName, String tableName, boolean isView) {
		int colCount=0;
		try {
			ResultSet columns = dbMetaData.getColumns(catalogName, schemaName, tableName, null);
			while(columns.next()) {
				colCount++;
				String columnName = columns.getString("COLUMN_NAME");
				//					                String datatype = columns.getString("DATA_TYPE");
				String typeName = columns.getString("TYPE_NAME");
				String columnsize = columns.getString("COLUMN_SIZE");
				String decimaldigits = columns.getString("DECIMAL_DIGITS");
				String isNullable = columns.getString("IS_NULLABLE");
				String remarks = columns.getString("REMARKS");
				String def = columns.getString("COLUMN_DEF");
				String sqlType = columns.getString("SQL_DATA_TYPE");
				String pos = columns.getString("ORDINAL_POSITION");
				//                String scTable = columns.getString("SCOPE_TABLE");
				//                String scCatlg = columns.getString("SCOPE_CATALOG");

//				System.out.println("\t\t\tcolumnn=" + catalogName + "/" + schemaName + "/" + tableName+ "/" + columnName+ "/" + typeName+ "/" + columnsize+ "/" + pos);

				//        		createColumn( );
				this.createColumn(catalogName, schemaName, tableName, columnName, typeName, typeName, pos, isView);

			}  // end for each column
		} catch (Exception ex) {
			System.out.println("error extracting column metadata...");
			ex.printStackTrace();
		}
		System.out.println("\t\t\tcolumns extracted: " + colCount);

	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length==0) {
			System.out.println("JDBC Custom scanner for EDC: missing configuration properties file: usage:  genericScanner <folder>/<config file>.properties");	
		} else {
			System.out.println("JDBC Custom scanner: " + args[0] + " currentTimeMillis=" +System.currentTimeMillis());
			GenericScanner scanner = new GenericScanner(args[0]);

			scanner.run();
		}

	}

	protected boolean initFiles() {
		// assume working, until it is not
		boolean initialized=true;
		System.out.println("Step 3: initializing files in: " + customMetadataFolder);

		try { 
			// check that the folder exists - if not, create it
			File directory = new File(String.valueOf(customMetadataFolder));
			if(!directory.exists()){
				System.out.println("\tfolder: " + customMetadataFolder + " does not exist, creating it");
				directory.mkdir();
			}
			//			otherObjWriter = new CSVWriter(new FileWriter(otherObjectCsvName), ',', CSVWriter.NO_QUOTE_CHARACTER); 
			otherObjWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + CATALOG_SCHEMA_FILENAME)); 
			tableWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + TABLEVIEWS_FILENAME)); 
			viewWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + VIEWS_FILENAME)); 
			this.columnWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + COLUMN_FILENAME)); 
			this.viewColumnWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + VCOLUMN_FILENAME)); 
			this.linksWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + LINKS_FILENAME)); 

			otherObjWriter.writeNext(new String[]{"class","identity","core.name"});
			tableWriter.writeNext(new String[]{"class","identity","core.name", "core.description"});
			viewWriter.writeNext(new String[]{"class","identity","core.name", "core.description", "com.infa.ldm.relational.ViewStatement"});
			columnWriter.writeNext(new String[]{"class","identity","core.name","com.infa.ldm.relational.Datatype"
					,"com.infa.ldm.relational.DatatypeLength", "com.infa.ldm.relational.Position"
					, "core.dataSetUuid" 
			});
			viewColumnWriter.writeNext(new String[]{"class","identity","core.name","com.infa.ldm.relational.Datatype"
					,"com.infa.ldm.relational.DatatypeLength", "com.infa.ldm.relational.Position"
					, "core.dataSetUuid" 
			});

			linksWriter.writeNext(new String[]{"association","fromObjectIdentity","toObjectIdentity"});

			System.out.println("\tFiles initialized");

		} catch (IOException e1) { 
			initialized=false;
			// TODO Auto-generated catch block 
			e1.printStackTrace(); 
		} 

		return initialized;
	}


	/**
	 * close the files that were opened - ensures that any buffers are cleared
	 * @return
	 */
	protected boolean closeFiles() {
		System.out.println("Step x: closing output files");

		try { 
			otherObjWriter.close(); 
			tableWriter.close();
			viewWriter.close();
			columnWriter.close(); 
			viewColumnWriter.close(); 
			linksWriter.close();
		} catch (IOException e) { 
			// TODO Auto-generated catch block 
			e.printStackTrace(); 
			return false;
		} 

		return true;

	}


	protected void createDatabase(String dbName) {
		System.out.println("\tcreating database: " + dbName);

		try {
			this.otherObjWriter.writeNext(new String[] {DB_TYPE,dbName,dbName});
			this.linksWriter.writeNext(new String[] {"core.ResourceParentChild","",dbName});
		} catch (Exception ex) {
			ex.printStackTrace();
		} 

		return;
	}


	protected void createSchema(String dbName, String schema) {
		//    	System.out.println("\tcreating database: " + dbName);

		String schId = dbName + "/" + schema;

		try {
			this.otherObjWriter.writeNext(new String[] {SCH_TYPE,schId,schema});
			this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.DatabaseSchema",dbName,schId});
		} catch (Exception ex) {
			ex.printStackTrace();
		} 

		return;
	}

	public void createTable(String dbName, String schema, String table, String desc) {

		String schId = dbName + "/" + schema;
		String tabId = schId + "/" + table;

		try {
			this.tableWriter.writeNext(new String[] {TAB_TYPE,tabId,table,desc});
			this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.SchemaTable",schId,tabId});
		} catch (Exception ex) {
			ex.printStackTrace();
		} 

		return;
	}

	public void createView(String dbName, String schema, String table, String desc, String ddl) {

		String schId = dbName + "/" + schema;
		String tabId = schId + "/" + table;

		try {
			this.viewWriter.writeNext(new String[] {VIEW_TYPE,tabId,table,desc,ddl});
			this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.SchemaView",schId,tabId});
		} catch (Exception ex) {
			ex.printStackTrace();
		} 

		return;
	}


	protected void createColumn(String dbName, String schema, String table, String column, 
			String type, String length, String pos, boolean isView) {

		String schId = dbName + "/" + schema;
		String tabId = schId + "/" + table;
		String colId = tabId + "/" + column;

		try {
			if (! isView) {
				this.columnWriter.writeNext(new String[] {COL_TYPE,colId,column,type,length, pos, tabId});
				this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.TableColumn",tabId,colId});
			} else {
				this.viewColumnWriter.writeNext(new String[] {VIEWCOL_TYPE,colId,column,type,length, pos, tabId});
				this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.ViewViewColumn",tabId,colId});
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} 

		return;
	}
	

	/**
	 * extra processing can call some dbms specific functions
	 * e.g. internal linage for denodo
	 * external lineage (back to s3 files) for athena
	 */
	protected void extraProcessing() {
		
		return;
	}
}
