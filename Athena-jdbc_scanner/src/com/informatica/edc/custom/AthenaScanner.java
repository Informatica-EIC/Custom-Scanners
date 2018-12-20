/**
 * 
 */
package com.informatica.edc.custom;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import com.opencsv.CSVWriter;


/**
 * @author dwrigley
 *
 */
public class AthenaScanner {

    public static final String version="0.3";
    
    private String jdbcDriver="";
    private String jdbcUrl="";
    private String userId="";
    private String passwd="";
    private ArrayList<String> schemaIncludeFilters = new ArrayList<String>();
    private ArrayList<String> schemaExcludeFilters = new ArrayList<String>();
    private ArrayList<String> tableIncludeFilters = new ArrayList<String>();
    private ArrayList<String> tableExcludeFilters = new ArrayList<String>();
    
    private String otherObjectCsvName="athenaObjects.csv";
    private String tabCsvName="athenaTablesViews.csv";
    private String columnCsvName="athenaColumns.csv";
    private String linksCsvName="links.csv";
	private CSVWriter otherObjWriter = null; 
	private CSVWriter tableWriter = null; 
	private CSVWriter columnWriter = null; 
	private CSVWriter linksWriter = null; 


	private String dbType="com.infa.ldm.relational.Database";
	private String schType="com.infa.ldm.relational.Schema";
	private String tabType="com.infa.ldm.relational.Table";
	private String colType="com.infa.ldm.relational.Column";
	private String vewType="com.infa.ldm.relational.View";
	private String vewColType="com.infa.ldm.relational.ViewColumn";

    
    /**
     * constructor for scanner initializaiton
     * @param propertyFile - controls how to connect and what to extract
     */
    AthenaScanner(String propertyFile) {
        System.out.println(this.getClass().getSimpleName() + " " + version +  " initializing properties from: " + propertyFile);
		try {			
			File file = new File(propertyFile);
			FileInputStream fileInput = new FileInputStream(file);
			Properties prop;
			prop = new Properties();
			prop.load(fileInput);
			fileInput.close();
			
			jdbcDriver = prop.getProperty("jdbc.driver.class");
			jdbcUrl =  prop.getProperty("jdbc.url");
			userId =  prop.getProperty("user");
			passwd = prop.getProperty("pwd");			
			if (passwd==null || passwd.equals("<prompt>") || passwd.equals("")) {
				System.out.println("password set to <prompt> for user " + userId  + " - waiting for user input...");
				passwd = getPassword();
//				System.out.println("pwd chars entered (debug):  " + passwd.length());
			}
			
			System.out.println("   jdbc driver=" + jdbcDriver);
			System.out.println("      jdbc url=" + jdbcUrl);
			System.out.println("          user=" + userId);
			System.out.println("           pwd=" + passwd.replaceAll(".", "*"));
			
			
			System.out.println("Include/Exclude settings");
			
			String filterText = prop.getProperty("schema.include.filter");
			if (filterText != null && filterText.length()>0) {
				schemaIncludeFilters = new ArrayList<String>(Arrays.asList(filterText.split(";"))); 
			}
		
			if (schemaIncludeFilters.size()==0) {
				System.out.println("\tschemas include filters=none - all (not excluded) will be extracted");			
			} else {
				System.out.println("\tschemas to include=" + schemaIncludeFilters.toString());
			}

			filterText = prop.getProperty("schema.exclude.filter");
			if (filterText != null && filterText.length()>0) {
				schemaExcludeFilters = new ArrayList<String>(Arrays.asList(filterText.split(";"))); 
			}
			if (schemaExcludeFilters.size()==0) {
				System.out.println("\tschemas exclude filters=none - all will be excluded");			
			} else {
				System.out.println("\tschemas to exclude=" + schemaExcludeFilters.toString());
			}

			filterText = prop.getProperty("table.include.filter");
			if (filterText != null && filterText.length()>0) {
				tableIncludeFilters = new ArrayList<String>(Arrays.asList(filterText.split(";"))); 
			}
			if (tableIncludeFilters.size()==0) {
				System.out.println("\ttables include  filters=none - all tables be extracted (in not excluded)");			
			} else {
				System.out.println("\ttables to include=" + tableIncludeFilters.toString());
			}

			
			filterText = prop.getProperty("table.exclude.filter");
			if (filterText != null && filterText.length()>0) {
				tableExcludeFilters = new ArrayList<String>(Arrays.asList(filterText.split(";"))); 
			}
			if (tableExcludeFilters.size()==0) {
				System.out.println("\ttables exclude  filters=none - no tables will be excluded (except for table.include.filter settings)");			
			} else {
				System.out.println("\ttables to exclude=" + tableExcludeFilters.toString());
			}

			
	     } catch(Exception e) {
	     	System.out.println("error reading properties file: " + propertyFile);
	     	e.printStackTrace();
	     }

    }  // end constructor
    
    /**
     * run the scanner
     */
    protected void run() {
    	
		try {  
			
			System.out.println("Initializing output files");
			this.initFiles();
			
			System.out.println("Initializing jdbc driver class: " + jdbcDriver);
			Class.forName(jdbcDriver);  
			
			System.out.println("establishing connection to: " + jdbcUrl);
			Connection con=DriverManager.getConnection(jdbcUrl, userId, passwd);  
			System.out.println("Connected!");

			System.out.println("getting database metadata object (con.getMetaData())");
			DatabaseMetaData dbMetaData = con.getMetaData();
			
			String catalog="";
			// get the catalogs - if any...


		    Statement stmntSchems = con.createStatement();
		    String schemaName="";
		    String tableName="";
		    
			System.out.println("getting catalogs:  DatabaseMetaData.getCatalogs()");
			ResultSet catalogs = dbMetaData.getCatalogs();
		    while (catalogs.next()) {
		        catalog = catalogs.getString(1);  //"TABLE_CATALOG"
		        System.out.println("catalog: "+catalog);
		        createDatabase(catalog);

		        ResultSet schemas=null;
		        boolean option1=true;
		        try {
			        System.out.println("\tgetting schemas - using 'show databases' command");
				    schemas = stmntSchems.executeQuery("show databases");
		        } catch (Exception ex) {
				    option1=false;
		        	System.out.println("Error getting list of databases using: show databases; " + ex.getMessage());
		        }
		        
		        if (schemas==null) {
			        try {
				        System.out.println("\tgetting schemas - jdbc DatabaseMetaData.getSchemas()");
					    schemas = dbMetaData.getSchemas();
			        } catch (Exception ex) {
			        	System.out.println("Error getting list of databases using: getSchemas. " + ex.getMessage());
			        }
		        	
		        }
		    
		        // if there are still no schemas - exit... there are some serious problems
		        if (schemas==null) {
		        	return;
		        }
		        
		        // schemas will have 0 or more records - but not be null
			    while(schemas.next()) {
			    	if (option1) {
			    		schemaName = schemas.getString("database_name");
			    	} else {
			    		schemaName = schemas.getString("TABLE_SCHEM");			    		
			    	}
			    	System.out.println("\tschema=" + schemaName);
			    	
			    	if (isObjectIncluded(schemaName, schemaIncludeFilters, schemaExcludeFilters)) {
//			    		System.out.println("\tschema: " + schemaName + " skipped - does not match any filter expressions");
//			    	} else { 
			    		// good to go for this schema
				    	createSchema(catalog, schemaName);
				    	
				    	// get a list of views - they also get listed as tables - so we need to get a list of the 
				    	// views first so they are not processed 2x (and we can extract the view sql)
				    	System.out.println("\t\tgetting view list using: 'show views in " + schemaName + "' command");
					    ResultSet viewRs = con.createStatement().executeQuery("show views in " + schemaName);
					    List<String> views = new ArrayList<String>();
					    while (viewRs.next()) {
					    	views.add(viewRs.getString("views"));
					    }
					    System.out.println("\t\tviews found: " + views.toString());
				    	
				    	System.out.println("\t\tgetting table list using: 'show tables in " + schemaName + "' command");
					    ResultSet tables = con.createStatement().executeQuery("show tables in " + schemaName);
					    while(tables.next()) {
					        //Print
					    	tableName = tables.getString("tab_name");
					        System.out.println("\t\t" + tableName);
					        
					        if (! isObjectIncluded(tableName, tableIncludeFilters, tableExcludeFilters)) {
					        	System.out.println("table not processed:  " + tableName);
					        } else {
					        	// is it a table or view
						        boolean isTable=true;
					        	ResultSet tabSQL;
					        	StringBuffer viewBuf = new StringBuffer();
					        	String s3Location = "";
					        	if (views.contains(tableName)) {
					        		isTable=false;  // its a view
					        		System.out.println("\t\t\t" + tableName  + " is a view");
					        		System.out.println("\t\t\textracting create view statement:  "  + "SHOW CREATE VIEW " + schemaName + "." + tableName);
					        		try {
						        	tabSQL = con.createStatement().executeQuery("SHOW CREATE VIEW " + schemaName + "." + tableName);
							        	while (tabSQL.next()) {
							        		viewBuf.append(tabSQL.getString("create view") + "\n");
							        	}
					        		} catch (Exception ex) {
					        			ex.printStackTrace();
					        		}
					        	} else {
					        		System.out.println("\t\t\textracting create table statement:  "  + "SHOW CREATE TABLE " + schemaName + "." + tableName);
					        		try {
						        	tabSQL = con.createStatement().executeQuery("SHOW CREATE TABLE " + schemaName + "." + tableName);
							        	while (tabSQL.next()) {
							        		viewBuf.append(tabSQL.getString("createtab_stmt") + "\n");
							        	}
							        	s3Location = extractLocation(viewBuf.toString());
							        	System.out.println("\t\t\tLocation=" + s3Location);
					        		} catch (Exception ex) {
					        			ex.printStackTrace();
					        		}
					        	}
					   
//					        	System.out.println("view/tab sql===");
//					        	System.out.print(viewBuf.toString());
					        	
					        	if (isTable) {
					        		createTable(catalog, schemaName, tableName, "TABLE", viewBuf.toString(), s3Location);
					        	} else {
					        		createTable(catalog, schemaName, tableName, "VIEW", viewBuf.toString(), "");					        		
					        	}
						        
						        //[TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, 
						        // DATA_TYPE, TYPE_NAME, COLUMN_SIZE, BUFFER_LENGTH, 
						        // DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, REMARKS, 
						        // COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH, 
						        // ORDINAL_POSITION, IS_NULLABLE, SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE, 
						        // SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN]
						    	
					        	int colCount=0;
					        	try {
								    ResultSet columns = dbMetaData.getColumns(catalog, schemaName, tableName, null);
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
						                String scTable = columns.getString("SCOPE_TABLE");
						                String scCatlg = columns.getString("SCOPE_CATALOG");
			
							        	if (isTable) {
							        		createColumn(catalog, schemaName, tableName, "TABLE", columnName, typeName, columnsize, pos);
							        	} else {
							        		createColumn(catalog, schemaName, tableName, "VIEW", columnName, typeName, columnsize, pos);						        		
							        	}
						                		
						                //Printing results
	//					                System.out.println("\t\t\t" + columnName + " type=" //+ dataTypes.get(datatype) + "|" + 
	//					                		+ typeName + " size=" + columnsize + " digits=" + decimaldigits + " nulls=" + isNullable
	//					                		+ " remarks=" + remarks + " def=" + def + " sqlType=" + sqlType
	//					                		+ " pos=" + pos + " scTable=" + scTable + " scCatlg=" + scCatlg
	//					                		);
								    }  // end for each column
					        	} catch (Exception ex) {
					        		System.out.println("error extracting column metadata...");
					        		ex.printStackTrace();
					        	}
							    System.out.println("\t\t\tcolumns extracted: " + colCount);
					        } // if the table should be processed
					    }  // end for each table
					    System.out.println("finished tables");
					    
			    	} // end of schema filter
	
			    }  // end loop for each schema
			    System.out.println("finished schemas");

		    }  // end loop for each catalog (usually only 1)		    

		    System.out.println("closing athena jdbc connection...");
		    con.close();  
		} catch(ClassNotFoundException cne) {
				System.out.println("\tcannot initialize class=" + jdbcDriver + " " + cne.getClass().getName() + " jdbc driver needs to be in current folder or CLASSPATH");
		} catch(Exception e) { 
			 	System.out.println(e);
				e.printStackTrace();
		}  
    	
    }

	private boolean isObjectIncluded(String objectName, List<String> includedRegexes, List<String> excludedRegexes) {
//		System.out.println("checking to exclude" + objectName);
		boolean processSchema = true;  // assume true - until we find out otherwise
		
		// first check if the schema should be excluded (this over-rides the include list)
		if (excludedRegexes.size() > 0) {
			for (String filter: excludedRegexes) {
				if (objectName.matches(filter)) {
					System.out.println("\t\tobject: " + objectName + " is excluded");
					processSchema = false;
					return false;
				}							
			}
		}
		
		// assuming it is not specifically excluded - see if it is in the include list
		if (includedRegexes.size()>0) {
			// assume false until we know it is true (e.g. filtered in)
			processSchema = false;  // only process the ones that match (since a filter has been added
			for (String filter: includedRegexes) {
				if (objectName.matches(filter)) {
					processSchema = true;
					break;
				}							
			}
		}
		System.out.println("\t\tobject: " + objectName + " included:" + processSchema);
		return processSchema;
	}
    
    private boolean initFiles() {
    	// assume working, until it is not
    	boolean initialized=true;
    	 
		try { 
//			otherObjWriter = new CSVWriter(new FileWriter(otherObjectCsvName), ',', CSVWriter.NO_QUOTE_CHARACTER); 
			otherObjWriter = new CSVWriter(new FileWriter(otherObjectCsvName)); 
			tableWriter = new CSVWriter(new FileWriter(this.tabCsvName)); 
			this.columnWriter = new CSVWriter(new FileWriter(columnCsvName)); 
			this.linksWriter = new CSVWriter(new FileWriter(this.linksCsvName)); 
			
			otherObjWriter.writeNext(new String[]{"class","identity","core.name"});
			tableWriter.writeNext(new String[]{"class","identity","core.name", "com.infa.ldm.relational.ViewStatement", "com.infa.ldm.relational.Location"});
			columnWriter.writeNext(new String[]{"class","identity","core.name","com.infa.ldm.relational.Datatype"
												,"com.infa.ldm.relational.DatatypeLength", "com.infa.ldm.relational.Position"
												, "core.dataSetUuid" 
											   });
			
			linksWriter.writeNext(new String[]{"association","fromObjectIdentity","toObjectIdentity"});
			
		} catch (IOException e1) { 
			initialized=false;
			// TODO Auto-generated catch block 
			e1.printStackTrace(); 
		} 
 
		return initialized;
    }
    
    
    private boolean closeFiles() {
		System.out.println("closing output files");

		try { 
			otherObjWriter.close(); 
			tableWriter.close();
			columnWriter.close(); 
			linksWriter.close();
		} catch (IOException e) { 
			// TODO Auto-generated catch block 
			e.printStackTrace(); 
			return false;
		} 
		
		return true;

    }
    
    private void createDatabase(String dbName) {
    	System.out.println("\tcreating database: " + dbName);
    	
    	try {
    		this.otherObjWriter.writeNext(new String[] {dbType,dbName,dbName});
    		this.linksWriter.writeNext(new String[] {"core.ResourceParentChild","",dbName});
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	} 
    	
    	return;
    }

    private void createSchema(String dbName, String schema) {
//    	System.out.println("\tcreating database: " + dbName);
    	
    	String schId = dbName + "/" + schema;
    	
    	try {
    		this.otherObjWriter.writeNext(new String[] {schType,schId,schema});
    		this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.DatabaseSchema",dbName,schId});
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	} 
    	
    	return;
    }

    private void createTable(String dbName, String schema, String table, String type, String ddl, String location) {
    	
    	String schId = dbName + "/" + schema;
    	String tabId = schId + "/" + table;
    	
    	try {
    		if (type=="TABLE") {
        		this.tableWriter.writeNext(new String[] {tabType,tabId,table,ddl,location});
    			this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.SchemaTable",schId,tabId});
    		} else {
    			// there is no location for a view
        		this.tableWriter.writeNext(new String[] {vewType,tabId,table, ddl, ""});
    			this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.SchemaView",schId,tabId});
    		}
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	} 
    	
    	return;
    }

    
    private void createColumn(String dbName, String schema, String table, String tableType, String column, String type, String length, String pos) {
    	
    	String schId = dbName + "/" + schema;
    	String tabId = schId + "/" + table;
    	String colId = tabId + "/" + column;
    	
    	try {
    		if (tableType=="TABLE") {
	    		this.columnWriter.writeNext(new String[] {colType,colId,column,type,length, pos, tabId});
	    		this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.TableColumn",tabId,colId});
    		} else {
    			// view
	    		this.columnWriter.writeNext(new String[] {vewColType,colId,column,type,length, pos, tabId});
	    		this.linksWriter.writeNext(new String[] {"com.infa.ldm.relational.ViewViewColumn",tabId,colId});
   			
    		}
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	} 
    	
    	return;
    }
    
    private String extractLocation(String sqlStatement) {
    	String theLocation="";
    	
    	// find "LOCATION" and 
    	int locStart = sqlStatement.indexOf("LOCATION");
    	int aposStart = sqlStatement.indexOf("'", locStart+9);
    	int aposEnd = sqlStatement.indexOf("'", aposStart+2);
    	
    	theLocation = sqlStatement.substring(aposStart +1, aposEnd);
    	
    	
    	return theLocation;
    }
    
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		AthenaScanner scanner;
		if (args.length==0) {
			System.out.println("AthenaScanner: missing configuration properties file: using athena.properties in current folder");
			scanner = new AthenaScanner("athena.properties");
		} else {
			System.out.println("AthenaScanner: " + args[0] + " currentTimeMillis=" +System.currentTimeMillis());
			
			// pass the property file - the constructor will read all input properties
			scanner = new AthenaScanner(args[0]);			
		}	
		scanner.initFiles();
		scanner.run();
		scanner.closeFiles();
		
		System.out.println("Finished");

	}  // end main()
	


	/**
	 * prompt the user for a password, using the console (default)
	 * for development environments like eclipse, their is no standard console.
	 * so in that case we open a swing ui panel with an input field to accept a password
	 * 
	 * @return the password entered
	 * 
	 * @author dwrigley
	 */
	protected String getPassword() {
		String password;
		Console c=System.console();
		if (c==null) { //IN ECLIPSE IDE (prompt for password using swing ui    	
			final JPasswordField pf = new JPasswordField(); 
			String message = "AWS Secret Access Key:";
			password = JOptionPane.showConfirmDialog( null, pf, message, JOptionPane.OK_CANCEL_OPTION, 
						JOptionPane.QUESTION_MESSAGE ) == JOptionPane.OK_OPTION ? new String( pf.getPassword() ) : "enter your pwd here...."; 
		} else { //Outside Eclipse IDE  (e.g. windows/linux console)
			password = new String(c.readPassword("User password: "));
		}		
		return password;
	}

}
