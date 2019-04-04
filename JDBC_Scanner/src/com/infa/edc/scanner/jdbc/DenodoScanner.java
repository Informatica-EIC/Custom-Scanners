package com.infa.edc.scanner.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.opencsv.CSVWriter;

public class DenodoScanner extends GenericScanner {
	public static final String version="0.9.2";

	protected String databaseName="";
	
	protected CSVWriter custLineageWriter = null; 
	
	protected int expressionsFound = 0;
	protected int expressionsProcessed = 0;
	protected int expressionLinks = 0;
	protected int totalColumnLineage = 0;
	protected int totalTableLineage = 0;
	
	protected Set<String> datasetsScanned = new HashSet<String>();
	protected Set<String> elementsScanned = new HashSet<String>();

	
	protected Map<String, List<String>> viewDbNameMap = new HashMap<String, List<String>>();
	protected Map<String, List<String>> tableDbNameMap = new HashMap<String, List<String>>();
	protected Map<String, Map<String, List<String>>> colDbNameMap = new HashMap<String, Map<String, List<String>>>();

	public DenodoScanner(String propertyFile) {
		super(propertyFile);
		
		// denodo specific settings read here (default settings in generic superclass)
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
			
			databaseName = prop.getProperty("denodo.databaseName", "denodo_vdp");
			if (databaseName == null || databaseName.equals("")) {
				System.out.println("empty value set for denodo.databaseName: using 'denodo_vdp'");
				databaseName = "denodo_vdp";
			}
			
			System.out.println("Database name used for Denodo export:" + databaseName);
						
	     } catch(Exception e) {
	     	System.out.println("error reading properties file: " + propertyFile);
	     	e.printStackTrace();
	     }

	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length==0) {
			System.out.println("Denodo Custom scanner for EDC: missing configuration properties file: usage:  DenodoScanner <folder>/<config file>.properties");
			System.exit(0);
		}

		System.out.println("Denodo Custom scanner: " + args[0] + " currentTimeMillis=" +System.currentTimeMillis());

		// check to see if a disclaimer override parameter was passed
		String disclaimerParm="";
		if (args.length>=2) {
			// 2nd argument is an "agreeToDisclaimer" string
			disclaimerParm=args[1];
//			System.out.println("disclaimer parameter passed: " + disclaimerParm);
			if ("agreeToDisclaimer".equalsIgnoreCase(disclaimerParm)) {
				System.out.println("the following disclaimer was agreed to by passing 'agreeToDisclaimer' as 2nd parameter");
				System.out.println(DISCLAIMER);
			}
		}
				
		// 1 of 2 conditions must be true for the scanner to start
		// 1 - 2nd parameter is equal to "agreeToDisclaimer"
		// or (if no parm passed, or value does not match)
		// then the user must agree to the prompt
		if ("agreeToDisclaimer".equalsIgnoreCase(disclaimerParm) || showDisclaimer()) {
			DenodoScanner scanner = new DenodoScanner(args[0]);
			scanner.run();
		} else {
			System.out.println("Disclaimer was declined - exiting");
		}

		

	}
	
	/**
	 * iterate over all catalogs (databases) there may be multiple
	 * need to determine whether to default to extracting all, or only a subset
	 */
	@Override
	public void getCatalogs() {
		System.out.println("Step 5: creating denodo catalog...");
		
        this.createDatabase(databaseName);
        
        getSchemas(databaseName);
	}
	

	/**
	 * get the schemas denodo - which are really catalogs
	 * @param catalogName
	 */
	@Override
	public void getSchemas(String catalogName) {
		System.out.println("Step 6: creating denodo schemas (from catalogs)");
		ResultSet catalogs;
		try {
			catalogs = dbMetaData.getCatalogs();
			String schemaName;
		    while (catalogs.next()) {
		    	schemaName = catalogs.getString(1);  //"TABLE_CATALOG"
		        System.out.println("\tschema: " + schemaName);
		        
				if (isCatalogScanned(schemaName)) {
			        // create the schema object
		    		createSchema(catalogName, schemaName);
		    		// process tables
		    		getTables(catalogName, schemaName);
		    		getViews(catalogName, schemaName);
				} else {
					// message for catalog is not exported...
					System.out.println("\tschema=" + schemaName + " skipped - not included in catalog filter: " + catalogFilter);
				}

		        

		    }
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
	protected void getTables(String catalogName, String schemaName) {
		try {
			ResultSet rsTables = dbMetaData.getTables(schemaName, null, null, new String[] { "TABLE" });
			int tableCount = 0;
			while (rsTables.next()) {
				// Print
				tableCount++;
				
				String tableName=rsTables.getString("TABLE_NAME");
				
				List<String> values = tableDbNameMap.get(schemaName);
				if (values==null) {
					values = new ArrayList<String>();
				}
				values.add(tableName);
				tableDbNameMap.put(schemaName, values);
				datasetsScanned.add(schemaName + "/" + tableName);


				// System.out.println("found one...");
				System.out.println("\t" + " catalog=" + rsTables.getString("TABLE_CAT") + " schema="
						+ rsTables.getString("TABLE_SCHEM") + " tablename=" + rsTables.getString("TABLE_NAME")
						+ " TABLE_TYPE=" + rsTables.getString("TABLE_TYPE")
//						+ " comments=" + rsTables.getClob("REMARKS")
						);
//				System.out.println(rsTables.getMetaData().getColumnTypeName(5));
				this.createTable(catalogName, schemaName, rsTables.getString("TABLE_NAME"), rsTables.getString("REMARKS"));
				
				
				this.getColumnsForTable(catalogName, schemaName, rsTables.getString("TABLE_NAME"), false);
			}
			System.out.println("\tTables extracted: " + tableCount);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	protected Map<String,String> columnExpressions = new HashMap<String,String>();
	
	protected void storeTableColExpressions(String schemaName, String tableName) {
//		System.out.println("gathering expression fields for table" + schemaName + "/" + tableName);
		
		String selectSt = "select input_view_database_name, input_view_name, column_name, expression "
				+ "from  COLUMN_DEPENDENCIES ('" 
				+ schemaName + "', '"
				+ tableName +"', null)"
				+ "where depth=1 and input_view_name='"
				+ tableName +"' "
				+ "and expression is not null";
		
		try {
			Statement viewExpressions = connection.createStatement();
			ResultSet viewExprRs = viewExpressions.executeQuery(selectSt);
			while (viewExprRs.next()) {
//				System.out.println("expresison field !!!!!!!!!!!");
				String colName = viewExprRs.getString("column_name");
				String expr = viewExprRs.getString("expression");
				String key = schemaName + "/" + tableName + "/" + colName;
				expressionsFound++;
				columnExpressions.put(key, expr);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * get view information
	 */
	protected void getViews(String catalogName, String schemaName) {
		try {
//			ResultSet rsViews = dbMetaData.getTables(schemaName, null, null, new String[] { "VIEW" });
			
			PreparedStatement viewMetadata = null;
			String viewQuery = "SELECT database_name, name, type, user_creator, last_user_modifier, create_date, last_modification_date, description, view_type, folder " +
					"FROM GET_VIEWS() " + 
					"WHERE input_database_name = ? and view_type>0";
			viewMetadata = connection.prepareStatement(viewQuery);
			viewMetadata.setString(1, schemaName);

	        ResultSet rsViews = viewMetadata.executeQuery();


			// instead of calling standard jdbc - use this
			// SELECT * FROM GET_VIEWS() WHERE input_database_name = '<catalogName>'
			// will also return the view folder
			
			int viewCount = 0;
			while (rsViews.next()) {
				// Print
				viewCount++;
				// jdbc standard call returns TABLE_NAME
				//String viewName = rsViews.getString("TABLE_NAME");
				
				// denodo GET_VIEWS() returns "name"
				String viewName = rsViews.getString("name");
				
				List<String> values = viewDbNameMap.get(schemaName);
				if (values==null) {
					values = new ArrayList<String>();
				}
				values.add(viewName);
				viewDbNameMap.put(schemaName, values);
				datasetsScanned.add(schemaName + "/" + viewName);


				// System.out.println("found one...");
				System.out.println("\t" + " catalog=" + rsViews.getString("database_name")  + " viewname=" + viewName
						+ " TABLE_TYPE=" + rsViews.getString("view_type")
						+ " comments=" + rsViews.getString("description")
						);
				//				System.out.println(rsTables.getMetaData().getColumnTypeName(5));
				
				// get the view sql
				String viewSQL="desc vql view " + schemaName + "." + viewName;
//				PreparedStatement wrapperStmnt = null;
//				String wrapperQuery = "DESC VQL WRAPPER JDBC ";
				String viewSqlStmnt="";
		        try {
					Statement stViewSql = connection.createStatement(); 
					ResultSet rs = stViewSql.executeQuery(viewSQL);
					while (rs.next()) {
//						System.out.println("\t\twrapper.....");
//						System.out.println("view sql^^^^=" + viewSQL);
						viewSqlStmnt = rs.getString("result");
						
						// @ todo - extract only the view definition - denodo also incudea all dependent objects
//						System.out.println("viewSQL=\n" + result);
						
						// now we need to extract the DATASOURCENAME='<name>'
						///int dsStart = result.indexOf("DATASOURCENAME=");
//						System.out.println("start pos=" + dsStart + " total length=" + result.length());
						///int dsEnd = result.indexOf("\n", dsStart);
//						System.out.println("start end=" + dsEnd + " total length=" + result.length());
						///if (dsStart>0 && dsEnd < result.length()) {
						///	connectionName = result.substring(dsStart+15, dsEnd);
						///}
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				
				
				this.createView(catalogName, schemaName, viewName, rsViews.getString("description"), viewSqlStmnt, rsViews.getString("folder"));
				
				// for denodo - we want to document the expression formula for any calculated fields
				// so for this table - we will store in memory - 
				storeTableColExpressions(schemaName, viewName);


				getColumnsForTable(catalogName, schemaName, viewName, true);
			}
			System.out.println("\tViews extracted: " + viewCount);
			
			// collect for later 0 tge 

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	


	protected void getColumnsForTable(String catalogName, String schemaName, String tableName, boolean isView) {
    	int colCount=0;
    	try {
		    ResultSet columns = dbMetaData.getColumns(schemaName, null, tableName, null);
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
                
                String colKey = schemaName + "/" + tableName + "/" + columnName;
                String exprVal = columnExpressions.get(colKey);
                if (exprVal != null) {
                	System.out.println("\t\texpression found for field " + colKey + " " + exprVal);
                }
//                if (this.columnExpressions.containsKey(schemaName + "/" + tableName + "/" + columnName)) {
//                	System.out.println("!!!!!!!!!!store the expression....." + exprVal + " for " + colKey);
//                }
                
                
                // store the list of columns - for linking later
                elementsScanned.add(colKey);
                
                
//                System.out.println("\t\t\tcolumnn=" + catalogName + "/" + schemaName + "/" + tableName+ "/" + columnName+ "/" + typeName+ "/" + columnsize+ "/" + pos);

//        		createColumn( );
                if (isView) {
                	this.createViewColumn(catalogName, schemaName, tableName, columnName, typeName, columnsize, pos, exprVal);
                } else {
                	this.createColumn(catalogName, schemaName, tableName, columnName, typeName, columnsize, pos, isView);
                }
                
                // add to name mapping
//                if (!isView) {
                	Map<String, List<String>> schemaMap = colDbNameMap.get(schemaName);
    				if (schemaMap==null) {
    					schemaMap = new HashMap<String, List<String>>();
    					colDbNameMap.put(schemaName, schemaMap);
    				}
    				
    				// check the table is in the schema map
    				List<String> cols = schemaMap.get(tableName);
    				if (cols==null) {
    					// add a new list of columns
    					cols = new ArrayList<String>();
    					schemaMap.put(tableName, cols);
    					
    				}
    				cols.add(columnName);
//                }

                		
		    }  // end for each column
    	} catch (Exception ex) {
    		System.out.println("error extracting column metadata...");
    		ex.printStackTrace();
    	}
	    System.out.println("\t\tcolumns extracted: " + colCount);

	}
	
	protected Map<String, String> privateDepsMap = new HashMap<String, String>();
	
	/**
	 * query the view_dependencies(<db>, <view>) stored procedure to get view level lineage
	 * 
	 * Note:  we need to be careful with how denodo is queried - the procedure can return some strange results
	 *        when joins are added  (private views are used)
	 *        best method at the moment is:-
	 *        
	 *        select * from   view_dependencies ('<db>', '<view>' )
	 *        where depth=1 and view_type != 'Base View'
	 *        order by input_view_database_name, input_view_name, view_identifier desc
	 *        
	 *        then we process each record - 
	 *          - if the input_view_name = view_name & private_view = false
	 *               make a direct connection - if depencency_name (a view name) is a real object (not a private view)
	 *          - if the inut_view_name != view_name & private_view = true
	 *               it is an indirect reference 
	 *               if the depencency_name object is valid - link it (otherwise skip it - since it is just a private view)
	 * 
	 * @param dbName the database that the view belongs to
	 * @param viewName the view that we need lineage for
	 */
	protected void extractViewLevelLineage(String dbName, String viewName) {
		System.out.println("\t\tview lineage extraction - using view_dependencies stored procedure for: " + dbName + "/" + viewName);
		String query="select * " + 
				"from   view_dependencies (?, ?)  " + 
				"where depth=1 and view_type != 'Base View' " + 
				"order by input_view_database_name, input_view_name, view_identifier desc"
				;
		
//		System.out.println("structs scanned..." + datasetsScanned.size());
//		System.out.println(datasetsScanned);
		
        int recCount=0;		
        try {
    		PreparedStatement deps = connection.prepareStatement(query);
    		deps.setString(1, dbName);
			deps.setString(2, viewName);
	        ResultSet rsDeps = deps.executeQuery();
		    while(rsDeps.next()) {
		    	recCount++;
		    	String inViewName = rsDeps.getString("input_view_name");
		    	String returnedviewName = rsDeps.getString("view_name");
		    	String privateView = rsDeps.getString("private_view");
//		    	String fromDB = rsDeps.getString("view_database_name");
		    	String fromDB = rsDeps.getString("dependency_database_name");
		    	String fromTab = rsDeps.getString("dependency_name");
		    	
//		    	System.out.println("\t3 way: " + inViewName + " " + returnedviewName + " " + privateView + " " + fromTab);
		    	if (
		    			( inViewName.equals(returnedviewName) && privateView.equals("false") )
		    			|
		    			( ! inViewName.equals(returnedviewName) && privateView.equals("true") && returnedviewName.startsWith("_" + inViewName + "_") )
		    			
		    	   ) {
		    		// case 1
		    		String objKey = fromDB + "/" + fromTab;
		    		if (datasetsScanned.contains(objKey)) {
//		    			System.out.println("\t\t\tok - direct link found...." + objKey);
				    	String lefttabId = databaseName + "/" + fromDB + "/" +fromTab;
				    	String righttabId = databaseName + "/" + dbName + "/" +viewName;
			    		linksWriter.writeNext(new String[] {"core.DataSetDataFlow",lefttabId,righttabId});	
//			    		System.out.println("\t\t\twriting table level lineage: " +  lefttabId + " ==>> " + righttabId);
			    		totalTableLineage++;
		    		} else {
//		    			System.out.println("\t\t\tprobaby a link to a virtual table XXX" + objKey);	
		    			// add to (verbose) logging (to be implemented)
		    		}
		    	} 
		    }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 
	}
	
	
	/**
	 * extact view column level lineage - internal to denodo
	 * similar to view level lineage - but it needs to be called individually for each column
	 * (if called with null as the column, we get results for all columns in a view - but the resultset is not accurate)
	 * 
	 * so there will be a lot of calls to the db - but it is the ONLY way to get the right result
	 * 
	 * @param dbName
	 * @param viewName
	 * @param columnName
	 */
	protected void extractViewColumnLevelLineage(String dbName, String viewName, String columnName) {
//		System.out.println("view column lineage extraction - using column_dependencies stored procedure");
		String query="select * " + 
				"from   COLUMN_DEPENDENCIES (?, ?, ?)  " + 
				"where depth=1 "; //+ 
				// "order by input_view_name, view_name, column_name, view_identifier desc, depth;"
				//;
		
//		System.out.println("elements scanned..." + elementsScanned.size());
//		System.out.println(elementsScanned);
		
        int recCount=0;		
        try {
    		PreparedStatement deps = connection.prepareStatement(query);
    		deps.setString(1, dbName);
			deps.setString(2, viewName);
			deps.setString(3, columnName);
	        ResultSet rsDeps = deps.executeQuery();
		    while(rsDeps.next()) {
		    	recCount++;
		    	String inViewName = rsDeps.getString("input_view_name");
		    	String toColName = rsDeps.getString("column_name");
		    	String returnedviewName = rsDeps.getString("view_name");
		    	String privateView = rsDeps.getString("private_view");
//		    	String fromDB = rsDeps.getString("view_database_name");
		    	String fromDB = rsDeps.getString("dependency_database_name");
		    	String fromTab = rsDeps.getString("dependency_name");
		    	String fromCol = rsDeps.getString("dependency_column_name");
		    	
		    	if (  	( inViewName.equals(returnedviewName) && privateView.equals("false") )
		    			|
		    			( ! inViewName.equals(returnedviewName) && privateView.equals("true")  && returnedviewName.startsWith("_" + inViewName + "_") )   
		    		) {
		    		String objKey = fromDB + "/" + fromTab + "/" + fromCol;
		    		// if it is an actual object reference - link it
		    		if (elementsScanned.contains(objKey)) {
//		    			System.out.println("\t\t\tok - direct link found...." + objKey);
				    	String leftId = databaseName + "/" + fromDB + "/" +fromTab + "/" + fromCol;
				    	String rightId = databaseName + "/" + dbName + "/" +viewName + "/" + columnName;
			    		linksWriter.writeNext(new String[] {"core.DirectionalDataFlow",leftId,rightId});		    			
//			    		System.out.println("\t\t\t\twriting column level lineage: " +  leftId + " ==>> " + rightId);
			    		totalColumnLineage++;
		    		} else {
		    			// it could be a virtual column (for a join etc, or an expression field)
		    			// special case - for expressions the parent object might match - but the column name may be a multi-field (with a comma)
		    			if (fromCol != null && fromCol.contains(",")) {
		    				expressionsProcessed++;
//		    				System.out.println("expr field...");
		    				if (datasetsScanned.contains(fromDB + "/" + fromTab)) {
//		    					System.out.println("split them here");
					    		String[] exprParts = fromCol.split(",");
					    		
					    		//@todo refactor here - too much copy/paste of code
					    		for (String exprPart: exprParts) {
					    			expressionLinks++;
//					    			System.out.println("\t\t\tlinking ...." + exprPart);
					    			String  leftId = databaseName + "/" + fromDB + "/" +fromTab + "/" + exprPart;
					    			String rightId = databaseName + "/" + dbName + "/" +viewName + "/" + columnName;
						    		linksWriter.writeNext(new String[] {"core.DirectionalDataFlow",leftId,rightId});		    			
//						    		System.out.println("\t\t\t\twriting column level lineage: " +  leftId + " ==>>> " + rightId);
						    		totalColumnLineage++;
					    		}

		    				}
		    				// end - is an expression reference
		    			}  else {
//		    				System.out.println("\t\t\tprobably a link to a virtual table column XXX" + objKey);	
		    			}
		    		}
		    	} 
		    }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 
	}	
	
	/**
	 * read the private view column dependencies and store in a memory model
	 * for lookup later
	 * @param dbname
	 * @param viewName
	 */
	
	/**
	protected void extractPrivateViewDeps(String dbname, String viewName) {
//		System.out.println("pre processoor for :-" + dbname + "__" + viewName);
		String query="select distinct * " + 
				"from  COLUMN_DEPENDENCIES (?, ?, null)  " + 
				"where depth=1 and input_view_name=? " + 
				"and private_view='true' " + 
				"order by input_view_name, dependency_identifier";
        try {
    		PreparedStatement deps = connection.prepareStatement(query);
			deps.setString(1, dbname);
	        deps.setString(2, viewName);
	        deps.setString(3, viewName);

	        ResultSet rsDeps = deps.executeQuery();
	        int recCount=0;
		    while(rsDeps.next()) {
		    	recCount++;
		    	String fromDB = rsDeps.getString("dependency_database_name");
		    	String fromTab = rsDeps.getString("dependency_name");
		    	String fromCol = rsDeps.getString("dependency_column_name");
		    	String toCol = rsDeps.getString("column_name");
		    	String expression = rsDeps.getString("expression");
		    	String private_view = rsDeps.getString("private_view");
		    	String view_name = rsDeps.getString("view_name");
		    	
		    	String toKey = view_name + "/" + toCol;
		    	String fromKey = fromTab + "/" + fromCol;
		    	
//		    	System.out.println("\nfrom " + fromKey);
//		    	System.out.println("to   " + toKey);
		    	
		    	if (privateDepsMap.containsKey(toKey)) {
//		    		System.out.println("duplicte????? rec-" + recCount + " toKey="  + toKey + " fromKey=" + fromKey);
		    	} else {
		    		privateDepsMap.put(toKey, fromKey);
		    	}
		    	
		    }
	        System.out.println("records processed: " + recCount + " maps created:=" + privateDepsMap.size());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        System.out.println("privateDepsMap..." + privateDepsMap);
		
	}	
	
	protected String getActualLineageForInternalObject(String aKey) {
		String ret = "";
		ret = aKey;
//		System.out.println("checking from key=" + aKey);
		while (privateDepsMap.containsKey(ret)) {
			ret = privateDepsMap.get(ret);
//			System.out.println("\tret=" + ret);
		}
		
//		System.out.println("returning: " + ret);
		return ret;
	}
	*/
	
	/**
	 * extra processing can call some dbms specific functions
	 * e.g. internal linage for denodo
	 * external lineage (back to s3 files) for athena
	 */
	protected void extraProcessing() {
		System.out.println("");
		System.out.println("Denodo specific processing - extracting view lineage...");
		
		
		// unique list of dataflows...
		Set datasetFlows = new HashSet();
		
		PreparedStatement deps = null;
//		String dependenciesQuery = "select input_view_database_name, input_view_name, " +
//				"view_name, column_name, dependency_database_name, dependency_name, " +
//				"dependency_column_name, expression, depth " + 
//				"from  COLUMN_DEPENDENCIES (?, ?, null)  " + 
//				"where depth=1 and view_name=?  " + 
//				"order by input_view_name, view_name, column_name, depth;";
		
		// if private_view='true' and depencency_type='Base View' - we have the last element
		String dependenciesQuery = "select * " +
				"from  COLUMN_DEPENDENCIES (?, ?, null)  " + 
				"where depth=1 and input_view_name=?  " + 
				"and private_view='false' " + 
				"order by input_view_name, view_name, column_name, depth;";
		
		// for each schema - then view...
		for (String schema : viewDbNameMap.keySet()) {
			System.out.println("\tschema=" + schema);
			for (String view : viewDbNameMap.get(schema)) {
				System.out.println("\t\tview=" + view);
				this.extractViewLevelLineage(schema, view);		
				
		    	Map<String, List<String>> tableMap = colDbNameMap.get(schema);
		    	for (String col : tableMap.get(view)) {
//		    		System.out.println("\t\t\t%%%%%%%\tcolumn lineage for: + " + schema + "/" + view + "/" + col);		    			
					this.extractViewColumnLevelLineage(schema, view, col);
		    		
		    	}

				/**
				
				try {

					this.extractPrivateViewDeps(schema, view);
					
			        deps = connection.prepareStatement(dependenciesQuery);
			        deps.setString(1, schema);
			        deps.setString(2, view);
			        deps.setString(3, view);

			        ResultSet rsDeps = deps.executeQuery();
			        
//			        System.out.println("\t\tquery executed to get dependencies using COLUMN_DEPENDENCIES() call");
			        int tabLvlLineageCount=0;
			        int colLvlLineageCount=0;
			        int privateDeps=0;
				    while(rsDeps.next()) {
				    	String fromDB = rsDeps.getString("dependency_database_name");
				    	String fromTab = rsDeps.getString("dependency_name");
				    	String fromCol = rsDeps.getString("dependency_column_name");
				    	String toCol = rsDeps.getString("column_name");
				    	String expression = rsDeps.getString("expression");
				    	String private_view = rsDeps.getString("private_view");
				    	String viewName = rsDeps.getString("view_name");
				    	String viewType = rsDeps.getString("view_type");
				    	
				    	
				    	if (viewType.equals("Base View")) {
//				    		System.out.println("skipping internal lineage for base view - processed later..." + viewName + " <> " + fromTab + " fc=" + fromCol);
//				    		privateDeps++;
				    	} else {
//					    	String lefttabId = databaseName + "/" + fromDB + "/" +fromTab;
//					    	String righttabId = databaseName + "/" + schema + "/" +view;
					    	String leftId = databaseName + "/" + fromDB + "/" +fromTab + "/" + fromCol;
					    	String rightId = databaseName + "/" + schema + "/" +view + "/" + toCol;

					    	if (expression==null) {
					    		// check if the dependency is an internal one...
					    		String fromKey = fromTab + "/" + fromCol;
//					    		System.out.println("checking " + fromKey + " " + privateDepsMap.containsKey(fromKey));
					    		if (privateDepsMap.containsKey(fromKey)) {
					    			// get the actual lineage colum upstream
					    			String retStr = this.getActualLineageForInternalObject(fromKey);
					    			leftId = databaseName + "/" + fromDB + "/" + retStr;
	//								linksWriter.writeNext(new String[] {"!!indirect!!core.DirectionalDataFlow",leftId,rightId});
									linksWriter.writeNext(new String[] {"core.DirectionalDataFlow",leftId,rightId});
									// @TODO: table level lineage here too	
					    		} else {
									linksWriter.writeNext(new String[] {"core.DirectionalDataFlow",leftId,rightId});
									colLvlLineageCount++;
					    		}
					    	} else {
					    		// expression special processing 
					    		System.out.println("Expression===" + expression + " from:" + fromCol + " private?=" +private_view);
					    		String[] exprParts = fromCol.split(",");
					    		
					    		//@todo refactor here - too much copy/paste of code
					    		for (String exprPart: exprParts) {
					    			System.out.println("\t\t\tlinking ...." + exprPart);
					    			leftId = databaseName + "/" + fromDB + "/" +viewName + "/" + exprPart;
					    			rightId = databaseName + "/" + schema + "/" +view + "/" + toCol;
					    			
					    			String fromKey = fromTab + "/" + exprPart;
//					    			System.out.println(" key found " + fromTab + " ??? " + privateDepsMap.containsKey(fromTab + "/" + exprPart));
//					    			System.out.println(" key returned " + fromTab + " ??? " + this.getActualLineageForInternalObject(fromTab + "/" + exprPart));
						    		if (privateDepsMap.containsKey(fromKey)) {
						    			// get the actual lineage colum upstream
						    			String retStr = this.getActualLineageForInternalObject(fromKey);
						    			leftId = databaseName + "/" + fromDB + "/" + retStr;
		//								linksWriter.writeNext(new String[] {"!!indirect!!core.DirectionalDataFlow",leftId,rightId});
										linksWriter.writeNext(new String[] {"core.DirectionalDataFlow",leftId,rightId});
										// @TODO: table level lineage here too	
						    		} else {
//									linksWriter.writeNext(new String[] {"EXPR.core.DirectionalDataFlow",leftId,rightId});
						    			linksWriter.writeNext(new String[] {"core.DirectionalDataFlow",leftId,rightId});
						    		}
					    		}
					    		expressionsProcessed++;
					    	}
				    	}
				    }

			        
			        rsDeps.close();
			        
			        System.out.println("\t\t\t" + "viewLineageLinks=" + tabLvlLineageCount + " columnLineage=" + colLvlLineageCount + " private dependencies:" + privateDeps);
			    } catch (SQLException e ) {
			        e.printStackTrace();
			    }
			        **/
				
			} // each view
		}
		
		
		System.out.println("extra processing to tables...");
		String tableSourcedFromSQL="SELECT * FROM GET_SOURCE_TABLE() " + 
				"WHERE input_database_name = ? " + 
				"AND input_view_name = ?";
		PreparedStatement tableDeps = null;
		// for each schema - then view...
		int allCustLineageCount=0;
		
		for (String schema : tableDbNameMap.keySet()) {
			System.out.println("\tschema=" + schema);
			for (String table : tableDbNameMap.get(schema)) {
				System.out.println("\t\ttable=" + table);
				
				try {
					tableDeps = connection.prepareStatement(tableSourcedFromSQL);
					tableDeps.setString(1, schema);
					tableDeps.setString(2, table);

			        ResultSet rsDeps = tableDeps.executeQuery();
			        
//			        System.out.println("\t\tquery executed to get dependencies using COLUMN_DEPENDENCIES() call");
//			        int tabLvlLineageCount=0;
			        int custLineageCount=0;
				    while(rsDeps.next()) {
				    	String fromSchema = rsDeps.getString("source_schema_name");
				    	String fromCatalog = rsDeps.getString("source_catalog_name");
				    	if (fromSchema==null) {
				    		fromSchema=fromCatalog;
				    	}
				    	String fromTab = rsDeps.getString("source_table_name");
				    	String fromSQL = rsDeps.getString("sqlsentence");
				    	String viewWrapperName = rsDeps.getString("view_wrapper_name");
				    	System.out.println("\t\t\t\tview wrapper=" + viewWrapperName);
				    	String connectionName = getConnectionNameFromWrapper(schema, viewWrapperName);

//				    	System.out.println("\t\t\tsourced from: connectio= " + connectionName + " " + fromSchema + "." + fromTab + " sql=" + fromSQL);
				    	// get the columns for the table...
				    	// refactor - call a function to do this
				    	Map<String, List<String>> tableMap = colDbNameMap.get(schema);
				    	for (String col : tableMap.get(table)) {
//				    		System.out.println("\t\t\t\tColumn lineage: " + col);
				    		
				    		String leftId = fromSchema + "/" + fromTab + "/" + col;
				    		String rightId = schema + "/" + table + "/" + col;
				    		String[] custLineage = new String[] {"",connectionName, "denodo_vdp",leftId,rightId};
				    		custLineageCount++;
				    		allCustLineageCount++;
				    		custLineageWriter.writeNext(custLineage);
//				    		System.out.println(Arrays.toString(custLineage));
				    	}

				    }
			        
			        rsDeps.close();
			        System.out.println("\t\t\t" + "custom lineage links written=" + custLineageCount);
			    } catch (SQLException e ) {
			    	System.out.println("\n**************  error executing query." + e.getMessage() + "\n**************  end of error");
//			        e.printStackTrace();
			    } catch (Exception ex) {
			    	System.out.println("unknown error" + ex.getMessage());
			    	ex.printStackTrace();
		    	
			    }

				System.out.println("total custom lineage links created: " + allCustLineageCount);
				
				
				
			} // each table
		}
		
		/**
		System.out.println("getting datasources JDBC:");
		try {
			Statement stJDBC = connection.createStatement(); 
			ResultSet rsJDBC = stJDBC.executeQuery("LIST DATASOURCES JDBC");
		    while(rsJDBC.next()) {
		    	String jdbcConnection = rsJDBC.getString("name");
		    	System.out.println("jdbc connection = " + jdbcConnection);
		    }

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/


		
		return;
	}
	

	/**
	 * get the connection name and database name (if known) 
	 * @param database - the database the wrapped table is from
	 * @param wrapper - the name of the wrapped object
	 * @return
	 */
	String getConnectionNameFromWrapper(String database, String wrapper) {
		String connectionName = "";

		// execute DESC VQL WRAPPER JDBC <db>.<wrapper>;
		String query="DESC VQL WRAPPER JDBC " + database + "." + wrapper;
		PreparedStatement wrapperStmnt = null;
//		String wrapperQuery = "DESC VQL WRAPPER JDBC ";

        try {
			Statement st = connection.createStatement(); 
			ResultSet rs = st.executeQuery("DESC VQL WRAPPER JDBC " + database + "." + wrapper);
			while (rs.next()) {
//				System.out.println("\t\twrapper.....");
				String result = rs.getString("result");
//				System.out.println("result=\n" + result);
				
				// now we need to extract the DATASOURCENAME='<name>'
				int dsStart = result.indexOf("DATASOURCENAME=");
//				System.out.println("start pos=" + dsStart + " total length=" + result.length());
				int dsEnd = result.indexOf("\n", dsStart);
//				System.out.println("start end=" + dsEnd + " total length=" + result.length());
				if (dsStart>0 && dsEnd < result.length()) {
					connectionName = result.substring(dsStart+15, dsEnd);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return connectionName;
	}

	
	protected boolean initFiles() {
		super.initFiles();
		// assume working, until it is not
		boolean initialized=true;
		String outFolder = customMetadataFolder + "_lineage";
		String lineageFileName = outFolder + "/" + "denodo_lineage.csv";
		System.out.println("Step 3.1: initializing denodo specific files: " + lineageFileName);

		try { 
			// check that the folder exists - if not, create it
			File directory = new File(String.valueOf(outFolder));
			if(!directory.exists()){
				System.out.println("\tfolder: " + outFolder + " does not exist, creating it");
				directory.mkdir();
			}
			//			otherObjWriter = new CSVWriter(new FileWriter(otherObjectCsvName), ',', CSVWriter.NO_QUOTE_CHARACTER); 
			custLineageWriter = new CSVWriter(new FileWriter(lineageFileName)); 
			custLineageWriter.writeNext(new String[]{"association","From Connection","To Connection","From Object","To Object"});

			System.out.println("\tDenodo Scanner Files initialized");

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
		System.out.println("closing denodo specific files");

		try { 
			custLineageWriter.close(); 
		} catch (IOException e) { 
			// TODO Auto-generated catch block 
			e.printStackTrace(); 
			return false;
		} 
		
		
		System.out.println("expressions found/links created: " + expressionsFound + "/" + expressionLinks);
		System.out.println("lineage links written:  viewlevel=" + totalTableLineage + " columnLevel=" + totalColumnLineage);

		super.closeFiles();

		return true;

	}


}
