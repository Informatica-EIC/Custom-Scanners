package com.infa.edc.scanner.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DenodoScanner extends GenericScanner {
	protected String databaseName="";
	
	protected Map<String, List<String>> viewDbNameMap = new HashMap<String, List<String>>();

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
			System.out.println("Denodo Custom scanner for EDC: missing configuration properties file: usage:  genericScanner <folder>/<config file>.properties");	
		} else {
			System.out.println("Denodo Custom scanner: " + args[0] + " currentTimeMillis=" +System.currentTimeMillis());
			DenodoScanner scanner = new DenodoScanner(args[0]);
			
			scanner.run();	

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
		        
				if (exportCatalog(schemaName)) {
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
	
	
	protected void getViews(String catalogName, String schemaName) {
		try {
			ResultSet rsViews = dbMetaData.getTables(schemaName, null, null, new String[] { "VIEW" });
			int viewCount = 0;
			while (rsViews.next()) {
				// Print
				viewCount++;
				String viewName = rsViews.getString("TABLE_NAME");
				
				List<String> values = viewDbNameMap.get(schemaName);
				if (values==null) {
					values = new ArrayList<String>();
				}
				values.add(viewName);
				viewDbNameMap.put(schemaName, values);

				// System.out.println("found one...");
				System.out.println("\t" + " catalog=" + rsViews.getString("TABLE_CAT") + " schema="
						+ rsViews.getString("TABLE_SCHEM") + " viewname=" + viewName
						+ " TABLE_TYPE=" + rsViews.getString("TABLE_TYPE")
//						+ " comments=" + rsViews.getClob("REMARKS")
						);
				//				System.out.println(rsTables.getMetaData().getColumnTypeName(5));
				this.createView(catalogName, schemaName, viewName, rsViews.getString("REMARKS"), "");

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
                
//                System.out.println("\t\t\tcolumnn=" + catalogName + "/" + schemaName + "/" + tableName+ "/" + columnName+ "/" + typeName+ "/" + columnsize+ "/" + pos);

//        		createColumn( );
                this.createColumn(catalogName, schemaName, tableName, columnName, typeName, columnsize, pos, isView);

                		
		    }  // end for each column
    	} catch (Exception ex) {
    		System.out.println("error extracting column metadata...");
    		ex.printStackTrace();
    	}
	    System.out.println("\t\t\tcolumns extracted: " + colCount);

	}
	
	
	/**
	 * extra processing can call some dbms specific functions
	 * e.g. internal linage for denodo
	 * external lineage (back to s3 files) for athena
	 */
	protected void extraProcessing() {
		System.out.println("extracting internal denodo lineage:");
		
		// unique list of dataflows...
		Set datasetFlows = new HashSet();
		
		PreparedStatement deps = null;
		String dependenciesQuery = "select input_view_database_name, input_view_name, " +
				"view_name, column_name, dependency_database_name, dependency_name, " +
				"dependency_column_name, expression, depth " + 
				"from  COLUMN_DEPENDENCIES (?, ?, null)  " + 
				"where depth=1 and view_name=?  " + 
				"order by input_view_name, view_name, column_name, depth;";
		
		// for each schema - then view...
		for (String schema : viewDbNameMap.keySet()) {
			System.out.println("\tschema=" + schema);
			for (String view : viewDbNameMap.get(schema)) {
				System.out.println("\t\tview=" + view);
				
				try {
			        deps = connection.prepareStatement(dependenciesQuery);
			        deps.setString(1, schema);
			        deps.setString(2, view);
			        deps.setString(3, view);

			        ResultSet rsDeps = deps.executeQuery();
			        
//			        System.out.println("\t\tquery executed to get dependencies using COLUMN_DEPENDENCIES() call");
			        int tabLvlLineageCount=0;
			        int colLvlLineageCount=0;
				    while(rsDeps.next()) {
				    	String fromDB = rsDeps.getString("dependency_database_name");
				    	String fromTab = rsDeps.getString("dependency_name");
				    	String fromCol = rsDeps.getString("dependency_column_name");
				    	String toCol = rsDeps.getString("column_name");
				    	
//				    	System.out.println(fromDB +"/" +fromTab + "/" + fromCol + "==>>" + schema + "/" + view + "/" + toCol);
				    	String lefttabId = databaseName + "/" + fromDB + "/" +fromTab;
				    	String righttabId = databaseName + "/" + schema + "/" +view;
				    	String leftId = databaseName + "/" + fromDB + "/" +fromTab + "/" + fromCol;
				    	String rightId = databaseName + "/" + schema + "/" +view + "/" + toCol;
				    	// field level flow...
				    	if (!datasetFlows.contains(lefttabId + ":" + righttabId)) {
							linksWriter.writeNext(new String[] {"core.DataSetDataFlow",lefttabId,righttabId});
							tabLvlLineageCount++;
							datasetFlows.add(lefttabId + ":" + righttabId);
				    		
				    	}
						linksWriter.writeNext(new String[] {"core.DirectionalDataFlow",leftId,rightId});
						colLvlLineageCount++;

				    }

			        
			        rsDeps.close();
			        System.out.println("\t\t\t" + "viewLineageLinks=" + tabLvlLineageCount + " columnLineage=" + colLvlLineageCount);
			    } catch (SQLException e ) {
			        e.printStackTrace();
			    }
				
			} // each view
		}
		return;
	}


}
