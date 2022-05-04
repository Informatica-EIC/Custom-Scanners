/**
 *
 */
package com.infa.edc.scanner.jdbc;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.swing.JOptionPane;
import javax.swing.JPasswordField;

import com.opencsv.CSVWriter;

/**
 * @author Administrator
 *
 */
public class GenericScanner implements IJdbcScanner {
    public static final String version = "0.9.2";
    public String propertyFileName;
    public String driverClass;
    public String dbURL;
    public String userName;
    public String pwd;
    public String catalogFilter;
    public String excludedSchemas = "";

    public Connection connection;
    public DatabaseMetaData dbMetaData;

    protected String customMetadataFolder;

    protected String dbProductName;

    protected static String DISCLAIMER = "\n************************************ Disclaimer *************************************\n"
            + "By using this custom scanner, you are agreeing to the following:-\n"
            + "- this custom scanner is not officially supported by Informatica\n"
            + "  it was created for situations where the generic JDBC scanner\n"
            + "  does not produce the correct results or fails to extract any metadata.\n"
            + "- It has only been tested with limited test/cases, and may report exceptions or fail.\n"
            + "- Issues can be created on githib:- \n"
            + "  https://github.com/Informatica-EIC/Custom-Scanners  (JDBC_Scanner folder)\n"
            + "*************************************************************************************\n" + "\n";

    // constants
    protected String DB_TYPE = "com.infa.ldm.relational.Database";
    protected String SCH_TYPE = "com.infa.ldm.relational.Schema";
    protected String TAB_TYPE = "com.infa.ldm.relational.Table";
    protected String COL_TYPE = "com.infa.ldm.relational.Column";
    protected String VIEW_TYPE = "com.infa.ldm.relational.View";
    protected String VIEWCOL_TYPE = "com.infa.ldm.relational.ViewColumn";

    protected String CATALOG_SCHEMA_FILENAME = "objects-catalogAndSchemas.csv";
    protected String TABLEVIEWS_FILENAME = "objects-tables.csv";
    protected String VIEWS_FILENAME = "objects-views.csv";
    protected String COLUMN_FILENAME = "objects-columns.csv";
    protected String VCOLUMN_FILENAME = "objects-viewColumns.csv";
    protected String LINKS_FILENAME = "links.csv";

    // file variables
    protected CSVWriter otherObjWriter = null;
    protected CSVWriter tableWriter = null;
    protected CSVWriter viewWriter = null;
    protected CSVWriter columnWriter = null;
    protected CSVWriter viewColumnWriter = null;
    protected CSVWriter linksWriter = null;

    // object counters
    protected int dbCount = 0;
    protected int schCount = 0;
    protected int tabCount = 0;
    protected int colCount = 0;
    protected int vwCount = 0;
    protected int vwColCount = 0;

    public static boolean showDisclaimer() {
        System.out.println(DISCLAIMER);
        Console c = System.console();
        String response;
        boolean hasAgreed = false;
        if (c == null) { // IN ECLIPSE IDE (prompt for password using swing ui
            System.out.println("no console found...");
            final JPasswordField pf = new JPasswordField();
            String message = "Do you agree to this disclaimer? Y or N ";
            response = JOptionPane.showConfirmDialog(null, pf, message, JOptionPane.OK_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE) == JOptionPane.OK_OPTION ? new String(pf.getPassword())
                            : "response (Y|N)";
        } else { // Outside Eclipse IDE (e.g. windows/linux console)
            response = new String(c.readLine("agree (Y|N)? "));
        }
        System.out.println("user entered:" + response);
        if (response != null && response.equalsIgnoreCase("Y")) {
            hasAgreed = true;
        }

        return hasAgreed;
    }

    /**
     * read the property file to get db connection settings
     */
    public GenericScanner(String propertyFile) {
        System.out.println("GenericScanner" + " " + version + " initializing properties from: " + propertyFile);

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
                System.out.println("password set to <prompt> for user " + userName + " - waiting for user input...");
                pwd = getPassword();
                // System.out.println("pwd chars entered (debug): " + pwd.length());
            }

            customMetadataFolder = prop.getProperty("customMetadata.folder", "custom_metadata_out");
            if (customMetadataFolder == null || customMetadataFolder.equals("")) {
                System.out.println("empty value set for custom metadata output folder: using 'custom_metadata_out'");
                customMetadataFolder = "custom_metadata_out";
            }

            excludedSchemas = prop.getProperty("excluded.schemas", "");
            if (excludedSchemas == null) {
                excludedSchemas = "";
            }

            catalogFilter = prop.getProperty("catalog", "");

            System.out.println("scanner settings from:" + propertyFile);
            System.out.println("\tdriver=" + driverClass);
            System.out.println("\turl=" + dbURL);
            System.out.println("\tuser=" + userName);
            System.out.println("\tpwd=" + pwd.replaceAll(".", "*"));
            System.out.println("\tout folder=" + customMetadataFolder);
            System.out.println("\tcatalog filter=" + catalogFilter);
            System.out.println("\tschemas to exclude=" + excludedSchemas);

        } catch (Exception e) {
            System.out.println("error reading properties file: " + propertyFile);
            e.printStackTrace();
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see com.infa.edc.scanner.jdbc.IJdbcScanner#getConnection(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public Connection getConnection(String classType, String url, String user, String pwd) {
        System.out
                .println("Step 1: validating jdbc driver class: " + classType + " using:" + this.getClass().getName());
        try {
            Class.forName(classType);
        } catch (ClassNotFoundException e) {
            System.out.println("\tunable to find class: " + classType + " " + e.getClass().getName() + " exiting...");
            return null;
        }
        System.out.println("\tjdbc driver class validated successfully!");

        // valid driver class - now try the actual connection
        System.out.println(
                "Step 2: Attempting to connect to database using url=" + url + " using: " + this.getClass().getName());
        try {
            Connection con = DriverManager.getConnection(url, user, pwd);
            // connection successful - return the connection object
            System.out.println("\tconnection successful!");
            return con;
        } catch (SQLException e) {
            System.out.println("connection failed for url=" + url + " " + e.getClass().getName() + "");
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
            System.out.println("\t" + this.getClass().getName() + " - no connection - exiting...");
            return;
        } else {
            // we have a connection - continue...
            long start = System.currentTimeMillis();
            initFiles();

            System.out.println("\t" + this.getClass().getName() + " ready to start extracting databse metadata!");

            System.out.println("Step 4: getting databaseMetadata object from connection");
            try {
                dbMetaData = connection.getMetaData();
                String allV = dbMetaData.getDatabaseProductVersion();
                dbProductName = dbMetaData.getDatabaseProductName();

                System.out.println("\tgetDatabaseProductVersion=" + allV);
                System.out.println("\tgetDatabaseProductName=" + dbProductName);
                System.out.println("\tcatalog term=" + dbMetaData.getCatalogTerm());
                System.out.println("\tschema term=" + dbMetaData.getSchemaTerm());
                System.out.println("\tDatabaseMajorVersion=" + dbMetaData.getDatabaseMajorVersion());
                System.out.println("\tDatabaseMinorVersion=" + dbMetaData.getDatabaseMinorVersion());
                System.out.println("\tDriverMajorVersion=" + dbMetaData.getDriverMajorVersion());
                System.out.println("\tDriverMinorVersion=" + dbMetaData.getDriverMinorVersion());
                System.out.println("\tDriverName=" + dbMetaData.getDriverName());
                System.out.println("\tJDBCMajorVersion=" + dbMetaData.getJDBCMajorVersion());
                System.out.println("\tDriverVersion=" + dbMetaData.getDriverVersion());
                System.out.println("\tCatalogSeparator=" + dbMetaData.getCatalogSeparator());

            } catch (SQLException e) {
                System.out.println("\terror getting DatabaseMetaData object from connection - exiting");
                e.printStackTrace();
                return;
            }

            getCatalogs();
            long end1 = System.currentTimeMillis();
            long totalMillis = end1 - start;
            String timeTaken = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(totalMillis),
                    TimeUnit.MILLISECONDS.toSeconds(totalMillis)
                            - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalMillis)));
            System.out.println("getCatalogs() time: " + timeTaken);

            extraProcessing();
            long end2 = System.currentTimeMillis();
            totalMillis = end2 - end1;
            timeTaken = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(totalMillis),
                    TimeUnit.MILLISECONDS.toSeconds(totalMillis)
                            - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalMillis)));
            System.out.println("extraProcessing() time: " + timeTaken);

            // object counts
            System.out.println("object counts: ");
            System.out.println("\tdatabases=" + dbCount);
            System.out.println("\t  schemas=" + schCount);
            System.out.println("\t   tables=" + tabCount);
            System.out.println("\t  columns=" + colCount);
            System.out.println("\t    views=" + vwCount);
            System.out.println("\tview cols=" + vwColCount);

            // after all processes are finished- close the csv files
            closeFiles();
            totalMillis = end2 - start;
            timeTaken = String.format("%d min, %d sec", TimeUnit.MILLISECONDS.toMinutes(totalMillis),
                    TimeUnit.MILLISECONDS.toSeconds(totalMillis)
                            - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalMillis)));
            System.out.println("scanner time: " + timeTaken);
        }

    }

    /**
     * iterate over all catalogs (databases) there may be multiple need to determine
     * whether to default to extracting all, or only a subset
     */
    public void getCatalogs() {
        System.out.println("Step 5: getting catalogs:  DatabaseMetaData.getCatalogs()");

        ResultSet catalogs;
        try {
            catalogs = dbMetaData.getCatalogs();
            String catalogName;

            int catCount = 0;
            while (catalogs.next()) {
                catCount++;
                catalogName = catalogs.getString(1); // "TABLE_CATALOG"
                System.out.println("\tcatalog: " + catalogName);

                // create the catalog object
                if (isCatalogScanned(catalogName)) {
                    this.createDatabase(catalogName);

                    // get schemas
                    getSchemas(catalogName);
                } else {
                    // message for catalog is not exported...
                    System.out.println(
                            "\tcatalog=" + catalogName + " skipped - not included in catalog filter: " + catalogFilter);
                }
            }

            // note: for some databases/drivers - getCatalogs returns nothing (e.g gemfire)
            // so we are going to create a database for each entry in catalogFilter & then
            // call getSchemas for each entry too
            // this worked for gemfire

            if (catCount == 0) {
                String[] catalogFilterParts = catalogFilter.split(",");
                System.out.println(
                        "no catalogs found using dbMetaData.getCatalogs(); - forcing catalog=" + catalogFilterParts);
                // System.out.println("filter conditions for catalog " + catalogs);
                for (String catFilter : catalogFilterParts) {
                    this.createDatabase(catFilter.trim());
                    getSchemas(catFilter.trim());
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * determine if the catalog should be exported or not - depends on filtering
     * conditions e.g. if catalog=<name>,<name> - then filter in from this list e.g.
     * if catalog=<name>,!<name> - then filter out any with !
     *
     * @param catalogName
     * @return
     */
    protected boolean isCatalogScanned(String catalogName) {
        // default to all
        if (catalogFilter.equals("")) {
            // no filtering - extract them all...
            return true;
        }
        // split the catalog filter by ,
        String[] catalogs = catalogFilter.split(",");
        // System.out.println("filter conditions for catalog " + catalogs);
        for (String catFilter : catalogs) {
            // System.out.println("does " + catFilter + "=" + catalogName + " " +
            // catFilter.equalsIgnoreCase(catalogName));
            if (catFilter.equalsIgnoreCase(catalogName)) {
                return true;
            }
        }
        // if (catalogFilter.contains(catalogName)) {
        // return true;
        // } else {
        // return false;
        // }
        return false;
    }

    /**
     * return true if the schema should be scanned, false if not use properties set
     * for the scanner to determine what to filter in/out
     *
     * @param schemaName
     * @return true|false
     */
    protected boolean isSchemaScanned(String schemaName) {
        // @TODO: refactor - allow for a list of schemas to scan too
        // System.out.println("should schema be scanned: " + schemaName + "
        // excludeList=" + excludedSchemas);
        if (this.excludedSchemas.equals("")) {
            return true;
        }

        if (excludedSchemas.contains(schemaName)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * get the schemas for a catalog
     *
     * @param catalogName
     */
    public void getSchemas(String catalogName) {
        try {
            System.out.println("Step 6: extracting schemas for catalog: " + catalogName);
            ResultSet schemas = dbMetaData.getSchemas(catalogName, null);
            int schemaCount = 0;
            while (schemas.next()) {
                schemaCount++;
                String schemaName = schemas.getString("TABLE_SCHEM");
                System.out.println("\tschema is: " + schemaName);
                if (!isSchemaScanned(schemaName)) {
                    System.out.println("\tschema filtered out - not processed: " + schemaName);
                    // go ahead
                } else {
                    createSchema(catalogName, schemaName);

                    // process tables
                    getTables(catalogName, schemaName);

                    // process views
                    getViews(catalogName, schemaName);
                }

            }
            System.out.println("\tSchemas extracted: " + schemaCount);

        } catch (Exception ex) {
            System.out.println("Error getting list of databases using: getSchemas. " + ex.getMessage());
        }
    }

    /**
     * find all table objects
     *
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
                // + " comments=" + rsTables.getClob("REMARKS")
                );
                // System.out.println(rsTables.getMetaData().getColumnTypeName(5));
                this.createTable(catalogName, schemaName, rsTables.getString("TABLE_NAME"),
                        rsTables.getString("REMARKS"));

                getColumnsForTable(catalogName, schemaName, rsTables.getString("TABLE_NAME"), false);
            }

            System.out.println("\tTables extracted: " + tableCount);

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * find all table objects
     *
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
                // + " comments=" + rsViews.getClob("REMARKS")
                );
                // System.out.println(rsTables.getMetaData().getColumnTypeName(5));
                this.createView(catalogName, schemaName, rsViews.getString("TABLE_NAME"), rsViews.getString("REMARKS"),
                        "", "");

                getColumnsForTable(catalogName, schemaName, rsViews.getString("TABLE_NAME"), true);
            }
            System.out.println("\tViews extracted: " + viewCount);

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    protected void getColumnsForTable(String catalogName, String schemaName, String tableName, boolean isView) {
        int colCount = 0;
        try {
            ResultSet columns = dbMetaData.getColumns(catalogName, schemaName, tableName, null);
            while (columns.next()) {
                colCount++;
                String columnName = columns.getString("COLUMN_NAME");
                // String datatype = columns.getString("DATA_TYPE");
                String typeName = columns.getString("TYPE_NAME");
                String columnsize = columns.getString("COLUMN_SIZE");
                // String decimaldigits = columns.getString("DECIMAL_DIGITS");
                // String isNullable = columns.getString("IS_NULLABLE");
                String remarks = columns.getString("REMARKS");
                // String def = columns.getString("COLUMN_DEF");
                // String sqlType = columns.getString("SQL_DATA_TYPE");
                String pos = columns.getString("ORDINAL_POSITION");
                // String scTable = columns.getString("SCOPE_TABLE");
                // String scCatlg = columns.getString("SCOPE_CATALOG");

                // System.out.println("\t\t\tcolumnn=" + catalogName + "/" + schemaName + "/" +
                // tableName+ "/" + columnName+ "/" + typeName+ "/" + columnsize+ "/" + pos);

                // createColumn( );
                this.createColumn(catalogName, schemaName, tableName, columnName, typeName, columnsize, pos, remarks,
                        isView);

            } // end for each column
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
        if (args.length == 0) {
            System.out.println(
                    "JDBC Custom scanner for EDC: missing configuration properties file: usage:  genericScanner <folder>/<config file>.properties");
            System.exit(0);
        }

        System.out.println("JDBC Custom scanner: " + args[0] + " currentTimeMillis=" + System.currentTimeMillis());

        // check to see if a disclaimer override parameter was passed
        String disclaimerParm = "";
        if (args.length >= 2) {
            // 2nd argument is an "agreeToDisclaimer" string
            disclaimerParm = args[1];
            // System.out.println("disclaimer parameter passed: " + disclaimerParm);
            if ("agreeToDisclaimer".equalsIgnoreCase(disclaimerParm)) {
                System.out.println(
                        "the following disclaimer was agreed to by passing 'agreeToDisclaimer' as 2nd parameter");
                System.out.println(DISCLAIMER);
            }
        }

        // 1 of 2 conditions must be true for the scanner to start
        // 1 - 2nd parameter is equal to "agreeToDisclaimer"
        // or (if no parm passed, or value does not match)
        // then the user must agree to the prompt
        if ("agreeToDisclaimer".equalsIgnoreCase(disclaimerParm) || showDisclaimer()) {
            GenericScanner scanner = new GenericScanner(args[0]);
            scanner.run();
        } else {
            System.out.println("Disclaimer was declined - exiting");
        }

    }

    protected boolean initFiles() {
        // assume working, until it is not
        boolean initialized = true;
        System.out.println("Step 3: initializing files in: " + customMetadataFolder);

        try {
            // check that the folder exists - if not, create it
            File directory = new File(String.valueOf(customMetadataFolder));
            if (!directory.exists()) {
                System.out.println("\tfolder: " + customMetadataFolder + " does not exist, creating it");
                directory.mkdir();
            }
            // otherObjWriter = new CSVWriter(new FileWriter(otherObjectCsvName), ',',
            // CSVWriter.NO_QUOTE_CHARACTER);
            otherObjWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + CATALOG_SCHEMA_FILENAME));
            tableWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + TABLEVIEWS_FILENAME));
            viewWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + VIEWS_FILENAME));
            this.columnWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + COLUMN_FILENAME));
            this.viewColumnWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + VCOLUMN_FILENAME));
            this.linksWriter = new CSVWriter(new FileWriter(customMetadataFolder + "/" + LINKS_FILENAME));

            otherObjWriter.writeNext(new String[] { "class", "identity", "core.name",
                    "com.infa.ldm.relational.StoreType", "com.infa.ldm.relational.SystemType" });
            tableWriter.writeNext(new String[] { "class", "identity", "core.name", "core.description" });
            viewWriter.writeNext(new String[] { "class", "identity", "core.name", "core.description",
                    "com.infa.ldm.relational.ViewStatement", "com.infa.ldm.relational.Location" });
            columnWriter.writeNext(new String[] { "class", "identity", "core.name", "com.infa.ldm.relational.Datatype",
                    "com.infa.ldm.relational.DatatypeLength", "com.infa.ldm.relational.Position", "core.dataSetUuid",
                    "core.description" });
            viewColumnWriter
                    .writeNext(new String[] { "class", "identity", "core.name", "com.infa.ldm.relational.Datatype",
                            "com.infa.ldm.relational.DatatypeLength", "com.infa.ldm.relational.Position",
                            "core.dataSetUuid", "com.infa.ldm.relational.ViewStatement", "core.description" });

            linksWriter.writeNext(new String[] { "association", "fromObjectIdentity", "toObjectIdentity" });

            System.out.println("\tFiles initialized");

        } catch (IOException e1) {
            initialized = false;
            e1.printStackTrace();
        }

        return initialized;
    }

    /**
     * close the files that were opened - ensures that any buffers are cleared
     *
     * @return
     */
    protected boolean closeFiles() {
        System.out.println("closing output files");

        try {
            otherObjWriter.close();
            tableWriter.close();
            viewWriter.close();
            columnWriter.close();
            viewColumnWriter.close();
            linksWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        /**
         * zip the files
         */
        List<String> srcFiles = Arrays.asList(customMetadataFolder + "/" + CATALOG_SCHEMA_FILENAME,
                customMetadataFolder + "/" + TABLEVIEWS_FILENAME, customMetadataFolder + "/" + VIEWS_FILENAME,
                customMetadataFolder + "/" + VCOLUMN_FILENAME, customMetadataFolder + "/" + COLUMN_FILENAME,
                customMetadataFolder + "/" + LINKS_FILENAME);

        try {
            System.out.println(
                    "creating zip file: " + customMetadataFolder + '/' + this.getClass().getSimpleName() + ".zip");
            FileOutputStream fos = new FileOutputStream(
                    customMetadataFolder + '/' + this.getClass().getSimpleName() + ".zip");
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            for (String srcFile : srcFiles) {
                File fileToZip = new File(srcFile);
                FileInputStream fis;
                fis = new FileInputStream(fileToZip);
                ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
                zipOut.putNextEntry(zipEntry);

                byte[] bytes = new byte[1024];
                int length;
                while ((length = fis.read(bytes)) >= 0) {
                    zipOut.write(bytes, 0, length);
                }
                fis.close();
            }
            zipOut.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;

    }

    protected void createDatabase(String dbName) {
        System.out.println("\tcreating database: " + dbName);

        try {
            this.otherObjWriter.writeNext(new String[] { DB_TYPE, dbName, dbName, "Relational", dbProductName });
            dbCount++;
            this.linksWriter.writeNext(new String[] { "core.ResourceParentChild", "", dbName });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    protected void createSchema(String dbName, String schema) {
        // System.out.println("\tcreating database: " + dbName);

        String schId = dbName + "/" + schema;

        try {
            this.otherObjWriter.writeNext(new String[] { SCH_TYPE, schId, schema, "", "" });
            schCount++;
            this.linksWriter.writeNext(new String[] { "com.infa.ldm.relational.DatabaseSchema", dbName, schId });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    public void createTable(String dbName, String schema, String table, String desc) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;

        try {
            this.tableWriter.writeNext(new String[] { TAB_TYPE, tabId, table, desc });
            tabCount++;
            this.linksWriter.writeNext(new String[] { "com.infa.ldm.relational.SchemaTable", schId, tabId });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    public void createView(String dbName, String schema, String table, String desc, String ddl, String location) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;

        try {
            this.viewWriter.writeNext(new String[] { VIEW_TYPE, tabId, table, desc, ddl, location });
            vwCount++;
            this.linksWriter.writeNext(new String[] { "com.infa.ldm.relational.SchemaView", schId, tabId });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    protected void createColumn(String dbName, String schema, String table, String column, String type, String length,
            String pos, String desc, boolean isView) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;
        String colId = tabId + "/" + column;

        try {
            if (!isView) {
                this.columnWriter.writeNext(new String[] { COL_TYPE, colId, column, type, length, pos, tabId, desc });
                colCount++;
                this.linksWriter.writeNext(new String[] { "com.infa.ldm.relational.TableColumn", tabId, colId });
            } else {
                this.viewColumnWriter
                        .writeNext(new String[] { VIEWCOL_TYPE, colId, column, type, length, pos, tabId, desc });
                vwColCount++;
                this.linksWriter.writeNext(new String[] { "com.infa.ldm.relational.ViewViewColumn", tabId, colId });
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    protected void createViewColumn(String dbName, String schema, String table, String column, String type,
            String length, String pos, String expression, String desc) {

        String schId = dbName + "/" + schema;
        String tabId = schId + "/" + table;
        String colId = tabId + "/" + column;

        try {
            this.viewColumnWriter.writeNext(
                    new String[] { VIEWCOL_TYPE, colId, column, type, length, pos, tabId, expression, desc });
            vwColCount++;
            this.linksWriter.writeNext(new String[] { "com.infa.ldm.relational.ViewViewColumn", tabId, colId });
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return;
    }

    /**
     * extra processing can call some dbms specific functions e.g. internal linage
     * for denodo external lineage (back to s3 files) for athena
     */
    protected void extraProcessing() {

        return;
    }

    /**
     * prompt the user for a password, using the console (default) for development
     * environments like eclipse, their is no standard console. so in that case we
     * open a swing ui panel with an input field to accept a password
     *
     * @return the password entered
     *
     * @author dwrigley
     */
    public static String getPassword() {
        String password;
        Console c = System.console();
        if (c == null) { // IN ECLIPSE IDE (prompt for password using swing ui
            final JPasswordField pf = new JPasswordField();
            String message = "User password:";
            password = JOptionPane.showConfirmDialog(null, pf, message, JOptionPane.OK_CANCEL_OPTION,
                    JOptionPane.QUESTION_MESSAGE) == JOptionPane.OK_OPTION ? new String(pf.getPassword())
                            : "enter your pwd here....";
            System.out.println("pwd=" + password);
        } else { // Outside Eclipse IDE (e.g. windows/linux console)
            password = new String(c.readPassword("User password: "));
        }
        return password;
    }

}
