JDBC custom scanner
---------------------------------

useful for situations where the standard edc generic jdbc scanner is not working, or you want to run some isolated tests

pre-requisite:  java 1.8+ is installed and in system path

to download:-
- from linux/macos command-line
	wget -O jdbcCustomScanner.zip https://github.com/Informatica-EIC/Custom-Scanners/blob/master/JDBC_Scanner/packaged/jdbcCustomScanner.zip?raw=true
- from github
	https://github.com/Informatica-EIC/Custom-Scanners/blob/master/JDBC_Scanner/packaged/jdbcCustomScanner.zip?raw=true
	
to configure
- unzip jdbcCustomScanner.zip to any folder you choose (windows/linux/macos)
- edit jdbc.properties (or copy to a name of your choice)
	add settings for:-
		- driverClass
		- URL
		- user
		- pwd
			Note:  if pwd set to <prompt> - then you will be prompted for a password
		- optionally set
			catalog		(some jdbc drivers you need to specify a catalog, leave blank to start with)
			schema		(a filter for schemas - empty = all)
			tables   	(a filter for tables - empty = all)
	
- copy your jdbc driver jar file(s) to the lib folder
		if you don't, you will get a classNotFound error
		
to run:
- linux/macos  (using jdbc.properties - substitute for any copies you made for other databases)
	./jdbcCustomScan.sh jdbc.properties
- windows  (powershell or cmd)
	./jdbcCustomScan.sh jdbc.properties
	
output will be written to jdbcScanner_out & messages to console