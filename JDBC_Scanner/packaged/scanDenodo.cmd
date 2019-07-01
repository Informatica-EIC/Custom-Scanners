

propfile=%1


java -cp "denodoCustomScanner.jar:lib/*" com.infa.edc.scanner.jdbc.DenodoScanner ${propfile} %2

#complete
echo 'Finished'
