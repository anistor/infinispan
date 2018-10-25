echo "-------------------- JAVADOC OUT -------------------------" > javadoc.txt
mvn -B javadoc:javadoc | grep -v "Generating /home/adrian/work/" | grep -v "Loading source files for" | grep -v "Building tree" | grep -v "Building index" | grep -v "\[INFO\] ---" | grep -v "\[INFO\] >>> " | grep -v "\[INFO\] <<< " | grep -v "^\[INFO\] $" >> javadoc.txt

echo "-------------------- JAVADOC OUT FOR TESTS -------------------------" >> javadoc.txt
mvn -B javadoc:test-javadoc | grep -v "Generating /home/adrian/work/" | grep -v "Loading source files for" | grep -v "Building tree" | grep -v "Building index" | grep -v "\[INFO\] ---" | grep -v "\[INFO\] >>> " | grep -v "\[INFO\] <<< " | grep -v "^\[INFO\] $" >> javadoc.txt
