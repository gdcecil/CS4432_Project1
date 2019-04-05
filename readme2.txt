Students: Griffin Cecil, Michael Warms

CS4432 Project 1

*/////////////////////////////*
*//Installation Instructions//*
*/////////////////////////////*

1. Download and unzip the files included in GCMWProject1.zip to your desktop directory
2. Open ECLIPSE
3. Go to File -> Import and select General>Existing projects into workspace
4. Once the file is loaded into the project explorer, open CS4432_Project1/src/server
5. Right click on Startup.java and select Run Configurations
6. Select Startup from the menu on the left
7. Navigate to the Arguments tab in the window on the right
8. Enter "CS4432" without quotation marks into the box labeled Program Arguments
(NOTE: you may change "CS4432" to whatever you want. If you want to re-run the database
you might need to delete this file to ensure the database is deleted.)
9. Select Apply and then Run at the bottom of the window
10. The database will now be ready to run.


*////////////////////////////////*
*// Running SQL examples       //*
*////////////////////////////////*

Before running the SQL examples found in Examples.java, make sure you follow the
installation instructions above and that the startup server is running.

NOTE: The provided Examples.sql file is a read only file, Examples.java contains
a working version of the SQL statements wrapped in JDBC.

1. In Eclipse, open the project to the root directory, CS4432_Project1
2. In the project explorer, navigate to CS4432_Project1/studentClient/simpledb
3. Right-click on Examples.java
4. Select Run As > Java Application
5. The output will be displayed in the console. You may need to switch the
displayed console to view the ouput.


*////////////////////////////////*
*// Running Buffer Tests       //*
*////////////////////////////////*

In order to run the buffer tests, the startup server does NOT need to be running.

NOTE: The output of this file has already been provided and annotated in
Testing.txt for your convenience.

1. In Eclipse, open the project to the root directory, CS4432_Project1
2. In the project explorer, navigate to CS4432_Project1/src/simpledb.buffer
3. Right-click on AdvancedBufferMgrTest.java
4. Select Run As > Java Application
5. The output will be displayed in the console. You may need to switch the
displayed console to view the ouput.


DELIVERABLES
- README.txt which you are currently reading
- Examples.java located in CS4432_Project1/studentClient/simpledb so that you can run it
	see file documentation for further details
- Design.txt zipped together with ExtendedSimpleDB.zip, enjoy that one
- Testing.txt zipped together with ExtendedSimpleDB.zip
	AdvancedBufferMgrTest is the file used to create Testing.txt, refer to that file for
	further details and implementation
- Bugs.txt which is pretty much empty, but is also zipped up here