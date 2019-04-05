Students: Griffin Cecil, Michael Warms

CS4432 Project 1

*/////////////////////////////*
*//Installation Instructions//*
*/////////////////////////////*

1. Download and unzip the files included in GCMWProject1.zip to your desktop directory
2. After unzipping, unzip ExampleSimpleDB.zip
2. Open ECLIPSE
3. Go to File -> Import and select General>Existing projects into workspace.
4. For Select root directly, input the folder simpledb2.10 from the unzipped files and select Finish
5. Once the file is loaded into the project explorer, open simpledb2.10/src/simpledb.server
6. Right click on Startup.java and select Run As > Run Configurations
7. Select Startup. Depending on your Eclipse version, you may need to create a new configuration. If so:
	- Create a new Java Application
	- Project should be bet to simpledb2.10
	- Main Class should be set to simpledb.server.startup
8. Navigate to the Arguments tab in the window on the right
9. Enter "CS4432" without quotation marks into the box labeled Program Arguments
(NOTE: you may change "CS4432" to whatever you want. If you want to re-run the database
you might need to delete this file to ensure the database entries are deleted.)
10. Select Apply and then Run at the bottom of the window
11. The database will now run.


*////////////////////////////////*
*// Running SQL examples       //*
*////////////////////////////////*

Before running the SQL examples found in Examples.java, make sure you follow the
installation instructions above and that the startup server is running.

NOTE: The provided Examples.sql file is a read only file, Examples.java contains
a working version of the SQL statements wrapped in JDBC.

1. In Eclipse, open the project to the root directory, simpledb2.10
2. In the project explorer, navigate to simpledb2.10/studentClient/simpledb
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

1. In Eclipse, open the project to the root directory, simpledb2.10
2. In the project explorer, navigate to simpledb2.10/src/simpledb.buffer
3. Right-click on AdvancedBufferMgrTest.java
4. Select Run As > Java Application
5. The output will be displayed in the console. You may need to switch the
selected console to view the ouput.


DELIVERABLES
- README.txt which you are currently reading

- Examples.sql is a read-only file containing the database tests found in Examples.java.
	
	Examples.java is located in CS4432_Project1/studentClient/simpledb so that you can 
	run it. See the instructions above and file documentation for further details.
	
- Design.txt zipped together with ExtendedSimpleDB.zip, enjoy that one

- Testing.txt zipped together with ExtendedSimpleDB.zip
	AdvancedBufferMgrTest is the file used to create Testing.txt, refer to that file for
	further details and implementation
	
- Bugs.txt which is pretty much empty, but is also zipped up here