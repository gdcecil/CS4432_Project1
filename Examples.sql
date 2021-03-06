/* This file is a conversion of Examples.java*/
/* into pure SQL statements. This file is for*/
/* reference only. If you would like to run  */
/* this file, follow the instructions in     */
/* README.txt to run Examples.java. The      */
/* output for these statements can also be   */
/* found in Examples.java.                   */

CREATE TABLE "TEST"
	(
	"ID" INTEGER,
	"Name" VARCHAR(20),
	"Year" INTEGER
	);

INSERT INTO TEST VALUES('0', 'rob', '2222');
INSERT INTO TEST VALUES('1', 'greg', '2018');
INSERT INTO TEST VALUES('2', 'may', '2017');
INSERT INTO TEST VALUES('3', 'dan', '1334');
INSERT INTO TEST VALUES('4', 'ted', '1335');
INSERT INTO TEST VALUES('5', 'alice', '1999');
INSERT INTO TEST VALUES('6', 'carl', '2002');
INSERT INTO TEST VALUES('7', 'peg', '1922');
INSERT INTO TEST VALUES('8', 'sue', '3000');
INSERT INTO TEST VALUES('9', 'meg', '2002');
INSERT INTO TEST VALUES('10', 'carl', '3000');

CREATE TABLE "TESTMORE"
	(
	"moreID" INTEGER,
	"Status" VARCHAR(8),
	"Cost" INTEGER
	);

INSERT INTO TESTMORE VALUES('1', 'dead', '-232');
INSERT INTO TESTMORE VALUES('3', 'dying', '100');
INSERT INTO TESTMORE VALUES('5', 'dead', '2');
INSERT INTO TESTMORE VALUES('7', 'alive', '5');
INSERT INTO TESTMORE VALUES('9', 'alive', '700000');
INSERT INTO TESTMORE VALUES('10', 'alive', '5');

SELECT NAME
FROM TEST;

SELECT Name, Status
FROM TEST, TESTMORE
WHERE ID = moreID;

SELECT Name, Cost
FROM TEST, TESTMORE
WHERE ID = moreID
AND Year = 3000
Cost = 5;

SELECT Cost
FROM TESTMORE;

DELETE FROM TESTMORE
WHERE moreID = 9;

SELECT moreID, Status, Cost
FROM TESTMORE

UPDATE TEST
SET Name = 'fred'
WHERE ID = 5

SELECT ID, Name, Year
FROM TEST
