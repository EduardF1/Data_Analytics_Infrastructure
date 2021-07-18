USE DM_Northwind


/***************************************************
Exercise 1
- Create table to store the sales promotions' names
***************************************************/
CREATE TABLE SalesPromotions(
PromotionName [nvarchar](50)
);

/***************************************************
- Insert all records from the .csv file (file where the promotions are located)
***************************************************/
BULK INSERT SalesPromotions FROM
'C:\Users\fisch\Desktop\import_of_data\promotions.csv' --- absolute file path
WITH
(
CODEPAGE = '1252' --- single-byte enconding
)

SELECT * FROM SalesPromotions; --- verify result

/***************************************************
		Exercise 2 - Data logger
***************************************************/

/***************************************************
	- Create table to store the environmental values from the text file
Note:
	Changes have been made to the file, its original state did not consist of
	valid data
***************************************************/

USE PrimarySchoolEnvironmentalValuesLog

CREATE TABLE EnvironmentalValues(
DeviceNumber nvarchar(50) NULL, --- attributes to store the data (strings)
Date nvarchar(50) NULL,			--- attributes to store the data (strings)
Time nvarchar(50) NULL,			--- attributes to store the data (strings)
Temperature nvarchar(50) NULL,	--- attributes to store the data (strings)
Humidity nvarchar(50) NULL,		--- attributes to store the data (strings)
CO2 nvarchar(50) NULL,			--- attributes to store the data (strings)
DewPt nvarchar(50) NULL			--- attributes to store the data (strings)
)

/***************************************************
	- Insert file contents into the above created table
***************************************************/
BULK INSERT EnvironmentalValues FROM --- BULK INSERT (Insert from file all at once)
'C:\Users\fisch\Desktop\DAI_Exam\import_of_data\Exercise_2\enviromental_values.txt'
WITH
(
CODEPAGE = '1252',  --- single-byte enconding
FIELDTERMINATOR = ';' --- attribute separator
)

SELECT * FROM EnvironmentalValues
DELETE FROM EnvironmentalValues
DROP TABLE EnvironmentalValues

/***************************************************
	- Creation of a new table to hold the previously inserted data
	(from the EnvironmentalValues table) in a more adequate format
	(attribute data types).
Note:
	This is done as initially the data was inserted with attributes
	of type nvarchar ("strings") and this can affect performance
***************************************************/
CREATE TABLE EnvironmentalValuesChangedDataType(
DeviceNumber int NULL,
Date date NULL,
Time nvarchar(10) NULL,
Temperature decimal(4,2) NULL,
Humidity decimal(4,2) NULL,
CO2 decimal(5,2) NULL,
DewPt decimal(4,2) NULL
)

DROP TABLE EnvironmentalValuesChangedDataType

/***************************************************
	- Insert file contents into the above created table 
	(changed attribute data types)
Note:
	Conversions are being made for each attribute
	except the time attribute.
***************************************************/
INSERT INTO EnvironmentalValuesChangedDataType(
deviceNumber,
date,
time,
temperature,
humidity,
co2,
dewPt
)
SELECT 
CONVERT(int, E.DeviceNumber), --- convert function (type, attribute)
CONVERT(date, E.Date),		  --- convert function (type, attribute)
E.Time, --- kept as a string
--- conversion of the file data to the right types (specific values used for performance considerations)
TRY_CONVERT(decimal(4,2),REPLACE(REPLACE(E.Temperature,'.',''),',','.')),	--- special case (String value to numeric)
TRY_CONVERT(decimal(4,2),REPLACE(REPLACE(E.Humidity,'.',''),',','.')),		--- special case (String value to numeric)
TRY_CONVERT(decimal(5,2),REPLACE(REPLACE(E.CO2,'.',''),',','.')),			--- special case (String value to numeric)
TRY_CONVERT(decimal(4,2),REPLACE(REPLACE(E.DewPt,'.',''),',','.'))			--- special case (String value to numeric)
FROM PrimarySchoolEnvironmentalValuesLog.dbo.EnvironmentalValues E


DELETE FROM PrimarySchoolEnvironmentalValuesLog.dbo.EnvironmentalValuesChangedDataType

SELECT * FROM PrimarySchoolEnvironmentalValuesLog.dbo.EnvironmentalValuesChangedDataType



/***************************************************
		Exercise 3 - Working with XML data (xQuery)
***************************************************/

/*
	Count all persons with 'Bachelors' - local XML schema
	CHARINDEX ( expressionToFind, expressionToSearch[ , start_location] )
*/
SELECT COUNT(1) FROM AdventureWorks2017.Person.Person WHERE CHARINDEX('Bachelors',CONVERT(VARCHAR(MAX), Demographics), 1) > 0;

/*
	Count all persons with 'Graduate Degree'
*/
SELECT COUNT(1) FROM AdventureWorks2017.Person.Person WHERE CHARINDEX('Graduate Degree',CONVERT(VARCHAR(MAX), Demographics), 1) > 0;

/*
	Query that return the top 25 records for the IndividualSurvey subschema
*/

WITH XMLNAMESPACES 
('http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey' AS ns) --- main schema namespace (root)
SELECT TOP 25 
LastName,
-- verify record existance (explicit usage of the singleton DP to ensure uniqueness - single instance of the node accessed)
Demographics.value('(/ns:IndividualSurvey/ns:TotalPurchaseYTD)[1]', 'decimal') AS --- reference sub-schema (first child), then sub-sub-schema (second child), return as decimal 
TotalPurchaseYTD,
Demographics.value('(/ns:IndividualSurvey/ns:Gender)[1]', 'char(1)') AS Gender, --- the arguments for value is the above mentioned structure and the data type for the returned selected node
Demographics.value('(/ns:IndividualSurvey/ns:YearlyIncome)[1]','char(20)') AS IncomeRange
FROM AdventureWorks2017.Person.Person
WHERE Demographics.exist('(/ns:IndividualSurvey/ns:Education[contains(.,"Graduate Degree")])')=1 --- check function (1 for true)
AND Demographics.exist('(/ns:IndividualSurvey/ns:Gender[contains(.,"M")])')=1
AND Demographics.value('(/ns:IndividualSurvey/ns:TotalPurchaseYTD)[1]', 'decimal') >= 0 --- returned value should be greater than 0
GO


/*
	Query that returns the total number of customers, purchase (grand total) and cars
*/
WITH XMLNAMESPACES 
('http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey' AS ns)
SELECT COUNT(1) AS NumberOfCustomers,SUM(Demographics.value('(/ns:IndividualSurvey/ns:TotalPurchaseYTD)[1]', 'decimal')) AS PurchaseTotal,
-- verify record existance (explicit usage of the singleton DP to ensure uniqueness - single instance of the node accessed)
SUM(Demographics.value('(/ns:IndividualSurvey/ns:NumberCarsOwned)[1]', 'int')) AS CarsTotal 
FROM AdventureWorks2017.Person.Person
WHERE Demographics.exist('(/ns:IndividualSurvey/ns:Education[contains(.,"Graduate Degree")])')=1 
AND Demographics.exist('(/ns:IndividualSurvey/ns:Gender[contains(.,"M")])')=1 
AND Demographics.value('(/ns:IndividualSurvey/ns:TotalPurchaseYTD)[1]', 'decimal') >= 0 



DECLARE @x NVARCHAR(MAX) =
  (SELECT TOP (10)
                [JobTitle], 
                [BirthDate], 
                [MaritalStatus]
FROM [AdventureWorks2017].[HumanResources].[Employee]
     FOR JSON AUTO)  
        select @x;

---- EXPORT FROM T-SQL TO JSON ---
/*
	Note:
	Variable declarations are done with DECLARE @variableName NVARCHAR(MAX) in this 
	case, to store the entire returned object (list of records translated to objects - json)
*/


--- SELECT 10 CUSTOMERS
SELECT TOP (10) Name, City, CustomerID, F.LineTotal AS Purchase FROM AdventureWorks2017_DataWarehouse.dbo.D_Customer DC
JOIN AdventureWorks2017_DataWarehouse.dbo.F_Sales F ON DC.C_ID = F.C_ID ORDER BY F.LineTotal;

--- CREATE FUNCTION FOR GETTING THE EMPLOYEES (argumentless) --- 
CREATE FUNCTION GetEmployeePurchases()
RETURNS NVARCHAR(MAX) 
AS
BEGIN
	RETURN (SELECT TOP (10) Name, City, CustomerID, F.LineTotal AS Purchase --- Return block (define the result)
	FROM AdventureWorks2017_DataWarehouse.dbo.D_Customer DC					---
	JOIN AdventureWorks2017_DataWarehouse.dbo.F_Sales F						---
	ON DC.C_ID = F.C_ID ORDER BY F.LineTotal								---	
	FOR JSON AUTO)
END

--- DECLARE VARIABLE AND ASSIGN IT (CALL THE ABOVE FUNCTION ON THE Database Object) ---
DECLARE @y NVARCHAR(MAX) = dbo.GetEmployeePurchases(); --- store results in variable (Anonymous column)
SELECT @y;

/* OUTPUT

[
  {
    "Name": "Eduardo A Adams",
    "City": "San Diego",
    "CustomerID": 25292,
    "F": [ { "Purchase": 2.2900 } ]
  },
  {
    "Name": "Edward  Adams",
    "City": "Berkeley",
    "CustomerID": 17163,
    "F": [ { "Purchase": 2.2900 } ]
  },
  {
    "Name": "Gabriella K Adams",
    "City": "Kirkland",
    "CustomerID": 24758,
    "F": [ { "Purchase": 2.2900 } ]
  },
  {
    "Name": "Isaiah L Adams",
    "City": "Newport Beach",
    "CustomerID": 28360,
    "F": [ { "Purchase": 2.2900 } ]
  },
  {
    "Name": "Jose J Adams",
    "City": "Colma",
    "CustomerID": 15477,
    "F": [ { "Purchase": 2.2900 } ]
  },
  {
    "Name": "Kaitlyn A Adams",
    "City": "Westminster",
    "CustomerID": 11869,
    "F": [
      { "Purchase": 2.2900 },
      { "Purchase": 2.2900 }
    ]
  },
  {
    "Name": "Miguel  Adams",
    "City": "Sooke",
    "CustomerID": 11659,
    "F": [
      { "Purchase": 2.2900 },
      { "Purchase": 2.2900 }
    ]
  },
  {
    "Name": "Xavier C Adams",
    "City": "Olympia",
    "CustomerID": 22133,
    "F": [ { "Purchase": 2.2900 } ]
  }
]


*/

--- CREATE FUNCTION FOR GETTING THE EMPLOYEES (argument) --- 
CREATE FUNCTION GetEmployeePurchases2(@name varchar(20))
RETURNS NVARCHAR(MAX) 
AS
BEGIN
	RETURN (SELECT TOP (10) Name, City, CustomerID, F.LineTotal AS Purchase --- Return block (define the result)
	FROM AdventureWorks2017_DataWarehouse.dbo.D_Customer DC					---
	JOIN AdventureWorks2017_DataWarehouse.dbo.F_Sales F						---
	ON DC.C_ID = F.C_ID WHERE Name = @name
	ORDER BY F.LineTotal								---	
	FOR JSON AUTO)
END


SELECT Name, City, CustomerID, F.LineTotal AS Purchase FROM AdventureWorks2017_DataWarehouse.dbo.D_Customer DC
JOIN AdventureWorks2017_DataWarehouse.dbo.F_Sales F ON DC.C_ID = F.C_ID ORDER BY F.LineTotal;

USE AdventureWorks2017_DataWarehouse

--- DECLARE VARIABLE AND ASSIGN IT (CALL THE ABOVE FUNCTION ON THE Database Object) ---
DECLARE @z NVARCHAR(MAX) = dbo.getEmployeePurchases2('Craig R Gill'); --- store results in variable (Anonymous column)
SELECT @z;

/*
	[
  {
    "Name": "Craig R Gill",
    "City": "Saint Ouen",
    "CustomerID": 29379,
    "F": [
      { "Purchase": 2.2900 },
      { "Purchase": 21.4900 }
    ]
  }
]
*/

SELECT TOP 3 * FROM D_Product WHERE ProductCategory != 'NONE';

DECLARE @v NVARCHAR(MAX) = (
	SELECT TOP 3 * FROM AdventureWorks2017_DataWarehouse.dbo.D_Product 
	WHERE ProductCategory != 'NONE'
	FOR JSON AUTO) ;
SELECT @v;

/*
	OUTPUT:
	[
  {
    "P_ID": 254,
    "ProductID": 749,
    "ProductName": "Road-150 Red, 62",
    "ProductNumber": "BK-R93R-62",
    "ProductCategory": "Components",
    "ProductSubCategory": "Road Bikes"
  },
  {
    "P_ID": 255,
    "ProductID": 750,
    "ProductName": "Road-150 Red, 44",
    "ProductNumber": "BK-R93R-44",
    "ProductCategory": "Components",
    "ProductSubCategory": "Road Bikes"
  },
  {
    "P_ID": 256,
    "ProductID": 751,
    "ProductName": "Road-150 Red, 48",
    "ProductNumber": "BK-R93R-48",
    "ProductCategory": "Components",
    "ProductSubCategory": "Road Bikes"
  }
]
*/

---- IMPORT OF JSON ---
/*
	OPENROWSET(BULK) is a table - valued function that can read data from any file on the local drive or network
*/

--- Simple import statement
SELECT BulkColumn FROM OPENROWSET(BULK 'C:\Users\fisch\Desktop\DAI_Exam\import_of_data\file_z.json', SINGLE_CLOB) as Import;

/*
	INPUT:
	[
  {
    "JobTitle": "Executive Officer",
    "BirthDate": "1969-01-29",
    "MaritalStatus": "M"
  },
  {
    "JobTitle": "King of Scotland",
    "BirthDate": "1971-08-01",
    "MaritalStatus": "S"
  },
  {
    "JobTitle": "Drill Master",
    "BirthDate": "1974-11-12",
    "MaritalStatus": "D"
  }
]    
*/

--- Load data into a temporary table (temporary table deleted by the system when the connection is over)
SELECT BulkColumn INTO #temp2
FROM OPENROWSET(BULK 'C:\Users\fisch\Desktop\DAI_Exam\import_of_data\file_z.json', SINGLE_CLOB) as Import;

--- see results
SELECT * FROM #temp2;

/*
	Parse Json documents into rows and columns

	Note:
	- As seen before, the function OPENROWSET, reads a single text value from the file,
	returns it as a BulkColumn and passes it to the OPENJSON function.
	- OPENJSON iterates through the array of JSON objects in the BulkColumn array and 
	returns one person in each row, formatted as JSON.
*/

--	if an attribute is not defined, it will not be mapped
SELECT person.*
FROM OPENROWSET(BULK 'C:\Users\fisch\Desktop\file_z.json', SINGLE_CLOB) as Import
CROSS APPLY OPENJSON(BulkColumn)
WITH(
	Name char(15),
	Money_In_Bank float,
	JobTitle nvarchar(100), --- map imported field to table attribute	(analyze data type)
	--BirthDate date,			---	omitted attributes will result in no mapping of the Json object attributes
	MaritalStatus char)		---	map imported field to table attribute	(analyze data type)
AS person;

USE TestingDatabase

--- create a table to hold the data
CREATE TABLE TestImportTable(
	Name char(15),
	Money_In_Bank float,
	JobTitle nvarchar(100),
	BirthDate date,
	MaritalStatus char
)

--- insert Json file data into table
INSERT INTO TestImportTable(
	Name,
	Money_In_Bank,
	JobTitle,
	BirthDate,
	MaritalStatus)
	SELECT person.*
FROM OPENROWSET(BULK 'C:\Users\fisch\Desktop\DAI_Exam\import_of_data\file_z.json', SINGLE_CLOB) as Import
CROSS APPLY OPENJSON(BulkColumn)
WITH(
	Name char(15),
	Money_In_Bank float,
	JobTitle nvarchar(100), 
	BirthDate date,			
	MaritalStatus char)		
AS person;

--- Verify the results
SELECT * FROM TestingDatabase.dbo.TestImportTable

Update TestingDatabase.dbo.TestImportTable 
SET Money_In_Bank = 0.0 WHERE Money_In_Bank IS NULL;


USE TestingDatabase


/*
	Test job scheduler simple function
*/
USE TestingDatabase

DROP TABLE IF EXISTS LastUpdateTest

Create table LastUpdateTest(
id int IDENTITY(1,1),
text nvarchar(30),
LastUpdate datetime
);

SELECT * FROM LastUpdateTest ORDER BY LastUpdate

select suser_sname(owner_sid) from sys.databases


SELECT * FROM TestingDatabase.dbo.LastUpdateTest Order By LastUpdate DESC

