/*
	AdventureWorks2017_StagingArea_SCD
*/

/*****************************************************************************************************
	STEP 1.0 (ETL - SCD)
	- Added 2 products to the source system (AdventureWorks2017 database, schema Production.Product)
*****************************************************************************************************/
INSERT INTO AdventureWorks2017.Production.Product(
Name,
ProductNumber,
SafetyStockLevel,
ReorderPoint,
StandardCost,
ListPrice,
DaysToManufacture,
SellStartDate,
ProductLine,
Style
)
VALUES
(
'MyUniqueProduct_1', 
'AB-9331',
1000,
400,
300,
200,
100,
'2008-01-20',
'R',
'U');

---	Find added product in source system 'MyUniqueProduct_1'
SELECT * FROM  AdventureWorks2017.Production.Product WHERE Name = 'MyUniqueProduct_1';


INSERT INTO AdventureWorks2017.Production.Product(
Name,
ProductNumber,
SafetyStockLevel,
ReorderPoint,
StandardCost,
ListPrice,
DaysToManufacture,
SellStartDate,
ProductLine,
Style
)
VALUES
(
'MyUniqueProduct_2', 
'AB-9330',
2000,
400,
300,
200,
100,
'2010-09-15',
'R',
'U');

---	Find added product 'MyUniqueProduct_2'
SELECT * FROM  AdventureWorks2017.Production.Product WHERE Name = 'MyUniqueProduct_2';

--- Rerun of the process
DROP TABLE AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Customer
DROP TABLE AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product
DROP TABLE AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales
DROP TABLE AdventureWorks2017_DataWarehouse_SCD.dbo.D_Customer
DROP TABLE AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product
DROP TABLE AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales
DROP TABLE AdventureWorks2017_DataWarehouse_SCD.dbo.LastUpdate
DROP TABLE AdventureWorks2017_DataWarehouse_SCD.dbo.D_Date
--- End of rerun





/*****************************************************************************************************
	STEP 1.1 (ETL - SCD)
	- Create dimension tables (D_Product and D_Customer) and fact table (F_Sales)

	NOTE: D_Product and D_Customer will have 2 added fields, ValidTo and ValidFrom
		  these fields are used to keep track of product, customer dimension record
		  validity for type 2 slowly changing dimensions. Type 2 SCD refers to the 
		  approach of preserving dimension record (row) history by adding new rows
		  upon modifications of existing rows where the validity might have been 
		  changed.

		  EX.: A Heinz Ketchup product might be categorised as a condiment up to '2020-02-23'
		  and from '2020-02-24', it will be regarded as sauce, therefore, in order to preserve
		  the history of the product, both entries will be captured within the product dimension
		  the difference however is that the old entry of the kecthup (condiment up to '2020-02-23') 
		  will not be the current valid one (it will be regarderd as of "yesterday's entry" whilst
		  the new entry from '2020-02-24' will be regarded as sauce and be the current ("today" - valid)
		  entry.
*****************************************************************************************************/

--- Create DWH Dimension tables - added fields ValidFrom and ValidTo (used to track data history)
-- Initial creation of D_Customer (the customer dimension of the DW)
USE AdventureWorks2017_DataWarehouse_SCD

CREATE TABLE [D_Customer](
C_ID [int] IDENTITY(1,1) NOT NULL,	--	Surrogate key, IDENTITY(1,1) used for generation (1..N) where N is the total number of records
CustomerID [int] NOT NULL,			--	Business/Natural key
Name [nvarchar](100) NOT NULL,		--	Customer name 
City [nvarchar](30) NOT NULL,		--	City (will be used for grouping as customers reside in multiple cities around the world)
ValidFrom [date] NOT NULL,	--***** Added field for tracking the history of the Products 
ValidTo[date] NOT NULL,		--***** Added field for tracking the history of the Products 
PRIMARY KEY(C_ID)                   --  Surrogate key used as Primary key
);


--	Initial creation of D_Product (the product dimension of the DW)
CREATE TABLE [D_Product](
P_ID [int] IDENTITY(1,1) NOT NULL,			--	Surrogate key, IDENTITY(1,1) used for generation (1..N) where N is the total number of records
ProductID [int] NOT NULL,					--	Business/Natural key
ProductName [nvarchar](50) NOT NULL,		--	Product name
ProductNumber [nvarchar] (25) NOT NULL,		--	Product number
ProductCategory[nvarchar] (50) NOT NULL,	--	Product category
ProductSubCategory[nvarchar] (50) NOT NULL,	--	Product SubCategory
ValidFrom [date] NOT NULL,	--***** Added field for tracking the history of the Products 
ValidTo[date] NOT NULL,		--***** Added field for tracking the history of the Products 
PRIMARY KEY(P_ID)							--	Surrogate key used as Primary key
);

--	Initial creation of D_Date (the date dimension of the DW)
CREATE TABLE [D_Date](
D_ID [int] IDENTITY(1,1) NOT NULL,		--	Surrogate key, IDENTITY(1,1) used for generation (1..N) where N is the total number of records
CalendarDate [date] NOT NULL,			--	Date of calendar
WeekDayName [nvarchar](20) NOT NULL,	--	Day of the week
MonthName [nvarchar](20) NOT NULL,		--	Name of the month
WeekDayNumber [smallint] NOT NULL,		--	Week day number
PRIMARY KEY(D_ID)						--	Surrogate key used as Primary key
);

--	Initial creation of F_Sales (the sales fact table of the DW)
CREATE TABLE [F_Sales](
C_ID [int] NOT NULL, 
P_ID [int] NOT NULL,
D_ID [int] NOT NULL,
SalesOrderID [int] NOT NULL,	---	SalesOrderID added as part of the primary key due to duplicate data being observed in the staging area's fact table while extraction was occuring (AdventureWorks2017_StagingArea.F_Sales)
LineTotal [money] NOT NULL,
OrderQty [smallint] NOT NULL,

CONSTRAINT FK_F_Sales_0 FOREIGN KEY (C_ID)
REFERENCES D_Customer(C_ID),
CONSTRAINT FK_F_Sales_1 FOREIGN KEY (P_ID)
REFERENCES D_Product(P_ID),
CONSTRAINT FK_F_Sales_2 FOREIGN KEY (D_ID)
REFERENCES D_Date(D_ID),

CONSTRAINT PK_F_Sales PRIMARY KEY (C_ID, P_ID, D_ID, SalesOrderID)
);

/*****************************************************************************************************
	STEP 1.2 (ETL - SCD)
	- Create "LastUpdate" table to keep track of the last update performed on the DW
*****************************************************************************************************/
CREATE TABLE LastUpdate(
LastUpdate [date],
)

/*****************************************************************************************************
	STEP 1.3 (ETL - SCD)
	- Create staging area tables: Stage_D_Customer and Stage_D_Product
*****************************************************************************************************/

USE AdventureWorks2017_StagingArea_SCD

---	Creation of the customer dimension staging area table (Stage_D_Customer)
CREATE TABLE [Stage_D_Customer](
CustomerID [int],			--	Business/Natural key
Name [nvarchar](100),		--	Customer name 
City [nvarchar](30),		--	City (will be used for grouping as customers reside in multiple cities around the world)
);


---	Creation of the customer dimension staging area table (Stage_D_Product)
CREATE TABLE [Stage_D_Product](
ProductID [int],					--	Business/Natural key
ProductName [nvarchar](50),		--	Product name
ProductNumber [nvarchar] (25),		--	Product number
ProductCategory[nvarchar] (50),	--	Product category
ProductSubCategory[nvarchar] (50),	--	Product SubCategory
);

/*****************************************************************************************************
	STEP 1.4 (ETL - SCD)
	- Populate staging area tables: Stage_D_Customer and Stage_D_Product
*****************************************************************************************************/

--- Populating Stage_D_Customer
--- 18484 records affected

INSERT INTO Stage_D_Customer(
CustomerID,
Name,
City)

SELECT 
CustomerID, 
CONCAT(P.FirstName,' ',P.MiddleName,' ',P.LastName),
City

FROM

AdventureWorks2017.Sales.Customer C 
JOIN AdventureWorks2017.Person.Person P
ON 
C.PersonID = P.BusinessEntityID	

JOIN AdventureWorks2017.Person.BusinessEntity B
ON
P.BusinessEntityID = B.BusinessEntityID 

JOIN AdventureWorks2017.Person.BusinessEntityAddress BA
ON B.BusinessEntityID = BA.BusinessEntityID 

JOIN AdventureWorks2017.Person.Address A 
ON BA.AddressID = A.AddressID 

WHERE BA.AddressTypeID = 2 AND StoreID IS NULL;

/*****************************************************************************************************
	STEP 1.4.1 (ETL - SCD)
	- Verify the staging area table - Stage_D_Customer data
	NOTE: 
	In this section, we are verifying the table attributes for NULLs, inconsistencies and mismatches
*****************************************************************************************************/

---	Verify the data
SELECT * FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Customer;

---	0 results
SELECT COUNT(*) AS CustomerID_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Customer WHERE CustomerID IS NULL;


---	0 results
SELECT COUNT(*) AS Customer_Name_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Customer WHERE Name IS NULL;

---	0	results
SELECT COUNT(*) AS Customer_City_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Customer WHERE City IS NULL;	

--- Declare the  Stage_D_Customer data valid

--- Continuation of STEP 1.4.0 - Populating Stage_D_Product

INSERT INTO Stage_D_Product(
ProductID,
ProductName,
ProductNumber)

SELECT

ProductID,
Name,
ProductNumber

FROM 

AdventureWorks2017.Production.Product;
--- 508 rows affected
--- NOTE: Row count as expected, as initially there were 506 records but since 2 records were added in STEP 1.0 the result is now 508


/*****************************************************************************************************
	STEP 1.4.2 (ETL - SCD)
	- Verify the staging area table - Stage_D_Product data
	NOTE: 
	In this section, we are verifying the table attributes for NULLs, inconsistencies and mismatches
*****************************************************************************************************/

---	Verify the data
SELECT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product;  --- the last 2 entries are the newly added records (entries 507, 508 respectively and names: 'MyUniqueProduct_1' and 'MyUniqueProduct_2')
/*
	Currently, all records hold 'NULL' as the value for the ProductCategory and ProductSubCategory
*/

---	All records are unique (508)
SELECT DISTINCT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product;

---	0 results, no product has the ProductID as NULL
SELECT COUNT(*) AS ProductID_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductID IS NULL;

---	0 results, no product name is NULL
SELECT COUNT(*) AS Product_Name_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductName IS NULL;

---	0	results, no product number is NULL
SELECT COUNT(*) AS Product_Number_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductNumber IS NULL;	


/*****************************************************************************************************
	STEP 1.5 (ETL - SCD)
	- Perform update of Stage_D_Product
	NOTE: In this section we set the product category and subcategory for products with such entries
*****************************************************************************************************/
USE AdventureWorks2017_StagingArea_SCD
---	Update NULL attributes (ProductCategory and ProductSubCategory) ---

---	a)	Update product subcategory --- 295 records affected (rows)
UPDATE Stage_D_Product
SET ProductSubCategory = PS.Name
FROM Stage_D_Product AS P 
JOIN AdventureWorks2017.Production.Product AS PR
ON P.ProductID = PR.ProductID
JOIN AdventureWorks2017.Production.ProductSubcategory AS PS
ON PR.ProductSubcategoryID = PS.ProductSubcategoryID;

---	b)	Update product category --- 105 records affected (rows)
UPDATE Stage_D_Product
SET ProductCategory = PC.Name

FROM Stage_D_Product AS P
JOIN AdventureWorks2017.Production.Product AS PR

ON P.ProductID = PR.ProductID

JOIN AdventureWorks2017.Production.ProductSubcategory AS PS
ON PR.ProductSubcategoryID = PS.ProductCategoryID

JOIN AdventureWorks2017.Production.ProductCategory AS PC
ON PS.ProductCategoryID = PC.ProductCategoryID;



/*****************************************************************************************************
	STEP 1.6 (ETL - SCD)
	- Perform update of Stage_D_Product
	NOTE: In this section we set the product category and subcategory for products with no such entries
		  to a default value, namely 'NONE'
*****************************************************************************************************/

---	Verify the changed data
---	213 records identified (Products with ProductSubCategory as NULL)
SELECT COUNT(*) AS Product_Subcategory_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductSubCategory IS NULL;

---	403 records identified (Products with ProductCategory as NULL)
SELECT COUNT(*) AS Product_Category_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductCategory IS NULL;


---	Handle the NULL values in Stage_D_Product (Replace the NULL values with a default placeholder value)
UPDATE Stage_D_Product
SET
ProductSubCategory = 'NONE' WHERE ProductSubCategory IS NULL;	---	213	records affected (as previously identified)

UPDATE Stage_D_Product
SET
ProductCategory = 'NONE' WHERE ProductCategory IS NULL;	---	403 records affected (as previously identified)

/*****************************************************************************************************
	STEP 1.7 (ETL - SCD)
	- Perform last verification of Stage_D_Product entries
	NOTE: In this section we verify all attributes for NULL values, atomicity and inconsistencies
*****************************************************************************************************/
/*
	Perform last verification of the Stage_D_Product table before declaring the data valid (ready for initial load into the dimension)
	Target dimension: AdventureWorks2017_DataWarehouse.D_Product
*/
---	All records are unique (508)
SELECT DISTINCT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product;

---	0 results, no product has the ProductID as NULL
SELECT COUNT(*) AS ProductID_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductID IS NULL;

---	0 results, no product name is NULL
SELECT COUNT(*) AS Product_Name_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductName IS NULL;

---	0	results, no product number is NULL
SELECT COUNT(*) AS Product_Number_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductNumber IS NULL;

---	0 results, no product category is NULL
SELECT COUNT(*) AS Product_Category_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductCategory IS NULL;

---	0	result, no product subcategory is NULL
SELECT COUNT(*) AS Product_Subcategory_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductSubCategory IS NULL;

---	The Stage_D_Product table data is declared valid, ready to perform initial load into the target dimension: AdventureWorks2017_DataWarehouse_SCD.D_Product

/*****************************************************************************************************
	STEP 1.8 (ETL - SCD)
	- Populate DW dimensions - D_Customer and D_Product
	NOTE: In this section, dimension data is loaded into the DW dimension tables from the staging area
		  currently loaded records hold a validity from '2011-05-31' to '2099-12-31'
	a) Populate the Customer dimension : AdventureWorks2017_DataWarehouse_SCD.D_Customer
	b) Populate the Product dimension : AdventureWorks2017_DataWarehouse.D_Product
*****************************************************************************************************/

---	a) Populate the Customer dimension : AdventureWorks2017_DataWarehouse_SCD.D_Customer
---	18 484 rows affected (inserted records)
--- Add ValidFrom = '2011-05-31' ( aka today, insertion date)
---	Add ValidTo = '2099-12-31' (default value to represent record validity, longetivity)
INSERT INTO AdventureWorks2017_DataWarehouse_SCD.dbo.D_Customer(
CustomerID,
Name,
City,
ValidFrom,
ValidTo
)
SELECT
CustomerID,
Name,
City,
'2011-05-31',	--- newly added fields (current record validity)
'2099-12-31'	---	newly added fields ( current validity upper bound/valid until)
FROM 
AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Customer

SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Customer

--- b) Populate the Product dimension : AdventureWorks2017_DataWarehouse.D_Product
---	508 records affected (inserted records)
--- Add ValidFrom = '2011-05-31' ( aka today, insertion date)
---	Add ValidTo = '2099-12-31' (default value to represent record validity, longetivity)
INSERT INTO AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product(
ProductID,
ProductName,
ProductNumber,
ProductCategory,
ProductSubCategory,
ValidFrom,
ValidTo)

SELECT
ProductID,
ProductName,
ProductNumber,
ProductCategory,
ProductSubCategory,
'2011-05-31',	--- newly added fields (current record validity)
'2099-12-31'	---	newly added fields ( current validity upper bound/valid until)

FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product


/*****************************************************************************************************
	STEP 1.9 (ETL - SCD)
	- Populate the D_Date (DW) date dimension table
*****************************************************************************************************/
USE AdventureWorks2017_DataWarehouse_SCD

DECLARE @StartDate DATETIME
DECLARE @EndDate DATETIME

SET @StartDate = '2010-01-01'
SET @EndDate = DATEADD(d, 4095, @StartDate)
WHILE @StartDate <= @EndDate
BEGIN

INSERT INTO AdventureWorks2017_DataWarehouse_SCD.dbo.D_Date	
(
CalendarDate,
WeekDayName,
MonthName,
WeekDayNumber
)

SELECT
@StartDate,
DATENAME(weekday, @StartDate),
DATENAME(month , @StartDate),
DATEPART(weekday, @StartDate)
SET @StartDate = DATEADD(dd, 1, @StartDate)
END

SELECT * FROM D_Date


/*****************************************************************************************************
	STEP 1.10 (ETL - SCD)
	- Create the Staging area fact table
*****************************************************************************************************/
USE AdventureWorks2017_StagingArea_SCD

CREATE TABLE Stage_F_Sales(
C_ID [int] NULL,
P_ID [int] NULL,
D_ID [int] NULL,
CustomerID [int] NULL,
SalesOrderID [int] NULL, --- added as duplicates were identified
ProductID [int] NULL,
LineTotal [money] NULL,
OrderQty [smallint] NULL,
OrderDate [date] NULL)


/*****************************************************************************************************
	STEP 1.11 (ETL - SCD)
	- Populating the Staging area fact table

	NOTE:
--- Populate the Stage_F_Sales Staging Area fact table (Except surrogate keys)
---	Add a WHERE clause : AND OrderDate <= '2013-12-31'
---	(60 398 records affected without the additional WHERE clause), currently 32 903 records affected
*****************************************************************************************************/

INSERT INTO Stage_F_Sales(
CustomerID,
ProductID,
SalesOrderID,  --- added as duplicates were identified
LineTotal,
OrderQty,
OrderDate)

SELECT 
C.CustomerID,
P.ProductID,
S.SalesOrderID,  --- added as duplicates were identified
(UnitPrice * OrderQty) AS LineTotal,
OrderQty,
OrderDate

FROM
AdventureWorks2017.Sales.Customer C
JOIN AdventureWorks2017.Sales.SalesOrderHeader S ON C.CustomerID = S.CustomerID
JOIN AdventureWorks2017.Sales.SalesOrderDetail D ON S.SalesOrderID = D.SalesOrderID
JOIN AdventureWorks2017.Production.Product P ON P.ProductID = D.ProductID
JOIN AdventureWorks2017.Person.Person PER ON C.PersonID = PER.BusinessEntityID
JOIN AdventureWorks2017.Person.BusinessEntity B ON PER.BusinessEntityID = B.BusinessEntityID
JOIN AdventureWorks2017.Person.BusinessEntityAddress BA ON B.BusinessEntityID = BA.BusinessEntityID
WHERE C.StoreID IS NULL AND BA.AddressTypeID = 2 AND OnlineOrderFlag = 1 AND OrderDate <= '2013-12-31'; --- TODAY

SELECT * FROM Stage_F_Sales

/*****************************************************************************************************
	STEP 1.12 (ETL - SCD)
	- Perform key lookup

	NOTE:
	Match business keys from the Product Dimension with the one from the Stage_F_Sales table to assign the 
	corresponding surrogate key to the sales records in the Stage_F_Sales table
*****************************************************************************************************/
--- a) Update the P_ID surrogate key (32 903 recoreds affected)
UPDATE Stage_F_Sales
SET P_ID = (SELECT P_ID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product P WHERE P.ProductID = Stage_F_Sales.ProductID)

---	Verify change, 0 records within the Stage_F_Sales have the P_ID as NULL => Valid lookup
SELECT COUNT(*) AS P_ID_Surrogate_Null FROM Stage_F_Sales WHERE P_ID IS NULL;

---	b) Update C_ID (Customer dimension surrogate key - 32 903 records affected)
/*
	Match business keys from the Customer Dimension with the one from the Stage_F_Sales table to assign the 
	corresponding surrogate key to the sales records in the Stage_F_Sales table
*/
---	32 903 records affected
UPDATE Stage_F_Sales SET C_ID = C.C_ID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Customer AS C JOIN Stage_F_Sales AS S
ON C.CustomerID = S.CustomerID WHERE C.CustomerID = S.CustomerID

---	Verify change, 0 records within the Stage_F_Sales have the C_ID as NULL => Valid lookup
SELECT COUNT(*) AS C_ID_Surrogate_Null FROM Stage_F_Sales WHERE C_ID IS NULL;

---	c) Update D_ID (Date dimension surrogate key)
/*
	Match business keys from the Date Dimension with the one from the Stage_F_Sales table to assign the 
	corresponding surrogate key to the sales records in the Stage_F_Sales table
*/
---	32 903 records affected
UPDATE Stage_F_Sales
SET D_ID = (SELECT D_ID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Date D WHERE D.CalendarDate = Stage_F_Sales.OrderDate)

---	Verify change, 0 records within the Stage_F_Sales have the D_ID as NULL => Valid lookup
SELECT COUNT(*) AS D_ID_Surrogate_Null FROM Stage_F_Sales WHERE D_ID IS NULL;


/*****************************************************************************************************
	STEP 1.13 (ETL - SCD)
	- Row atomicity/uniqueness verification

	NOTE:
	Upon verification, from the total count of rows (32,903), several rows appear as duplicates 
	(32,903 [total row count] - 32,898[unique/distinct] = 5 [duplicates])
*****************************************************************************************************/

SELECT DISTINCT C_ID,D_ID,P_ID,CustomerID,ProductID,LineTotal,OrderDate,OrderQty FROM Stage_F_Sales --- 5 duplicates identified


/*****************************************************************************************************
	STEP 1.14 (ETL - SCD)
	- Initial load of F_Sales (DW)

*****************************************************************************************************/
---	Initial load of F_Sales in the DW (32 903 records affected)
INSERT INTO AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales(
P_ID,
C_ID,
D_ID,
SalesOrderID,
OrderQty,
LineTotal)
SELECT
P_ID,
C_ID,
D_ID,
SalesOrderID,
OrderQty,
LineTotal
FROM Stage_F_Sales;

/*****************************************************************************************************
	STEP 1.15 (ETL - SCD)
	- Validity check (row atomicity)

	NOTE: The result is that all rows are atomic

*****************************************************************************************************/

SELECT COUNT(*) Occurences,C_ID,D_ID,P_ID,LineTotal,OrderQty, SalesOrderID 
FROM AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales GROUP BY C_ID,D_ID,P_ID,LineTotal,OrderQty, SalesOrderID HAVING COUNT(*) >1;
---	All current AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales are atomic (32, 903 - no duplicates)


/*****************************************************************************************************
	STEP 2.1 (ETL - SCD)
	- Add a product to the source system
*****************************************************************************************************/
USE AdventureWorks2017

SELECT * FROM AdventureWorks2017.Production.Product -- currently 508 products

INSERT INTO AdventureWorks2017.Production.Product(
Name,
ProductNumber,
SafetyStockLevel,
ReorderPoint,
StandardCost,
ListPrice,
DaysToManufacture,
SellStartDate,
ProductLine,
Style
)
VALUES
(
'MyUniqueProduct_3', 
'AB-9333',
6000,
500,
400,
300,
200,
'2012-12-12',
'R',
'U');

---	Find added product in the source system
SELECT * FROM  AdventureWorks2017.Production.Product WHERE Name = 'MyUniqueProduct_3';

SELECT * FROM AdventureWorks2017.Production.Product -- 509 products as 'MyUniqueProduct_3' was recently added


--- Find added product (present in source but not in DW)
SELECT ProductID,Name,
ProductNumber,
SafetyStockLevel,
ReorderPoint,
StandardCost,
ListPrice,
DaysToManufacture,
SellStartDate,
ProductLine,
Style
FROM AdventureWorks2017.Production.Product

WHERE ProductID IN ((
SELECT ProductID

FROM AdventureWorks2017.Production.Product 

)
EXCEPT
(
SELECT ProductID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product 
)
)
--- Result: As expected, 'MyUniqueProduct_3' is present in the source system (AdventureWorks2017) but not in the D_Product dimension (DW - AdventureWorks2017_DataWarehouse_SCD)




/*****************************************************************************************************
	STEP 2.2 (ETL - SCD)
	- Extract added product of the source system to the staging area corresponding table (Stage_D_Product)
*****************************************************************************************************/
USE AdventureWorks2017_StagingArea_SCD

INSERT INTO AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product(
ProductID,
ProductName,
ProductNumber)

SELECT 
ProductID,
Name,
ProductNumber


FROM AdventureWorks2017.Production.Product 

WHERE ProductID IN ((
SELECT ProductID

FROM AdventureWorks2017.Production.Product 

)
EXCEPT
(
SELECT ProductID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product 
)
)

--- Identify added product in the Stage_D_Product table (entry 509)
SELECT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductName = 'MyUniqueProduct_3'
SELECT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product




/*****************************************************************************************************
	STEP 2.3 (ETL - SCD)
	- Verify the staging area table - Stage_D_Product data
	NOTE: 
	In this section, we are verifying the newly added entry for NULLs, inconsistencies and mismatches
*****************************************************************************************************/

---	Verify the data
SELECT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product;  --- the last entry is the newly added record (ProductIDs 3005, entry 509 name: 'MyUniqueProduct_3')
/*
	Currently, the last record hold 'NULL' as the value for the ProductCategory and ProductSubCategory
*/

---	All records are unique (509)
SELECT DISTINCT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product;

---	0 results, no product has the ProductID as NULL
SELECT COUNT(*) AS ProductID_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductID IS NULL;

---	0 results, no product name is NULL
SELECT COUNT(*) AS Product_Name_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductName IS NULL;

---	0	results, no product number is NULL
SELECT COUNT(*) AS Product_Number_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductNumber IS NULL;	


/*****************************************************************************************************
	STEP 2.4 (ETL - SCD)
	- Perform update of Stage_D_Product
	NOTE: In this section we set the product category and subcategory for the newly added product
*****************************************************************************************************/

---	Verify the changed data
---	1 record identified (Products with ProductSubCategory as NULL)
SELECT COUNT(*) AS Product_Subcategory_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductSubCategory IS NULL;

---	1 record identified (Products with ProductCategory as NULL)
SELECT COUNT(*) AS Product_Category_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product WHERE ProductCategory IS NULL;


---	Handle the NULL values in Stage_D_Product (Replace the NULL values with a default placeholder value)
UPDATE Stage_D_Product
SET
ProductSubCategory = 'NONE' WHERE ProductSubCategory IS NULL;	---	1	records affected (as previously identified)

UPDATE Stage_D_Product
SET
ProductCategory = 'NONE' WHERE ProductCategory IS NULL;	---	1 records affected (as previously identified)

/*****************************************************************************************************
	STEP 2.5 (ETL - SCD)
	- Perform last verification of Stage_D_Product entries
	NOTE: In this section we verify all attributes for NULL values, atomicity and inconsistencies
*****************************************************************************************************/
/*
	Perform last verification of the Stage_D_Product table before declaring the data valid (ready for initial load into the dimension)
	Target dimension: AdventureWorks2017_DataWarehouse.D_Product
*/
---	All records are unique (509)
SELECT DISTINCT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product;

---	0 results, no product has the ProductID as NULL
SELECT COUNT(*) AS ProductID_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductID IS NULL;

---	0 results, no product name is NULL
SELECT COUNT(*) AS Product_Name_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductName IS NULL;

---	0	results, no product number is NULL
SELECT COUNT(*) AS Product_Number_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductNumber IS NULL;

---	0 results, no product category is NULL
SELECT COUNT(*) AS Product_Category_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductCategory IS NULL;

---	0	result, no product subcategory is NULL
SELECT COUNT(*) AS Product_Subcategory_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductSubCategory IS NULL;

/*****************************************************************************************************
	STEP 2.6 (ETL - SCD)
	- Load (Incrementally) the added product from the staging area in the D_Product dimension
*****************************************************************************************************/
USE AdventureWorks2017_DataWarehouse_SCD

INSERT INTO AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product(
ProductID,
ProductName,
ProductNumber,
ProductSubCategory,
ProductCategory,
ValidFrom,
ValidTo)

SELECT 
ProductID,
ProductName,
ProductNumber,
ProductSubcategory,
ProductCategory,
'2014-01-01',
'2099-12-31'

FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product

WHERE ProductID IN ((
SELECT ProductID

FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product

)
EXCEPT
(
SELECT ProductID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product
)
)

--- Identify added product in the Stage_D_Product table (entry 509)
SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product WHERE ProductName = 'MyUniqueProduct_3'
SELECT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product

/*****************************************************************************************************
	STEP 2.7 (ETL - SCD)
	- Delete product 'MyUniqueProduct_2'
*****************************************************************************************************/

SELECT * FROM AdventureWorks2017.Production.Product
--- 'MyUniqueProduct_2' selected for deletion

DELETE FROM AdventureWorks2017.Production.Product WHERE Name = 'MyUniqueProduct_2'


--- DETECT DELETED ROW

SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product

WHERE ProductID IN ((
SELECT ProductID

FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product

)
EXCEPT
(
SELECT ProductID FROM AdventureWorks2017.Production.Product
)
)

--- Update deleted row (from source system)
--- Set ValidTo = '2013-12-31' (Last update)
/*
	NOTE:
	We still keep outdated products within the dimension,
	however, here, set difference is used to detect existing
	dimension products that have a validity higher than the 
	last update - '2013-12-31' but do not exist in the 
	source system anymore
*/

/*****************************************************************************************************
	STEP 2.8 (ETL - SCD)
	- Update deleted product 'MyUniqueProduct_2'
	- Set ValidTo = '2013-12-31' (the product was valid until) as when compared with the source
	system, it does not exist anymore, but there is still need of the history for this specific 
	product.
*****************************************************************************************************/
UPDATE AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product SET ValidTo = '2013-12-31' --- last Update
WHERE ProductID IN
((
SELECT ProductID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product WHERE ValidTo >'2013-12-31' --- "yesterday" (products that were still valid)
)
EXCEPT
(
SELECT ProductID FROM AdventureWorks2017.Production.Product --- "today" (products that currently have been removed, as of today, are no longer valid)
)
)

SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product



/*****************************************************************************************************
	STEP 2.9 (ETL - SCD)
	- Update product 'MyUniqueProduct_1'
	- Set the product name to 'Laravel'
*****************************************************************************************************/

--- UPDATE ROW AND DETECT CHANGE

Update AdventureWorks2017.Production.Product SET Name = 'Laravel' WHERE Name = 'MyUniqueProduct_1';

--- Identify/Detect updated product (Use set difference for each attribute to detect changes)
GO
(
SELECT ProductID ,Name COLLATE DATABASE_DEFAULT AS ProductName,ProductNumber COLLATE DATABASE_DEFAULT AS ProductNumber

FROM AdventureWorks2017.Production.Product) --- today

EXCEPT 
(
SELECT ProductID, ProductName, ProductNumber  
FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product) --- yesterday
EXCEPT
(
SELECT ProductID , Name COLLATE DATABASE_DEFAULT , ProductNumber  COLLATE DATABASE_DEFAULT
FROM AdventureWorks2017.Production.Product
WHERE ProductID NOT IN(SELECT ProductID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product))


/*****************************************************************************************************
	STEP 2.10 (ETL - SCD)
	- Create table Stage_D_Product_Updates
	NOTE:
	a) The table has the exact same attributes as the Stage_D_Product table
	b) This table is designated for products that have been changed in the source
	system and need to be updated in the DW product dimension
	Comment:
	- It is essential to note that as the interest lies within keeping the history of products,
	as per the SCD - type 2 approach, we keep within the dimension both the outdated and
	updated product records (we keep the old product but update its validity - ValidTo and we 
	add a new product entry - the newly updated one in the source system)
	- The process occurs in 2 steps:
	Step 2.11 - Update of the existing row in the product dimension
	Step 2.12 - Insertion of the new row (updated product record in the source system) 
*****************************************************************************************************/
---	Creation of the customer dimension staging area temporary table (Stage_D_Product_Updates)
---	Table for handling updated products
USE AdventureWorks2017_StagingArea_SCD

CREATE TABLE Stage_D_Product_Updates(
ProductID [int],					--	Business/Natural key
ProductName [nvarchar](50),		--	Product name
ProductNumber [nvarchar] (25),		--	Product number
ProductCategory[nvarchar] (50),	--	Product category
ProductSubCategory[nvarchar] (50),	--	Product SubCategory
);


INSERT INTO Stage_D_Product_Updates(
ProductID,
ProductName ,
ProductNumber --- Temporary staging area table for changed products
)
(
SELECT ---	today (current changed product in source system)
ProductID ,
Name COLLATE DATABASE_DEFAULT,
ProductNumber COLLATE DATABASE_DEFAULT

FROM
AdventureWorks2017.Production.Product --- source
)
EXCEPT
(
---	yesterday (up to '2013-12-31' valid products)
SELECT 
ProductID,
ProductName,
ProductNumber
FROM
AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product
WHERE ValidTo > '2013-12-31' 
)
EXCEPT
(
SELECT 
ProductID,
Name,
ProductNumber
FROM 
AdventureWorks2017.Production.Product
WHERE ProductID NOT IN(
SELECT ProductID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product))

SELECT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates

---	Handle the NULL values in Stage_D_Product (Replace the NULL values with a default placeholder value)
UPDATE Stage_D_Product_Updates
SET
ProductSubCategory = 'NONE' WHERE ProductSubCategory IS NULL;	---	1	records affected (as previously identified)

UPDATE Stage_D_Product_Updates
SET
ProductCategory = 'NONE' WHERE ProductCategory IS NULL;	---	1 records affected (as previously identified)

/*
	Perform last verification of the Stage_D_Product table before declaring the data valid (ready for initial load into the dimension)
	Target dimension: AdventureWorks2017_DataWarehouse.D_Product
*/
---	All records are unique (509)
SELECT DISTINCT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates;

---	0 results, no product has the ProductID as NULL
SELECT COUNT(*) AS ProductID_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates WHERE ProductID IS NULL;

---	0 results, no product name is NULL
SELECT COUNT(*) AS Product_Name_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates WHERE ProductName IS NULL;

---	0	results, no product number is NULL
SELECT COUNT(*) AS Product_Number_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates WHERE ProductNumber IS NULL;

---	0 results, no product category is NULL
SELECT COUNT(*) AS Product_Category_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates WHERE ProductCategory IS NULL;

---	0	result, no product subcategory is NULL
SELECT COUNT(*) AS Product_Subcategory_Null_Count FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates WHERE ProductSubCategory IS NULL;


/*****************************************************************************************************
	STEP 2.11 (ETL - SCD)
	- Update of the existing row in the product dimension
*****************************************************************************************************/
UPDATE AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product
SET ValidTo = '2013-12-31'
WHERE ProductID IN
(
SELECT ProductID
FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates)
AND ValidTo >= '2013-12-31'
SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product


/*****************************************************************************************************
	STEP 2.12 (ETL - SCD)
	- Insertion of the new row (updated product record in the source system) 
*****************************************************************************************************/
INSERT INTO AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product
(
ProductID,
ProductNumber,
ProductName,
ProductCategory,
ProductSubCategory,
ValidFrom,
ValidTo)
SELECT 
ProductID,
ProductNumber,
ProductName,
ProductCategory,
ProductSubCategory,
'2014-01-01', --- last update + 1
'2099-12-31'
FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_D_Product_Updates

SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product




/***********************************************************************
Step 2.13 ETL for the fact table (incremental load)
- Delete Stage_F_Sales table contents
***********************************************************************/
--- Clean the staging area table before the incremental load of data
DELETE FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales

--- 32 903 records deleted

/***********************************************************************
Step 2.14 ETL for the fact table (incremental load)
- Add facts that occured since the last update
***********************************************************************/
INSERT INTO AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales(
CustomerID,
ProductID,
SalesOrderID,  --- added as duplicates were identified
LineTotal,
OrderQty,
OrderDate)

SELECT 
C.CustomerID,
P.ProductID,
S.SalesOrderID,  --- added as duplicates were identified
(UnitPrice * OrderQty) AS LineTotal,
OrderQty,
OrderDate

FROM
AdventureWorks2017.Sales.Customer C
JOIN AdventureWorks2017.Sales.SalesOrderHeader S ON C.CustomerID = S.CustomerID
JOIN AdventureWorks2017.Sales.SalesOrderDetail D ON S.SalesOrderID = D.SalesOrderID
JOIN AdventureWorks2017.Production.Product P ON P.ProductID = D.ProductID
JOIN AdventureWorks2017.Person.Person PER ON C.PersonID = PER.BusinessEntityID
JOIN AdventureWorks2017.Person.BusinessEntity B ON PER.BusinessEntityID = B.BusinessEntityID
JOIN AdventureWorks2017.Person.BusinessEntityAddress BA ON B.BusinessEntityID = BA.BusinessEntityID
WHERE C.StoreID IS NULL AND BA.AddressTypeID = 2 AND OnlineOrderFlag = 1 
AND OrderDate > (SELECT LastUpdate from AdventureWorks2017_DataWarehouse_SCD.dbo.LastUpdate);

--- 27 495 records affected

SELECT * FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales

/***********************************************************************
Step 2.15 ETL for the fact table (incremental load)
- Perform key lookup
***********************************************************************/

--- PERFORM KEY LOOKUP
UPDATE AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales
SET P_ID = (SELECT P_ID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product WHERE ProductID = Stage_F_Sales.ProductID
AND ValidTo = '2099-12-31');

UPDATE AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales
SET C_ID = (SELECT C_ID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Customer WHERE CustomerID = Stage_F_Sales.CustomerID
AND ValidTo = '2099-12-31');


UPDATE AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales 
SET D_ID = (SELECT D_ID FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Date
WHERE CalendarDate = Stage_F_Sales.OrderDate );

---	Verify change, 0 records within the Stage_F_Sales have the P_ID as NULL => Valid lookup
SELECT COUNT(*) AS P_ID_Surrogate_Null FROM Stage_F_Sales WHERE P_ID IS NULL;

---	Verify change, 0 records within the Stage_F_Sales have the C_ID as NULL => Valid lookup
SELECT COUNT(*) AS C_ID_Surrogate_Null FROM Stage_F_Sales WHERE C_ID IS NULL;


--- Current D_Customer records (after incremental load )
SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Customer

--- Current D_Product records (after incremental load and the 3 CDC)
SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.D_Product

--- Current facts (after initial load)
SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales

--- Current incremental load staging area fact table records
SELECT COUNT(*) FROM AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales

/***********************************************************************
Step 2.15 ETL for the fact table (incremental load)
- Insert new records into the fact table of the DW
***********************************************************************/

INSERT INTO AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales(
C_ID,
P_ID,
D_ID,
SalesOrderID,
LineTotal,
OrderQty)
SELECT
C_ID,
P_ID,
D_ID,
SalesOrderID,
LineTotal,
OrderQty
FROM AdventureWorks2017_StagingArea_SCD.dbo.Stage_F_Sales 

/*
	Comment:
	In order to see a new record in the fact table,
	we would need all the criteria of the SELECT statement
	fulfilled (in the extraction for the Fact table)
*/


SELECT * FROM AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales 














