USE AdventureWorks2017_StagingArea

/*
	ETL Process design and implementation
*/

/*
	--- Previously done ---
	1) Identification of needed tables
	2) Creation of the DW dimensional model
	3) Documentation of the design for the DW dimensional model
	4) Implementation of the DW tables and relations between them
		-	Dimension tables:
				D_Customer (C_ID - PK)
				D_Product (P_ID - PK)
				D_Date (D_ID - PK)

		-	Fact table: 
				F_Sales ( (C_ID,P_ID,D_ID) - composite PK, each individual attribute of the key
											 being a FK to their respective dimension table 
											 {ex.: C_ID in F_Sales is FK to C_ID PK in the customer dimension} )
*/

/*
	---	ETL OVERVIEW ---

	Definition (of the ETL abreviation): 
		Extract (data from the source system) 
		Transform (data within the staging area)
		Load (transformed/valid data from the staging area into the DW)

	Flow of metadata:

	Source system ==> Staging Area (transformations performed) ==> DataWarehouse (Presentation Area)

	1) the 'E' in ETL
	Extraction (types, both from the source system to the staging area):
		-	Full extraction
		-	Partial extraction
	Considerations: None should affect the system regarding performance, response time or lock

	2) the 'T' in ETL
	Transformation (of metadata within the staging area): a series of tasks, namely, selection, matching, data cleansing, consolidations or summarization of data
	Major transformation types:
		-	Standarising data
		-	Character set conversion/enconding handling
		-	Calculated, derived values
		-	Splitting, merging fields (attributes)
		-	Conversion of units of measurement (ex.: DateTime)
		-	Aggregation
		-	Deduplication
		-	Key restructuring (surrogate keys)

	3)	the 'L' in ETL:
	Loading (of metadata from the staging area, once the data has been transformed and validated to the DW - presentation server/layer)

	Types:
	
	- Initial load (populating the DW tables for the first time)
	- Incremental load (applying ongoing changes as necessary in a periodic manner)
	- Full refresh (complete erase of content for 1..* tables and reloading it/them with new data)
*/



/*
	Current focus:
	-	Extract data from the sources into the Staging area (dedicated area for data manipulation)
	-	Cleansing and transforming the data
	-	Initial load of the dimension tables
	-	Initial load of the Date dimension

	NOTES:
	1)	Data quality
		-	The ETL process must validate and cleanse data
		-	Source systems may provide incosistent, wrong, missing data
		-	There may be bugs in the source systems, these will be fixed over time, however, they live on in the data

	2)	Data profiling and checks
		-	We need to check values and frequencies of data
		-	'Outliers', these are nulls, extreme values and odd values
		-	Dates are a special case, quite often they are difficult to handle
		-	Data inconsistencies may be present, dependent values or relationships

		This can be verified on a source system by verifying the existent data,
		several ways to do this are:
		-	Odd values: SELECT DISTINCT ... (to complete the query)
		-	Distribution of values: SELECT COUNT(*) ... (From tableName) GROUP BY .... (attributeName)
		-	Search for empty fields or NULL values (... (query to be performed) IS NULL..)
*/



/*
	Current phase: ETL Process implementation

	Steps:
		1) Design the ETL Process (through an activity diagram)
		2) Create the staging area tables (data stores), will be
			needed for performing the ETL process
		3) Load data into the Staging area tables (dimensions)
		4) Perform data verification(s)
		5) Conduct necessary transformations (ensure data quality and meaningness)
			-	replacement of null values to a meaningful default value
			(ex.: a NULL for a customer record name could be replaced with 'UNKNOWN')
			-	if allowed by the business requirements, concatenate (group) multiple attributes
			into one which would give more meaning
			(ex.: the customer name will be concatenated as it is not required to identify a customer
			by middleName, lastName etc., essentially a name would suffice (as it uniquely allows filtering of customers)
		6) Once the data has been transformed (validated), load it into the DW dimension tables
		---	End of populating (loading) into the dimension tables ---

		--- Beginning of populating the fact table ---
		7) Create a stage fact table
		8) Populate (load data) into the fact table
		9) Perform key lookup in the DW dimension tables and update the stage fact table
		10) Populate the DW fact table


*/


/*
	*********************************
	ETL PART 1 **********************
	*********************************

	-	Implementation of ETL for all dimensions (D_Customer, D_Product) except D_Date
		a)	Extract data from sorce system (AdventureWorks2017) to the staging area
		b)	Perform transformations on the data as needed
		c)	Perform the initial load of data into the DW (AdventureWorks2017_DataWarehouse)
		d)	Document the ETL part 1 process (revised ERD and activity diagram - ETL process flow)
		e)	Populate the Date dimension table
*/


---	SET STAGING AREA DATABASE FOR USE ---
/*
	As per the feedback, schemas should not be used, instead a separate database for the staging area
*/

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

---	Populate the Stage_D_Customer table (perform extraction)
---	18 484 rows affected (Records)
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

WHERE BA.AddressTypeID = 2;

---DROP TABLE Stage_F_Sales
SELECT COUNT (*) AS Person_Null_MiddleName_Count

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
ON BA.AddressID = A.AddressID ;

---	Verification query for Customer MiddleName (7830 records where a customer MiddleName is NULL)
/*
	In our case, we have concluded that one name attribute would suffice the business requirements previously identified
*/
SELECT * FROM

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

WHERE BA.AddressTypeID = 2 AND MiddleName IS NULL;


---	Verify the data
SELECT * FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Customer;

---	0 results
SELECT COUNT(*) AS CustomerID_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Customer WHERE CustomerID IS NULL;


---	0 results
SELECT COUNT(*) AS Customer_Name_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Customer WHERE Name IS NULL;

---	0	results
SELECT COUNT(*) AS Customer_City_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Customer WHERE City IS NULL;	

/*	As there are no NULL values for the Stage_D_Customer table, we declare the data valid, hence, ready for the initial load into
	the target dimension: AdventureWorks2017_DataWarehouse.D_Customer
*/

---	Populate the Stage_D_Product table (perform extraction)
---	506 results (rows affected)

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


---	Verify the data
SELECT * FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product;
/*
	Currently, all records hold 'NULL' as the value for the ProductCategory and ProductSubCategory
*/

---	All records are unique (506)
SELECT DISTINCT * FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product;

---	0 results, no product has the ProductID as NULL
SELECT COUNT(*) AS ProductID_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductID IS NULL;

---	0 results, no product name is NULL
SELECT COUNT(*) AS Product_Name_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductName IS NULL;

---	0	results, no product number is NULL
SELECT COUNT(*) AS Product_Number_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductNumber IS NULL;	


---	Update NULL attributes (ProductCategory and ProductSubCategory) ---

---	1)	Update product subcategory --- 295 records affected (rows)
UPDATE Stage_D_Product
SET ProductSubCategory = PS.Name
FROM Stage_D_Product AS P 
JOIN AdventureWorks2017.Production.Product AS PR
ON P.ProductID = PR.ProductID
JOIN AdventureWorks2017.Production.ProductSubcategory AS PS
ON PR.ProductSubcategoryID = PS.ProductSubcategoryID;

---	2)	Update product category --- 105 records affected (rows)
UPDATE Stage_D_Product
SET ProductCategory = PC.Name

FROM Stage_D_Product AS P
JOIN AdventureWorks2017.Production.Product AS PR

ON P.ProductID = PR.ProductID

JOIN AdventureWorks2017.Production.ProductSubcategory AS PS
ON PR.ProductSubcategoryID = PS.ProductCategoryID

JOIN AdventureWorks2017.Production.ProductCategory AS PC
ON PS.ProductCategoryID = PC.ProductCategoryID;

---	Verify the changed data
---	211 records identified (Products with ProductSubCategory as NULL)
SELECT COUNT(*) AS Product_Subcategory_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductSubCategory IS NULL;

---	401 records identified (Products with ProductCategory as NULL)
SELECT COUNT(*) AS Product_Category_Null_Count FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product WHERE ProductCategory IS NULL;


---	Handle the NULL values in Stage_D_Product (Replace the NULL values with a default placeholder value)
UPDATE Stage_D_Product
SET
ProductSubCategory = 'NONE' WHERE ProductSubCategory IS NULL;	---	211	records affected (as previously identified)

UPDATE Stage_D_Product
SET
ProductCategory = 'NONE' WHERE ProductCategory IS NULL;	---	401 records affected (as previously identified)





/*
	Perform last verification of the Stage_D_Product table before declaring the data valid (ready for initial load into the dimension)
	Target dimension: AdventureWorks2017_DataWarehouse.D_Product
*/
---	All records are unique (506)
SELECT DISTINCT * FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product;

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

---	The Stage_D_Product table data is declared valid, ready to perform initial load into the target dimension: AdventureWorks2017_DataWarehouse.D_Product



/*
	Perform initial load of data into the AdventureWorks2017_DataWarehouse database for the previously managed dimensions
*/

---	1) Populate the Customer dimension : AdventureWorks2017_DataWarehouse.D_Customer
---	18 484 rows affected (inserted records)
INSERT INTO AdventureWorks2017_DataWarehouse.dbo.D_Customer(
CustomerID,
Name,
City)
SELECT
CustomerID,
Name,
City
FROM 
AdventureWorks2017_StagingArea.dbo.Stage_D_Customer

--- 2) Populate the Product dimension : AdventureWorks2017_DataWarehouse.D_Product
---	506 records affected (inserted records)
INSERT INTO AdventureWorks2017_DataWarehouse.dbo.D_Product(
ProductID,
ProductName,
ProductNumber,
ProductCategory,
ProductSubCategory)

SELECT
ProductID,
ProductName,
ProductNumber,
ProductCategory,
ProductSubCategory

FROM AdventureWorks2017_StagingArea.dbo.Stage_D_Product



/*
	*********************************
	ETL PART 2 **********************
	*********************************

	Notes:
	-	While populating the fact table, we need to lookup for every dimension:
		a) Find Matching business key in the dimension table
		b) Get the surrogate key
		c) Insert surrogate key as foreign key in the fact table
		d) Insert unit_prince * quantity as sales_amount, quantity

	-	Simplified version
		a) Create a stage fact table
		b) Populate the Stage fact table
		c) Look up keys in DW dimension tables (and update the stage fact table)
		d) Populate the DW fact table
*/

USE AdventureWorks2017_StagingArea

---	a) Creation of the Stage_F_Sales Staging Area fact table
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

--- b) Populate the Stage_F_Sales Staging Area fact table (Except surrogate keys)
---	60 398 records affected
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
WHERE C.StoreID IS NULL AND BA.AddressTypeID = 2 AND OnlineOrderFlag = 1;

SELECT * FROM Stage_F_Sales

---	c) Key lookup (in the DW dimension tables) and Stage_F_Sales update

---	Update P_ID (Product dimension surrogate key)
/*
	Match business keys from the Product Dimension with the one from the Stage_F_Sales table to assign the 
	corresponding surrogate key to the sales records in the Stage_F_Sales table
*/

---	60 398 records affected
UPDATE Stage_F_Sales
SET P_ID = (SELECT P_ID FROM AdventureWorks2017_DataWarehouse.dbo.D_Product P WHERE P.ProductID = Stage_F_Sales.ProductID)

---	Verify change, 0 records within the Stage_F_Sales have the P_ID as NULL => Valid lookup
SELECT COUNT(*) AS P_ID_Surrogate_Null FROM Stage_F_Sales WHERE P_ID IS NULL;



---	Update C_ID (Customer dimension surrogate key)
/*
	Match business keys from the Customer Dimension with the one from the Stage_F_Sales table to assign the 
	corresponding surrogate key to the sales records in the Stage_F_Sales table
*/
---	60 398 records affected
UPDATE Stage_F_Sales SET C_ID = C.C_ID FROM AdventureWorks2017_DataWarehouse.dbo.D_Customer AS C JOIN Stage_F_Sales AS S
ON C.CustomerID = S.CustomerID WHERE C.CustomerID = S.CustomerID

---	Verify change, 0 records within the Stage_F_Sales have the C_ID as NULL => Valid lookup
SELECT COUNT(*) AS C_ID_Surrogate_Null FROM Stage_F_Sales WHERE C_ID IS NULL;



---	Update D_ID (Date dimension surrogate key)
/*
	Match business keys from the Date Dimension with the one from the Stage_F_Sales table to assign the 
	corresponding surrogate key to the sales records in the Stage_F_Sales table
*/
---	60 398 records affected
UPDATE Stage_F_Sales
SET D_ID = (SELECT D_ID FROM AdventureWorks2017_DataWarehouse.dbo.D_Date D WHERE D.CalendarDate = Stage_F_Sales.OrderDate)

---	Verify change, 0 records within the Stage_F_Sales have the D_ID as NULL => Valid lookup
SELECT COUNT(*) AS D_ID_Surrogate_Null FROM Stage_F_Sales WHERE D_ID IS NULL;

---	60 391 unique records
SELECT DISTINCT C_ID,D_ID,P_ID,CustomerID,ProductID,LineTotal,OrderDate,OrderQty FROM Stage_F_Sales

---	7 Customers have made a purchase multiple times during the same date (day)
SELECT COUNT(*) Occurences,C_ID,D_ID,P_ID,CustomerID,ProductID,LineTotal,OrderDate,OrderQty 
FROM Stage_F_Sales GROUP BY C_ID,D_ID,P_ID,CustomerID,ProductID,LineTotal,OrderDate,OrderQty HAVING COUNT(*) >1;

DROP TABLE Stage_F_Sales --- duplicates identified


---		d) Populate the DW fact table --- 60 398 rows affected
INSERT INTO AdventureWorks2017_DataWarehouse.dbo.F_Sales(
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

---	All current AdventureWorks2017_DataWarehouse.dbo.F_Sales are atomic (no duplicates)
SELECT COUNT(*) Occurences,C_ID,D_ID,P_ID,LineTotal,OrderQty, SalesOrderID 
FROM AdventureWorks2017_DataWarehouse_SCD.dbo.F_Sales GROUP BY C_ID,D_ID,P_ID,LineTotal,OrderQty, SalesOrderID HAVING COUNT(*) >1;

DROP TABLE Stage_D_Customer
DROP TABLE Stage_F_Sales