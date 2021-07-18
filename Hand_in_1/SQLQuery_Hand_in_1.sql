--	SET DATABASE TO QUERY
USE [AdventureWorks2017_DataWarehouse]

--	IMPLEMENTATION OF INITIAL DESIGN

/*
	NOTES:
	1) Syntax: IDENTITY [ (seed , increment) ] 
	Definition: Creates an identity column in a table. 
	This property is used with the CREATE TABLE and ALTER TABLE Transact-SQL statements.
	
	Important: uniqueness must be enforced by using it as a PK

	Default: (1,1) - start at 1, incrementing continuously each row by 1

	Arguments:
	a) seed - value used for the first row inserted into the table
	b) increment - incremental value added to the value of the previously inserted row

	Official documentation:
	https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-transact-sql-identity-property?view=sql-server-ver15

	2)  Dimensions (within the DW) should have conformed data, that is, cleansed, relevant and processed data to support the 
		the business process for which they were identified and business decision making.
		Dimensions serve as the contextual pillars of the fact, together forming the core data layer ("the front kitchen" for the presentation layer,
		the BI application(s).
*/

-- Initial creation of D_Customer (the customer dimension of the DW)
CREATE TABLE [D_Customer](
C_ID [int] IDENTITY(1,1) NOT NULL,	--	Surrogate key, IDENTITY(1,1) used for generation (1..N) where N is the total number of records
Customer_ID [int] NOT NULL,			--	Business/Natural key
Name [nvarchar](100) NOT NULL,		--	Customer name 
City [nvarchar](30) NOT NULL,		--	City (will be used for grouping as customers reside in multiple cities around the world)
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

--	Initial creation of F_Sales
CREATE TABLE [F_Sales](
C_ID [int] NOT NULL, 
P_ID [int] NOT NULL,
D_ID [int] NOT NULL,
LineTotal [money] NOT NULL,
OrderQty [smallint] NOT NULL,

CONSTRAINT FK_F_Sales_0 FOREIGN KEY (C_ID)
REFERENCES D_Customer(C_ID),
CONSTRAINT FK_F_Sales_1 FOREIGN KEY (P_ID)
REFERENCES D_Product(P_ID),
CONSTRAINT FK_F_Sales_2 FOREIGN KEY (D_ID)
REFERENCES D_Date(D_ID),

CONSTRAINT PK_F_Sales PRIMARY KEY (C_ID, P_ID, D_ID)
);