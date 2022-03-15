-- Delete Schema :
DROP SCHEMA "Raw_Data";

-- Create a Schema :
CREATE SCHEMA "Raw_Data" AUTHORIZATION sraj;

-- Permissions on Schema: 
GRANT ALL ON SCHEMA "Raw_Data" TO sraj;

-- Commands to Create Tables: 
create table client_billing(
	srNo SERIAL primary key,
	Client_Surname1 varchar not null,
	Total_Profit1 float, 
	Total_Bill_Amount float,
	Total_Profit float not null, 
	Total_Bill_Amount1 float not null, 
	Date_Added date not null
)

create table fee_brkdn_dept_fe(
	srNo SERIAL primary key,
	Matter_Dept_Dept_Name varchar not null,
	Fee_Earner_Reference varchar(10) not null, 
	Matter_Ref varchar not null, 
	Profit float8 not null,
	Non_Vatable_Disbursements int, 
	Vatable_Disbursements float8, 
	VAT_Amount float8 not null, 
	Bill_Amount float8 not null, 
	WIP_Cost_Billed float8 not null,
	Date_Added date not null
)

create table fee_smry_dept_fe(
	srNo SERIAL primary key,
	Matter_Dept_Dept_Name varchar not null,
	Fee_Earner_Reference1 varchar(10) not null, 
	Profit float8 not null, 
	Date_Added date not null
)

create table fee_billed(
	srNo SERIAL primary key,
	feeref varchar(10) not null, 
	fe_month date not null,
	splitamount float8 not null,
	Date_Added date not null
)