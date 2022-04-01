-- Delete Schema :
-- DROP SCHEMA "Bowling_Data";

-- Create a Schema :
-- CREATE SCHEMA "Bowling_Data" AUTHORIZATION db_Admin;

-- Permissions on Schema: 
-- GRANT ALL ON SCHEMA "Bowling_Data" TO db_Admin;

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

create table fees_billed(
	srno SERIAL primary key,
	feeref varchar(10) not null, 
	tnx_month varchar not null,
	split_amount float8,
	Date_Added date not null
)

-- Write the script to change above column names 


create table mttr_src_ref(
	srno SERIAL primary key,
	Fee_Earner_Reference1 varchar(10) not null, 
	Business_Source1 varchar not null,
	Client_Name1 varchar not null, 
	Matter_Ref varchar not null, 
	Matter_Description varchar,
	Bill_Date date not null,
	Bill_Ref varchar(10) not null, 
	Bill_Amount int,
	Date_Added date not null
)

create table tot_hrs_by_fe(
	srno SERIAL primary key,
	EarnerRef varchar(10) not null, 
	EarnerName varchar not null, 
	RecordedHours varchar not null,
	RecordedHours1 float8, 
	RecordedValue int, 
	NonChargeHours varchar not null,
	NonChargeHours1 float8,
	NonChargeValue int,
	WOHours varchar not null,
	WOHours1 float8,
	WOValue float8,
	TotalHour varchar not null,
	TotalHour1 float8,
	TotalValue int, 
	NetBillings float8,
	Date_Added date not null
)

create table mtrs_by_fe(
	srno SERIAL primary key, 
	fee_earner_full_name1 varchar not null, 
	business_source VARCHAR not null,
	work_type_description1 varchar not null, 
	matter_ref varchar(10),
	Date_Added date not null
)

create table pmt_rcv_analysis(
	srno SERIAL primary key, 
	feeearnercode1 varchar(10) not null,
	mattercode1 varchar(20) not null, 
	client_name VARCHAR(200) not null,
	matbranchref VARCHAR(10),
	postingdetailsdate DATE not null, 
	paymentref varchar(10) not null,
	departmentcode varchar(20) not null, 
	feesreceived float8 not null, 
	feesnet float8 not null, 
	disballoc float8 not null, 
	unalloc int not null, 
	payalloc int not null, 
	feesnet1 float8 not null, 
	feesnet2 float8 not null, 
	Date_Added date not null 
)