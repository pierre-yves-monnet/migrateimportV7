# migrateimportV7

Example of CSV file accepted
* file name is the process name
* content is for example (2 lines, 2 cases will be created)

# Principle

See the Repair API
The Repair documentation:
* Can't handle any rendez-vous, so if you have a Step 1 / Step 2 in parallelle, and a rendez vous AFTER, the import can creates tasks in Step 1 and Step 2, but the case will be stuck in the Rendez vous gateway

* can't handle sub process. 

An iteration task can be handle by the Repair API, then by the import <to be documented>



# parameters
[option] APPLICATIONURL APPLICATIONNAME LOGINNAME PASSWORD IMPORTPATH MOVEIMPORTEDPATH URLDATABASE

## APPLICATIONURL
To call the Bonita Engine, example http://localhost:8080

## APPLICATIONNAME
Server application name "bonita" in general

## LOGINNAME 
Login to connect, "walter.bates" for example

## PASSWORD 
Password of the user

## IMPORTPATH 
Path where the CSV are present.

## MOVEIMPORTEDPATH 
When the file is imported, then file is moved in this path

## URLDATABASE
Url to connect to the database. This is not possible by the API to update an STRING INDEX. String Index are used to register the case loaded, in order to not reload it twice if you restart the import.
This is optionnal if
* you load only active case
* you deactived the control via "indexname noindex" option and you deactived the history via "-historyinindex noindex" option
 
 
 
# Fields in CSV

## TypeLine
TypeLine : P_A (active Case) or P_F (Archived case)

## CaseId
CaseId : the caseId is saved as the StringIndex1. This avoid to load the same case twice: a first search is done with StringIndex1 == <CaseId>. If a case is found, import consider the case was already imported.
Example, CaseId = 489192

## Tasks
Tasks: Give a list of tasks separate by # Each task contains this information

 <processName>--<processVersion>--<processInstanceId>--<tasksName>--<READY|FAILED|EXECUTING>
 
 or this for an archive task:
 <processName>--<processVersion>--<processInstanceId>--<tasksName>--<READY|FAILED|EXECUTING>--<Date:format yyyyMMdd HH:mm:ss>--<ExecutedBy>

It's possible to create a case in a Service Task. 
Keep in mind variables are update AFTER the creation. So, if a task is created in a task "CompleteName", then the case is created, executed and then variable are updated. So, if a connector in the task need variables, it will not have them initialized.

Example:ImportInMyProcess--1.0--489192--Step 1--READY#ImportInMyProcess--1.0--489192--Complete Name--READY

## RendezVous
RendezVous: <Not Implemented>

## Variables
Variables: 
Must me a Map of Variables. Key is the Process -- Version -- CaseId. When the case contains Sub process, this is the possibility to give value on each process / subprocess
  
Example: the caseID has variables
{ "ImportInMyProcess--1.0--489192" : { "FirstName": "S:John", "LastName":"S:Malkovith", "Age" : "L:54 } }



# Example
TypeLine;CaseId;StartedBy;StartedDate;Tasks;RendezVous;Variables;SubProcessInstance;VariablesActivity
P_A;489192;Walter.Bates;20190206 17:31:20;ProvideInformation;;"{""expenseDetail"":{""amount"":""S:22830"",""currency"":null,""processID"":null,""cod_int_fluxo"":""S:1832"",""exercicio"":""S:2019"",""labels"":""S:Restaurant,Hotel,Car"",""comments"":""S:sim"",""ProcessID"":""S:443"",""Department"":""S:Sale""}}";;
P_A;483833;Helen.Kelly;20190129 15:56:46;Review Expense;;"{""expenseDetail"":{""amount"":""L:17603"",""currency"":null,""processID"":""S:DRPE_GPRE_Consulta_Formal--1.0"",""cod_int_fluxo"":""L:3003"",""exercicio"":""L:2019"",""labels"":""S:Hotel"",""comments"":""S:sim"",""Department"":""S:R&D""}}";;
 
