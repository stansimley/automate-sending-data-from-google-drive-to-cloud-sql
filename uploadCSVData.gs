/*
README

Steps:
1. change folderId and parentFolderId in config file
2. change runtime to Rhino V8 (on top left, 'Run' > Disable new Apps Script runtime powered by Chrome V8)*
2. change hardcoded values below
3. optional: run createDatabase to create database
4. run createTable to create table
5. create trigger to run uploadFolder regularly 


To query for all data in the table, run the readFromTable function
To clear the table, run the clearTable function

*issues: well known error due to long processing time/certain sql queries: 'Error	Exception: Statement cancelled due to timeout or client request'
*/




//Hardcode BELOW for each folder:************************************************************************
//config file ID*****************************************************************************************
var configId = '18TqSnMtPIx9pLh8xhBHZyjnHpQ2vSUFNKPTGG4wkZGY';




var ss = SpreadsheetApp.openById(configId);
var sheet = ss.getSheets()[0]; //access data in first sheet (tab)
sheet = ss.setActiveSheet(sheet); //set as active sheet
var range = sheet.getDataRange(); //from all columns
var values = range.getValues(); //get all data 

if (!values) {
  Logger.log('Config file not found!');
} 
else {
  //Cloud Project details
  var connectionName = values[1][0];
  var rootPwd = values[1][1];
  
  //Database details
  var user = values[1][2];
  var userPwd = values[1][3];
  var db = values[1][4];
  var root = values[1][5];
  var instanceUrl = 'jdbc:google:mysql://' + connectionName;
  var dbUrl = instanceUrl + '/' + db;
  
  //folder Id
  var folderId = values[1][6];
  var folderObject = DriveApp.getFolderById(folderId);
  
  //parent folder Id; MAIN FOLDER
  var parentFolderId = values[1][7];
  
  //CSV format
  var encoding = values[1][8];//file encoding
  var delimiter = values[1][9];//delimiter/seperator
  var parametersRow = values[1][10];//columns/row where parameters are stated
  var startRow = values[1][11];//first row of data fields
  
  //archive folder Id
  var archiveFolderIdCell = sheet.getRange(2, 13)
  var archiveFolderId = values[1][12];
  
  //Table name
  var tableNameCell = sheet.getRange(2, 14)
  var tableName = values[1][13];
  
  //number of variables in dataset; if table already created => numCol established
  var numColCell = sheet.getRange(2, 15)
  var numCol = values[1][14];
}

//var conn = Jdbc.getCloudSqlConnection(dbUrl, user, userPwd);
//establish connectinon to SQL server (execution time ~ 4s)

function uploadFolder() {  
  var conn = Jdbc.getCloudSqlConnection(dbUrl, user, userPwd);
  conn.setAutoCommit(false);

  var fileIterator = folderObject.getFiles(); //iterator for all files in folder
  
  var archiveFolder = DriveApp.getFolderById(archiveFolderId);
  
  //loop through all sheets except 'config'
  while (fileIterator.hasNext()) { 
    file = fileIterator.next();//files start from second iteration
    
    fileType = file.getMimeType();//get media type of file
    var mimeTypes = ['text/csv', 'application/vnd.ms-excel'];//list of media types to filter
    if (mimeTypes.indexOf(fileType) !== -1){ //if csv or (csv saved in ms excel format)
      Logger.log("File name: " + file)
      uploadSingleCSV(conn, file);
      conn.commit();
      file.moveTo(archiveFolder);
    }
  }
  conn.close();
}

function uploadSingleCSV(conn, file) {//Time: 170s (4s + 3* (~50+ s))
  var values = Utilities.parseCsv(file.getBlob().getDataAsString(encoding), delimiter);//decode CSV format (execution time ~1s/10000rows)
  if (values[parametersRow].length > numCol) {//length of columns > number of columns of table => new columns could be added
    addColumns(conn, values);
  }
  
  ////list CSV column names in order with SQL conventions (camelize and remove special char) 
  var csvColumns = 'fileName,'
  for (var i = 0; i < values[parametersRow].length; i++ ) {
    var column = camelize(values[parametersRow][i]).replace(/[^a-zA-Z ]/g, "");
    if (column.toLowerCase() == 'int') {
      column = 'intensity';
    }
    csvColumns = csvColumns + column + ',';
  }
  csvColumns = '(' + csvColumns.slice(0,-1) + ')';
  
  //Add data to table 
  var fileName = file.getName();
  var rowData = "";
  for (var i = 0; i < values.length; i++) {
    if (i < startRow) {
      continue; //first row assumed to be sep=; and second row, the column names
    }
    else {
      //Get each cell of current row
      row = "'" + fileName + "',";
      for (var j = 0; j < values[i].length; j++) {
        if (values[i][j]) {
        row = row + " '" + values[i][j] + "',";
        }
        else {
          row = row + "NULL,";//append data as NULL is no data in cell
        }
      }
      rowData = rowData + '(' + row.slice(0, -1) + '),'; 
      if (i%100==0){// upload in batches of 100 with a bulk insert; consider stmt.addBatch() for futher optimisation
        Logger.log("Success");
        Logger.log('INSERT INTO '+ tableName + ' ' + csvColumns + ' VALUES ' + rowData.slice(0,-1) + ';');
        conn.createStatement().executeUpdate('INSERT INTO '+ tableName + ' ' + csvColumns + ' VALUES ' + rowData.slice(0,-1) + ';');
        rowData = '';
      }
    }
  }
  if (rowData) {
    conn.createStatement().executeUpdate('INSERT INTO '+ tableName + ' ' + csvColumns + ' VALUES ' + rowData.slice(0,-1) + ';');
  }
}

function addColumns (conn, values) {  
  var stmt = conn.createStatement()
  stmt.setMaxRows(10);
  var resultSet = stmt.executeQuery("SELECT * FROM " + tableName);
  var resultSetMetaData = resultSet.getMetaData();
  
  //error logging + filter new columns (check if ALL columns in table are also in CSV file then add new columns; skip "fileName" column)
  //get list of columns in CSV file
  var csvColumns = [];
  for (var i = 0; i < values[parametersRow].length; i++ ) {
    csvColumns.push(camelize(values[parametersRow][i]).replace(/[^a-zA-Z ]/g, ""));//list CSV column names with SQL conventions (camelize and remove special char) 
  }
  //compare columns in table with list of columns in CSV file
  var missingColumns = ''
  for (var i = 0; i < resultSetMetaData.getColumnCount(); i++ ) {
    if (csvColumns.indexOf(resultSetMetaData.getColumnName(i+1)) === -1) {//if column in table has no match in CSV file; result set meta data starts from 1
      if (resultSetMetaData.getColumnName(i+1) == "fileName") {//skip "fileName" column 
          continue;
      }
      missingColumns = missingColumns + resultSetMetaData.getColumnName(i+1);//append missing columns from CSV
    }
    //remove columns that have been checked => at the end of the loop, only columns to be added remain
    var index = csvColumns.indexOf(resultSetMetaData.getColumnName(i+1));
    if (index > -1) {
      csvColumns.splice(index, 1);
    }
  }
  if (missingColumns != '') {
    Logger.log("Error adding new columns: " + resultSetMetaData.getColumnName(i+1));//throw error stating missing columns from CSV
  }
  Logger.log("Columns to add: " + csvColumns);//show columns to be added
  
  //append new columns into table (columns in CSV that are not in table)
  var query = 'ALTER TABLE ' + tableName;
  for (var i = 0; i < csvColumns.length; i++) {
    if (csvColumns[i].toLowerCase().indexOf('time') !== -1) {
      var dataType = 'TIME';
    }
    else if (csvColumns[i].toLowerCase() == 'int') {
      csvColumns[i] = 'intensity';
    }
    else {   
      var dataType = 'FLOAT'; 
    }
    query = query + ' ADD ' + csvColumns[i] + ' ' + dataType + ',';
  }
  var stmt = conn.createStatement();
  Logger.log(query.slice(0,-1) + ';');
  stmt.executeUpdate(query.slice(0,-1) + ';');
  
  //update cofig file with new number of columns/parameters
  numColCell.setValue(values[parametersRow].length); 
}     

function camelize(str) {
  return str.replace(/(?:^\w|[A-Z]|\b\w)/g, function(word, index) {
    return index === 0 ? word.toLowerCase() : word.toUpperCase();
  }).replace(/\s+/g, '');
}

function createTable() {
  var conn = Jdbc.getCloudSqlConnection(dbUrl, user, userPwd);
  
  var tableName = folderObject.getName();// tableName is the folder name
  
  //update config file with tableName
  tableNameCell.setValue(tableName); 
  
  var fileIterator = folderObject.getFiles(); //iterator for all files in folder
  //loop through all files
  while (fileIterator.hasNext()) { 
    file = fileIterator.next();
    fileType = file.getMimeType();
    if (fileType === 'text/csv'){
      var values = Utilities.parseCsv(file.getBlob().getDataAsString(encoding), delimiter);//decode CSV format (execution time ~1s/10000rows)
      break;
    }
  }
  
  //update config file with number of columns using values
  var numCol = values[parametersRow].length;//Get number of columns (note: columns may start in row 1 or 2- if "sep" is specified in file)
  numColCell.setValue(numCol); 
  
  //get name of columns and convert to SQL naming convention for table
  columns = 'fileName VARCHAR(255),'
  for (var j = 0; j < numCol; j++) {
    var currentCol = camelize(values[parametersRow][j]).replace(/[^a-zA-Z ]/g, "")//convert column names to SQL naming convention (camelize converts to pascal case and replace function to remove all special characters (non-alphaumeric))
    Logger.log(currentCol.toLowerCase());
    if (currentCol.toLowerCase().indexOf('time') !== -1) {
      currentCol = currentCol + ' TIME,';
    }
    else if (currentCol.toLowerCase() == 'int') {
      currentCol = 'intensity FLOAT,';
    }
    else {   
      currentCol = currentCol + ' FLOAT,'; 
    }
    columns = columns + currentCol
  }
  columns = columns.slice(0, -1);//remove extra comma at the end
  Logger.log('CREATE TABLE ' + tableName + ' (' + columns + '); ');
  conn.createStatement().executeUpdate('CREATE TABLE ' + tableName + ' (' + columns + '); '); //Create table
  conn.close();
  
  //create new folder for achieves
  var parentFolder = DriveApp.getFolderById(parentFolderId);
  var archiveFolder = parentFolder.createFolder(tableName + "_archive");
  var archiveFolderId = archiveFolder.getId();
  archiveFolderIdCell.setValue(archiveFolderId);
}




/*
Utilities
*/




function testAddColums() {
  var conn = Jdbc.getCloudSqlConnection(dbUrl, user, userPwd);
  conn.createStatement().executeUpdate('ALTER TABLE ' + tableName + ' ADD ' + 'tMGaSourceCurrentValue FLOAT, ADD ' + 'column2 FLOAT;');
}

function testDeleteColums() {
  var conn = Jdbc.getCloudSqlConnection(dbUrl, user, userPwd);
   conn.createStatement().executeUpdate('ALTER TABLE ' + tableName + ' DROP tMGaSourceCurrentValue, DROP tMGaInjectCurrentValue;');
}

function clearTable() {
  var conn = Jdbc.getCloudSqlConnection(dbUrl, user, userPwd);
  conn.createStatement().execute('TRUNCATE ' + tableName + '; ');
  conn.close()
}

function readFromTable() {
  var conn = Jdbc.getCloudSqlConnection(dbUrl, user, userPwd);

  var start = new Date();
  var stmt = conn.createStatement();
  stmt.setMaxRows(10);
  var results = stmt.executeQuery('SELECT * FROM ' + tableName + ' ORDER BY timeRel DESC LIMIT 10;');
  var numCols = results.getMetaData().getColumnCount();
  
  var values = [];
  while (results.next()) {
    var temp = [];
    for (var col = 0; col < numCols; col++) {
      temp.push(results.getString(col + 1));
    }
    values.push(temp);
  }
  Logger.log(values);
  results.close();
  stmt.close();

  var end = new Date();
  Logger.log('Time elapsed: %sms', end - start);
}

function deleteTable() {
  var conn = Jdbc.getCloudSqlConnection(dbUrl, root, rootPwd);
  stmt = conn.createStatement();
  var sql = "DROP TABLE " + tableName;
  stmt.execute(sql);
}

/**
 * Create a new database within a Cloud SQL instance.
 */
function createDatabase() {
  var conn = Jdbc.getCloudSqlConnection(instanceUrl, root, rootPwd);
  conn.createStatement().execute('CREATE DATABASE ' + db);
}

/**
 * Create a new user for your database with full privileges.
 */
function createUser() {
  var conn = Jdbc.getCloudSqlConnection(dbUrl, root, rootPwd);

  var stmt = conn.prepareStatement('CREATE USER ? IDENTIFIED BY ?');
  stmt.setString(1, user);
  stmt.setString(2, userPwd);
  stmt.execute();

  conn.createStatement().execute('GRANT ALL ON `%`.* TO ' + user);
}
