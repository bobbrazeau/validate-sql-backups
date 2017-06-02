##############################################################################
#.SYNOPSIS
# Script to verify backups for SQL Server and log results
#
#.DESCRIPTION
# By setting a few constraints the script will traverse all subfolders 
# In each folder it will find the most recent .bak file.
#
# The script reads the logical file name and physical location from the backup 
# These are used to rename and move during the restore process.  
#
# Once restored DBCC CheckDB is run to verify the database's integrity.
# Finally the restored database is dropped.
# 
# The actions of the process are logged to a database by a trio of stored procedure calls.
#
# Inspiration - https://stuart-moore.com/31-days-of-sql-server-backup-and-restores-with-powershell-index/
#
#.LINK
# https://github.com/bobbrazeau/validate-sql-backups
#
##############################################################################


#Requires -Modules SqlServer

<#
# Import SQLPS if running on a pre-2016 SQL Server.  Use SqlServer if 2016 or later
#import-module "SQLPS" -DisableNameChecking

#>

# clear window so only output is shown.
clear-host
$error.clear()


# Set Local Enviroment Variables
$ServerName = "(local)"
$restoreDataPath = "B:\backupFolderData\"
$restoreLogPath = "B:\backupFolderLog\"
$restoreDbName = "checkdb"
$loggingDB = "loggingDBCC"
# 256 MB extra drive space after the restore
$freeSpaceBuffer = 256MB 

# Restore info - TODO - use an array so backups from multiple servers can be restored.
$backupSource =  "B:\"
$friendlyServerName = "Test Server" #useful if multiple servers exist

<#
.Synopsis         
  Get info from backup file about current logical and physical file info.  Create new physical path based on path variables
#>
function getBackupInfo($ServerName, $restoreDbName, $backupName, $restoreDataPath, $restoreLogPath) {
    $restore = new-object Microsoft.SqlServer.Management.Smo.Restore
    $deviceType = [Microsoft.SqlServer.Management.Smo.DeviceType]::File
    $restoreDevice = New-Object -TypeName Microsoft.SQLServer.Management.Smo.BackupDeviceItem($backupName,$deviceType)
    $restore.Devices.add($restoreDevice)

    # databases can contain multiple data files so use an array. Keep some overall Database info and some file specific info.
    $fileData = $restore.ReadFileList($ServerName)
    $fileInfo = @{}
    $fileInfo["LogSize"] = 0
    $fileInfo["DataSize"] = 0
    $fileInfo["Count"] = 0 

    $i = 0
    foreach($row in $fileData.Rows)
    {
        $fileInfo[$i] = @{}
        $fileInfo[$i]["LogicalName"] = $row.LogicalName
        $fileInfo[$i]["PhysicalName"] = $row.PhysicalName
        $fileInfo[$i]["Size"] = $row.Size
        $fileInfo[$i]["Type"] = $row.Type
        if($row.Type.ToString() -eq "D")
        {
            $fileInfo["DataSize"] += $row.Size
            $fileInfo[$i]["NewPhysicalName"] = $restoreDataPath + $restoreDbName + $i.toString() + [System.IO.Path]::GetExtension($row.PhysicalName)
        }
        ElseIf($row.Type.ToString() -eq "L")
        {  
            # while there should only be 1 ldf this should handle whatever is provided.
            $fileInfo["LogSize"] += $row.Size
            $fileInfo[$i]["NewPhysicalName"] =$restoreLogPath + $restoreDbName + $i.toString() + [System.IO.Path]::GetExtension($row.PhysicalName)
        }
        $i++
    }
    $fileInfo["Count"] = $i;
    Return $fileInfo
}

<#
.Synopsis         
  As it says on the tin, Restore the backup under the new information, veriy its integrity then remove it.
#>
function restoreVerifyDropDatabase($ServerName, $oldDbName, $backupname, $fileInfo, $restoreName){
    $sqlsvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
    $restore = new-object Microsoft.SqlServer.Management.Smo.Restore
    $devicetype = [Microsoft.SqlServer.Management.Smo.DeviceType]::File

    $restoredevice = New-Object -TypeName Microsoft.SQLServer.Management.Smo.BackupDeviceItem($backupname,$devicetype)
    $restore.Database = $restoreDbName
    $restore.ReplaceDatabase = $True
    $restore.NoRecovery = $false
    $restore.FileNumber = 1
    $restore.Devices.add($restoredevice)
    for($i=0; $i -lt $fileInfo.Count; $i++)
    {
        $relocate = New-Object Microsoft.SqlServer.Management.Smo.RelocateFile ($fileInfo[$i].LogicalName,$fileInfo[$i].NewPhysicalName)
        $restore.RelocateFiles.add($relocate)
    }

    # Restore DB and run checkdb.  If errors detected save results to variable for logging purposes.
    # Set the timeout to maxvalue (5 hours) as the default 30 seconds isn't enough time to run checkdb
    $restore.sqlrestore($ServerName)
    $dbccQuery = "DBCC CHECKDB ([" + $restoreDbName.ToString() + "]) WITH TABLERESULTS, NO_INFOMSGS"
    $dbccResults = Invoke-SQLCmd -ServerInstance $ServerName -Query $dbccQuery -querytimeout ([int]::MaxValue)
    $errorMessage = ""

    # If checkdb is clean, there will be no results (length = 0), but if an error is encountered have to pull the message out.     
    if ($dbccResults.length -ne 0){
        foreach ($line in $dbccResults){
            $errorMessage += $line.Item(3).ToString()  #Column 3 has the message - better way then referencing and id?
        }
    } else { $errorMessage = ""  }
    $db = New-Object -TypeName Microsoft.SqlServer.Management.Smo.Database -argumentlist $ServerName, $restoreDbName  
    $db = $sqlsvr.Databases[$restoreDbName]  
    $db.Drop()

    # delay a second to give sql server time to clean up.
    Start-Sleep -Milliseconds 1000 
    return $errorMessage
}

<#
.Synopsis
    Ensures there is enough free space to restore the database.
    Check before each restore as other activity could fill up drive.
#>
function checkFreeSpace($logPath, $dataPath, $logUsed, $dataUsed, $buffer){
    $FSO = New-Object -Com Scripting.FileSystemObject
    $logInfo = $FSO.getdrive($(Split-Path $logPath -Qualifier))
    $logSpace = $logInfo.AvailableSpace - $buffer

    $dataInfo = $FSO.getdrive($(Split-Path $dataPath -Qualifier)) 
    $dataSpace = $dataInfo.AvailableSpace  - $buffer 

    # if using the same drive for log and data files, add their space together
    if($logInfo.DriveLetter -eq $dataInfo.DriveLetter){
        $dataSpace = $dataSpace - $LogUsed - $dataUsed - $buffer
    } else {
        $dataSpace = $dataSpace - $dataUsed - $buffer
        $logSpace = $logSpace - $logUsed - $buffer
    }
    $ret = 0 
    if($logSpace -gt 0 -And $dataSpace -gt 0){
        $ret = 1
    }
    return $ret
}

<#
.Synopsis         
    Start logging of the run.  Returns ID of the header record.
#>
function LoggingInit($serverName, $loggingDB, $sourceServerName){
    # Create a record in the master table for the run.  Calls a SP which returns the ID which individual DBs reference.
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Server=" + $serverName + ";Database=" + $loggingDB + ";Integrated Security=True"
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $loggingDB + ".dbo.dbccLogStart"
    $SqlCmd.Connection = $SqlConnection
    $SqlCmd.CommandType = [System.Data.CommandType]'StoredProcedure';

    $inParameter = new-object System.Data.SqlClient.SqlParameter;
    $inParameter.ParameterName = "@serverName";
    $inParameter.Direction = [System.Data.ParameterDirection]'Input';
    $inParameter.DbType = [System.Data.DbType]'String';
    $inParameter.Size = 255;
    $inParameter.Value = $sourceServerName;
    $SqlCmd.Parameters.Add($inParameter) >> $null;

    $outParameter = new-object System.Data.SqlClient.SqlParameter;
    $outParameter.ParameterName = "@id";
    $outParameter.Direction = [System.Data.ParameterDirection]'Output';
    $outParameter.DbType = [System.Data.DbType]'Int32';
    $SqlCmd.Parameters.Add($outParameter) >> $null;

    $SqlConnection.Open();
    $result = $SqlCmd.ExecuteNonQuery();
    $returnID = $SqlCmd.Parameters["@id"].Value;
    $SqlConnection.Close();
    return $returnID;
}
<#
.Synopsis
    Log the results of a single backups validation.
#>

function LoggingDB($serverName, $loggingDB, $backupFile, $logID, $dbName, $errors, $errorMsg, $logStart, $logEnd){
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Server=" + $serverName + ";Database=" + $loggingDB + ";Integrated Security=True"
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $loggingDB + ".dbo.dbccLogDB"
    $SqlCmd.Connection = $SqlConnection
    $SqlCmd.CommandType = [System.Data.CommandType]'StoredProcedure';

    $inLogID = new-object System.Data.SqlClient.SqlParameter;
    $inLogID.ParameterName = "@masterID";
    $inLogID.Direction = [System.Data.ParameterDirection]'Input';
    $inLogID.DbType = [System.Data.DbType]'Int32';
    $inLogID.Value = $logID;
    $SqlCmd.Parameters.Add($inLogID) >> $null;

    $inDBName = new-object System.Data.SqlClient.SqlParameter;
    $inDBName.ParameterName = "@dbName";
    $inDBName.Direction = [System.Data.ParameterDirection]'Input';
    $inDBName.DbType = [System.Data.DbType]'string';
    $inDBName.size = 255;
    $inDBName.Value = $dbName;
    $SqlCmd.Parameters.Add($inDBName) >> $null;

    $inDBPath = new-object System.Data.SqlClient.SqlParameter;
    $inDBPath.ParameterName = "@filePath";
    $inDBPath.Direction = [System.Data.ParameterDirection]'Input';
    $inDBPath.DbType = [System.Data.DbType]'string';
    $inDBPath.size = 255;
    $inDBPath.Value = $backupFile;
    $SqlCmd.Parameters.Add($inDBPath) >> $null;

    $inErrors = new-object System.Data.SqlClient.SqlParameter;
    $inErrors.ParameterName = "@errors";
    $inErrors.Direction = [System.Data.ParameterDirection]'Input';
    $inErrors.DbType = [System.Data.DbType]'Int32';
    $inErrors.Value = $errors;
    $SqlCmd.Parameters.Add($inErrors) >> $null;

    $inErrorMsg = new-object System.Data.SqlClient.SqlParameter;
    $inErrorMsg.ParameterName = "@errorMsg";
    $inErrorMsg.Direction = [System.Data.ParameterDirection]'Input';
    $inErrorMsg.DbType = [System.Data.DbType]'string';
    $inErrorMsg.size = 4000;
    $inErrorMsg.Value = $errorMsg;
    $SqlCmd.Parameters.Add($inErrorMsg) >> $null;

    $inStart = new-object System.Data.SqlClient.SqlParameter;
    $inStart.ParameterName = "@startTime";
    $inStart.Direction = [System.Data.ParameterDirection]'Input';
    $inStart.DbType = [System.Data.DbType]'DateTime';
    $inStart.Value = $logStart;
    $SqlCmd.Parameters.Add($inStart) >> $null;

    $inEnd = new-object System.Data.SqlClient.SqlParameter;
    $inEnd.ParameterName = "@endTime";
    $inEnd.Direction = [System.Data.ParameterDirection]'Input';
    $inEnd.DbType = [System.Data.DbType]'DateTime';
    $inEnd.Value = $logEnd;
    $SqlCmd.Parameters.Add($inEnd) >> $null;

    $SqlConnection.Open();
    $SqlCmd.ExecuteNonQuery();
    $SqlConnection.Close();
}
<#
.Synopsis
    Call Stored Procedure to wrap up the run.
#>

function LoggingFinalize($serverName, $loggingDB, $logID){
    # After all the DBs are verified, finalize the record on the master table which includes some totals and time info.
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Server=" + $serverName + ";Database=" + $loggingDB + ";Integrated Security=True"
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $loggingDB + ".dbo.dbccLogEnd"
    $SqlCmd.Connection = $SqlConnection
    $SqlCmd.CommandType = [System.Data.CommandType]'StoredProcedure';

    $inParameter = new-object System.Data.SqlClient.SqlParameter;
    $inParameter.ParameterName = "@id";
    $inParameter.Direction = [System.Data.ParameterDirection]'Input';
    $inParameter.DbType = [System.Data.DbType]'Int32';
    $inParameter.Value = $logID;
    $SqlCmd.Parameters.Add($inParameter) >> $null;

    $SqlConnection.Open();
    $SqlCmd.ExecuteNonQuery();
    $SqlConnection.Close();
}


##
# Check out the enviroment - make sure the server can be connected and the folders exist.
# Could have it email out a message - or depending on the error log that back to the DB server.
##
$sqlsvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
if ($sqlsvr.Edition  -eq $null) 
{
    write-output "Could not connect to server"
    exit
} 
if( $sqlsvr.Databases[$restoreDbName] -ne $null) 
{
    write-output "Target database [$restoreDbName] for testing restore already exists.  Exiting"
    exit
} 
if($sqlsvr.Databases[$loggingDB] -eq $null) 
{
    write-output "Target logging database does not exists.  Exiting"
    exit
} 
if((Test-Path $restoreDataPath) -eq $false)
{
    write-output "Restore Datafile [$restoreDataPath] destination does not exists.  Exiting"
    exit
} 
if((Test-Path $restoreLogPath) –eq $false)
{
    write-output "Restore Logfile destination does not exists.  Exiting"
    exit
} 
if((Test-Path $backupSource) –eq $false)
{
    write-output "Backup Root folder does not exists.  Exiting"
    exit
}

$logID = LoggingInit $serverName $loggingDB $friendlyServerName
$folders = Get-ChildItem -Recurse $backupSource -Directory

foreach ($subfolder in $folders){
    try{
        # Get the most recent backup file from each subfolder
        $file = Get-ChildItem $subfolder.FullName -File -Filter "*.bak" | sort-object LastWriteTime | select-object -last 1
        
        if($file -eq $null){
            continue
        }
        $backupFile = $subfolder.FullName + "\" + $file
        $logStart = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")

        # read file to get size information and other properties of the backup.
        $backupInfo = getBackupInfo $ServerName $restoreDbName $backupFile $restoreDataPath $restoreLogPath
        
        # dbcc check against master will always fail so exclude
        if ($backupInfo[0].LogicalName -eq "master") {
            continue
        }
        
        $oldDbName = $backupInfo[0].LogicalName
        $free = checkFreeSpace $restoreLogPath $restoreDataPath $backupInfo.LogSize $backupInfo.DataSize $freeSpaceBuffer
        if ($free -eq 1){
            [string]$errorMessage = ""
            $errorMessage = restoreVerifyDropDatabase $ServerName $oldDbName $backupFile $backupInfo $restoreDbName              
            $logEnd = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")

            if($errorMessage.Length -lt 5){
                LoggingDB $serverName $loggingDB $backupFile $logID $oldDbName 0 "" $logStart $logEnd
            } else {
                LoggingDB $serverName $loggingDB $backupFile $logID $oldDbName 1 $errorMessage $logStart $logEnd
            }
        } else {
            $logEnd = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
            LoggingDB $serverName $loggingDB $backupFile $logID $oldDbName 1 "Not enough Free Space" $logStart $logEnd
        }
    } catch {
        "in catch for some reason ... [$backupFile]"
        $logEnd = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")

        # Capture the entire error message as a string so it can be logged.
        # http://stackoverflow.com/questions/38419325/catching-full-exception-message
        $e = $_.Exception
        $errorMessage = $e.Message
        while ($e.InnerException) {
          $e = $e.InnerException
          $errorMessage += "`n" + $e.Message
        }
       LoggingDB $serverName $loggingDB $backupFile $logID $oldDbName 1 $errorMessage $logStart $logEnd
    }
}
LoggingFinalize $serverName $loggingDB $logID
