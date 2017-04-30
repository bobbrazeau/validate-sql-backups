<#
Set a few constants and the script will loop throuh the subfolders looking for the most recent .bak file.
Once found it will read in the logical file name and physical location.  
It uss the $restoreDB variable to generate new locations and names for all files
Using that information it will restore the database moving/relocating to the new locations allowing validation of DBs on same server
Then it will run DBCC CheckDB to detect any corruption.
Finally it will drop the new database and start the process over for any other backup files.

# Inspiration
# https://stuart-moore.com/31-days-of-sql-server-backup-and-restores-with-powershell-index/
#

#>
cls
import-module "SQLPS" -DisableNameChecking

function getBackupInfo($ServerName, $restoreDbName, $backupName, $restoreDataPath, $restoreLogPath) {
    #Restore database to get current logical and physical file info.  Create new physical path based on path variables
    $sqlSvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
    $restore = new-object Microsoft.SqlServer.Management.Smo.Restore
    $deviceType = [Microsoft.SqlServer.Management.Smo.DeviceType]::File
    $restoreDevice = New-Object -TypeName Microsoft.SQLServer.Management.Smo.BackupDeviceItem($backupName,$deviceType)
    $restore.Devices.add($restoreDevice)

    #$fileData = $restore.ReadFileList($sqlSvr)
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
        {  #Grab extension - multiple data files will have 1 mdf and X ndf's
            $fileInfo["DataSize"] += $row.Size
            $fileInfo[$i]["NewPhysicalName"] = $restoreDataPath + $restoreDbName + $i.toString() + [System.IO.Path]::GetExtension($row.PhysicalName)
        }
        ElseIf($row.Type.ToString() -eq "L")
        {  #while there should only be 1 ldf this should handle whatever is provided.
            $fileInfo["LogSize"] += $row.Size
            $fileInfo[$i]["NewPhysicalName"] =$restoreLogPath + $restoreDbName + $i.toString() + [System.IO.Path]::GetExtension($row.PhysicalName)
        }
        $i++
    }
    $fileInfo["Count"] = $i;
    Return $fileInfo
}

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

    #$restore.sqlrestore($sqlsvr)
    $restore.sqlrestore($ServerName)
    # After restore verify the database is valid.
    $errorCount = 0 -as [int]

    $dbccQuery = "DBCC CHECKDB ([" + $restoreDbName.ToString() + "]) WITH TABLERESULTS, NO_INFOMSGS"
    $dbccResults = Invoke-SQLCmd -ServerInstance $ServerName -Query $dbccQuery -querytimeout ([int]::MaxValue)
    
    if ($dbccResults.length -ne 0){
        $errorCount = 1 #DBCC output something ergo, errors.
    }
    Start-Sleep -Milliseconds 1000 #delay a second to give sql server time to clean up.


    $db = New-Object -TypeName Microsoft.SqlServer.Management.Smo.Database -argumentlist $ServerName, $restoreDbName  
    $db = $sqlsvr.Databases[$restoreDbName]  
    $db.Drop()
    return $errorCount
}

function checkFreeSpace($logPath, $dataPath, $logUsed, $dataUsed, $buffer){
    #Cleanup should remove the db, but there could be other activity on the drive so recalc each time
    $FSO = New-Object -Com Scripting.FileSystemObject
    $logInfo = $FSO.getdrive($(Split-Path $logPath -Qualifier))
    $logSpace = $logInfo.AvailableSpace - $buffer

    $dataInfo = $FSO.getdrive($(Split-Path $dataPath -Qualifier)) 
    $dataSpace = $dataInfo.AvailableSpace  - $buffer 
    if($logInfo.DriveLetter -eq $dataInfo.DriveLetter){
        $dataSpace = $dataSpace - $LogUsed - $dataUsed - $buffer
    } else {
        $logSpace = $logSpace - $logUsed - $buffer
        $dataSpace = $dataSpace - $dataUsed - $buffer
    }
    $ret = 0 
    if($logSpace -gt 0 -And $dataSpace -gt 0){
        $ret = 1
    }
    return $ret
}

function LoggingInit($serverName, $loggingDB, $sourceServerName){
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

function LoggingDB($serverName, $loggingDB, $logID, $dbName, $errors, $errorMsg, $logStart, $logEnd){
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
    $result = $SqlCmd.ExecuteNonQuery();
    $SqlConnection.Close();
}

function LoggingFinalize($serverName, $loggingDB, $logID){
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
    $result = $SqlCmd.ExecuteNonQuery();
    $returnID = $SqlCmd.Parameters["@id"].Value;
    $SqlConnection.Close();
}

#Set Enviroment Variables

$ServerName = "(local)"
$friendlyServerName = "Test Server" #useful if multiple servers exist
$restoreDbName = "checkdb"
$restoreDataPath = "Y:\"
$restoreLogPath = "Y:\"
$backupSource =  "Y:\Weekly Full\"
$loggingDB = "loggingDBCC"
$freeSpaceBuffer = 200*1024*1024 #200 MB to spare


##
# Check out the enviroment - make sure the server can be connected and the folders exist.
# try catch doesn't play well with SMO objects

$sqlsvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
if ($sqlsvr.Edition  -eq $null) 
{
    write-output "Could not connect to server"
    exit
} 
elseif( $sqlsvr.Databases[$restoreDbName] -ne $null) 
{
    write-output "Target database [$restoreDbName] for testing restore already exists.  Exiting"
    exit
} 
elseif($sqlsvr.Databases[$loggingDB] -eq $null) 
{
    write-output "Target logging database does not exists.  Exiting"
    exit
} elseif((Test-Path $restoreDataPath) -eq $false)
{
    write-output "Restore Datafile destination does not exists.  Exiting"
    exit
} elseif((Test-Path $restoreLogPath) –eq $false)
{
    write-output "Restore Logfile destination does not exists.  Exiting"
    exit
} elseif((Test-Path $backupSource) –eq $false)
{
    write-output "Backup Root folder does not exists.  Exiting"
    exit
}

$logID = LoggingInit $serverName $loggingDB $friendlyServerName
$folders = Get-ChildItem -Recurse $backupSource | ?{ $_.PSIsContainer }
foreach ($subfolder in $folders){
    try{
        #Get the most recent backup file from each subfolder
        $f = $backupSource + $subfolder
        $file = Get-ChildItem $f -Filter "*.bak" -recurse | sort LastWriteTime | select -last 1
        
        if($file -eq $null){
            continue
        }
        $backupFile = $f + "\" + $file

        $logStart = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        #read file to get size information and other properties of the backup.
        $backupInfo = getBackupInfo $ServerName $restoreDbName $backupFile $restoreDataPath $restoreLogPath
        
        $oldDbName = $backupInfo[0].LogicalName
        $oldDbName
        $free = checkFreeSpace $restoreLogPath $restoreDataPath $backupInfo.LogSize $backupInfo.DataSize $freeSpaceBuffer
        if ($free -eq 1){
            $errorCount = restoreVerifyDropDatabase $ServerName $oldDbName $backupFile $backupInfo $restoreDbName
            $errorCount -as [int]
            $logEnd = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")

            if($errorCount -eq 0){
                LoggingDB $serverName $loggingDB $logID $oldDbName 0 "" $logStart $logEnd
            } else {
                LoggingDB $serverName $loggingDB $logID $oldDbName 1 "" $logStart $logEnd
            }

        } else {
            $logEnd = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
            LoggingDB $serverName $loggingDB $logID $oldDbName 1 "Not enough Free Space" $logStart $logEnd
        }
    } catch {
        "in catch for some reason"
        $error[0] | format-list -force
        $logEnd = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        $err = $error[0] | format-list -force

        LoggingDB $serverName $loggingDB $logID $oldDbName 1 "unknown error" $logStart $logEnd
    }
}
LoggingFinalize $serverName $loggingDB $logID

#$error[0] | format-list -force

<#
TODO
    Error handling, try catch blocks
    Review PS best practices around formatting, naming etc.
#>
