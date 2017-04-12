<#
Setting a few constants and the script will loop throuh the subfolders looking for the most recent .bak file.
Once found it will read in the logical file name and physical location.  
It then generates new physical location for all files based on the database name and restore path.
Using that information it will restore the database moving/relocating  to the new locations
Then it will run DBCC CheckDB to detect any corruption.
Finally it will drop the new database and start the process over for any other backup files.
#>
cls
import-module "SQLPS" -DisableNameChecking

function getBackupInfo($ServerName, $databaseName, $backupName, $newDataPath, $newLogPath) {
    #Restore database to get current logical and physical file info.  Create new physical path based on path variables
    $sqlsvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
    $restore = new-object Microsoft.SqlServer.Management.Smo.Restore
    $devicetype = [Microsoft.SqlServer.Management.Smo.DeviceType]::File
    $restoredevice = New-Object -TypeName Microsoft.SQLServer.Management.Smo.BackupDeviceItem($backupname,$devicetype)
    $restore.Devices.add($restoredevice)

    $fileData = $restore.ReadFileList($sqlsvr)
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
            $fileInfo[$i]["NewPhysicalName"] = $newDataPath + $databaseName + $i.toString() + [System.IO.Path]::GetExtension($row.PhysicalName)
        }
        ElseIf($row.Type.ToString() -eq "L")
        {  #while there should only be 1 ldf this should handle whatever is provided.
            $fileInfo["LogSize"] += $row.Size
            $fileInfo[$i]["NewPhysicalName"] =$newLogPath + $databaseName + $i.toString() + [System.IO.Path]::GetExtension($row.PhysicalName)
        }
        $i++
    }
    $fileInfo["Count"] = $i;
    Return $fileInfo
}

function restoreVerifyDropDatabase($ServerName, $databaseName, $backupname, $fileInfo){
    $sqlsvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
    $restore = new-object Microsoft.SqlServer.Management.Smo.Restore
    $devicetype = [Microsoft.SqlServer.Management.Smo.DeviceType]::File

    $restoredevice = New-Object -TypeName Microsoft.SQLServer.Management.Smo.BackupDeviceItem($backupname,$devicetype)
    $restore.Database = $databaseName
    $restore.ReplaceDatabase = $True
    $restore.NoRecovery = $false
    $restore.FileNumber = 1
    $restore.Devices.add($restoredevice)
    for($i=0; $i -lt $fileInfo.Count; $i++)
    {
        $relocate = New-Object Microsoft.SqlServer.Management.Smo.RelocateFile ($fileInfo[$i].LogicalName,$fileInfo[$i].NewPhysicalName)
        $restore.RelocateFiles.add($relocate)
    }

    $restore.sqlrestore($sqlsvr)
    # After restore verify the database is valid.

    $dbccQuery = "DBCC CHECKDB ([" + $databaseName.ToString() + "]) WITH TABLERESULTS, NO_INFOMSGS"
    $dbccResults = Invoke-SQLCmd -ServerInstance $ServerName -Query $dbccQuery

    if ($dbccResults.length -eq 0){
        Write-Host " We have only information messages, do nothing" + $backupname
    }else{
        Write-Host "We have error records, do something." + $backupname
    }
    $db = New-Object -TypeName Microsoft.SqlServer.Management.Smo.Database -argumentlist $ServerName, $databaseName  
    $db = $sqlsvr.Databases[$databaseName]  
    $db.Drop()  
}
function checkIfDatabaseExists($ServerName, $databaseName){
    $sqlsvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
    $exists = 0
    if ( $null -ne $sqlsvr.Databases[$databaseName] ) { $exists = 1 } else { $exists = 0 }  
    return $exists
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

####
# Adding 3 functions to make calls to Stored Procs to begin DB logging, add each DB as it runs then finalize the main record
# These aren't tested as the table + SPs have to be written.
# Research if there is a way to abstract this into a generic function to reduce repetition.
####
function LoggingInit($loggingServer, $loggingDB, $serverVerified){
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Server=" + $loggingServer + ";Database=" + $loggingDB + ";Integrated Security=True"
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $loggingDB + ".dbo.LogStart"
    $SqlCmd.Connection = $SqlConnection
    $SqlCmd.CommandType = [System.Data.CommandType]'StoredProcedure';

    $inParameter = new-object System.Data.SqlClient.SqlParameter;
    $inParameter.ParameterName = "@serverName";
    $inParameter.Direction = [System.Data.ParameterDirection]'Input';
    $inParameter.DbType = [System.Data.DbType]'String';
    $inParameter.Size = 255;
    $inParameter.Value = $serverVerified;
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

function LoggingDB($loggingServer, $loggingDB, $serverVerified, $logID, $db){
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Server=" + $loggingServer + ";Database=" + $loggingDB + ";Integrated Security=True"
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $loggingDB + ".dbo.LogAddDB"
    $SqlCmd.Connection = $SqlConnection
    $SqlCmd.CommandType = [System.Data.CommandType]'StoredProcedure';

    $inParameter = new-object System.Data.SqlClient.SqlParameter;
    $inParameter.ParameterName = "@LogID";
    $inParameter.Direction = [System.Data.ParameterDirection]'Input';
    $inParameter.DbType = [System.Data.DbType]'Int32';
    $inParameter.Value = $logID;
    $SqlCmd.Parameters.Add($inParameter) >> $null;

    $inParameter2 = new-object System.Data.SqlClient.SqlParameter;
    $inParameter2.ParameterName = "@LoggingID";
    $inParameter2.Direction = [System.Data.ParameterDirection]'Input';
    $inParameter2.DbType = [System.Data.DbType]'string';
    $inParameter2.size = 255;
    $inParameter2.Value = $db;
    $SqlCmd.Parameters.Add($inParameter2) >> $null;

    $SqlConnection.Open();
    $result = $SqlCmd.ExecuteNonQuery();
    $returnID = $SqlCmd.Parameters["@id"].Value;
    $SqlConnection.Close();
}

function LoggingFinalize($loggingServer, $loggingDB, $serverVerified, $logID){
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = "Server=" + $loggingServer + ";Database=" + $loggingDB + ";Integrated Security=True"
    $SqlCmd = New-Object System.Data.SqlClient.SqlCommand
    $SqlCmd.CommandText = $loggingDB + ".dbo.LogFinalize"
    $SqlCmd.Connection = $SqlConnection
    $SqlCmd.CommandType = [System.Data.CommandType]'StoredProcedure';

    $inParameter = new-object System.Data.SqlClient.SqlParameter;
    $inParameter.ParameterName = "@LogID";
    $inParameter.Direction = [System.Data.ParameterDirection]'Input';
    $inParameter.DbType = [System.Data.DbType]'Int32';
    $inParameter.Value = $logID;
    $SqlCmd.Parameters.Add($inParameter) >> $null;

    $SqlConnection.Open();
    $result = $SqlCmd.ExecuteNonQuery();
    $returnID = $SqlCmd.Parameters["@id"].Value;
    $SqlConnection.Close();
}


$ServerName = "WIN-3J398F4GRU4"
$databaseName = "checkdb"
$newDataPath = "B:\Backups\testing\"
$newLogPath = "B:\Backups\testing\"
$backupRoot =  "B:\Backups\testing\"


$freeSpaceBuffer = 200*1024*1024 #200 MB to spare
$exists = checkIfDatabaseExists $ServerName $databaseName
if ($exists -eq 0) {
    $folders = Get-ChildItem -Recurse $backupRoot | ?{ $_.PSIsContainer }
    foreach ($subfolder in $folders){
        $f = $backupRoot + $subfolder
        $file = gci $f -Filter "*.bak" -recurse | sort LastWriteTime | select -last 1
        $backupname = $f + "\" + $file

        $backupInfo = getBackupInfo $ServerName $databaseName $backupname $newDataPath $newLogPath

        $free = checkFreeSpace $newLogPath $newDataPath $fileInfo.LogSize $fileInfo.DataSize $freeSpaceBuffer
        if ($free -eq 1){
            restoreVerifyDropDatabase $ServerName $databaseName $backupname $backupInfo
        } else {
            "error not enough free space"
        }
    }
} else {
"fail, db already exists"
}
#$error[0] | format-list -force

<#
TODO
     Log information - create a table and save results
    Error handling, try catch blocks
    Review PS best practices around formatting, naming etc.
#>
