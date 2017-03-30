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
        Write-Host " We have only information messages, do nothing"
    }else{
        Write-Host "We have error records, do something."
    }
    $db = New-Object -TypeName Microsoft.SqlServer.Management.Smo.Database -argumentlist $ServerName, $databaseName  
    $db = $sqlsvr.Databases[$databaseName]  
    $db.Drop()  
}
function checkForDatabase($ServerName, $databaseName){
    $sqlsvr = new-object Microsoft.SqlServer.Management.Smo.Server($ServerName)
    $exists = 0
    if ( $null -ne $sqlsvr.Databases[$databaseName] ) { $exists = 1 } else { $exists = 0 }  
    return $exists
}
 
$ServerName = "WIN-3J398F4GRU4"
$databaseName = "dbCheck"
$newDataPath = "B:\Backups\testing\"
$newLogPath = "B:\Backups\testing\"
$backupRoot =  "B:\Backups\testing\"

$freeSpaceBuffer = 200*1024*1024 
$FSO = New-Object -Com Scripting.FileSystemObject
$logDriveAvailableSpace = $FSO.getdrive($(Split-Path $newLogPath -Qualifier)).AvailableSpace - $freeSpaceBuffer
$dataDriveAvailableSpace = $FSO.getdrive($(Split-Path $newDataPath -Qualifier)).AvailableSpace  - $freeSpaceBuffer 


$x =checkForDatabase $ServerName $databaseName
if ($x -eq 0) {

    $folders = Get-ChildItem -Recurse $backupRoot | ?{ $_.PSIsContainer }
    foreach ($subfolder in $folders){
        $f = $backupRoot + $subfolder
        $file = gci $f -Filter "*.bak" -recurse | sort LastWriteTime | select -last 1
        $backupname = $f + "\" + $file

        $backupInfo = getBackupInfo $ServerName $databaseName $backupname $newDataPath $newLogPath
        restoreVerifyDropDatabase $ServerName $databaseName $backupname $backupInfo
    }
} else {
"fail, db exists"
}

#$error[0] | format-list -force

<#
TODO:
    get available space per drive to verify enough free space + buffer exists
    Log information - create a table and save results
    Error handling, try catch blocks
    Review PS best practices around formatting, naming etc.
#>
