Param
(
    [string]$modelfile = "D:\Temp\testi\Model.bim",
    [string]$logfile = "D:\Temp\deploy.txt",
    [string]$scriptdir = "D:\Temp\tabularscript\",
    [string]$testsqlserver = "dwitvipusql16",
    [string]$prodserver = "dwitviputab16",
    [string]$prodsqlserver = "dwipvipusql16",
    [string]$exe = "C:\Program Files (x86)\Microsoft SQL Server\130\Tools\Binn\ManagementStudio\Microsoft.AnalysisServices.Deployment.exe",
    [string]$username = "USERNAME",
    [string]$password = "PASSWORD"
)

[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices")
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices.AdomdClient") | out-null
[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.VisualBasic') | Out-Null

if([bool]((Get-Content $modelfile) -as [xml]))
{
    $scriptfile = $scriptdir + "deploy.xmla"
    $arg2 = "/o:""" + $scriptfile + """"
    $arg3 = "/d"

    try
    {
        Start-Process -FilePath $exe -ArgumentList $modelfile, $arg2, $arg3 -Wait #-RedirectStandardOutput d:\temp\stdout.txt -RedirectStandardError d:\temp\stderr.txt

        (Get-Content $scriptfile) | Foreach-Object {$_ -replace $testsqlserver, $prodsqlserver} | Out-File $scriptfile

        $xml = [xml]((Get-Content $scriptfile -Encoding Unicode))
        $node = $xml.Alter.ObjectDefinition.Database.DataSources.ChildNodes
        foreach($datasource in $node)
        {
            $datasource.ImpersonationInfo.Account = $username
            $pw = $xml.CreateElement("Password", "http://schemas.microsoft.com/analysisservices/2003/engine")
            $pwtext = $xml.CreateTextNode($password)
            $datasource.ImpersonationInfo.AppendChild($pw)
            $pw.AppendChild($pwtext)
        }

        $serverName = “Data Source=” + $prodserver
        $serverAS = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection $serverName
        $serverAS.open()

        $query = $xml.OuterXml
        $cmd = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdCommand $query, $serverAS
        $cmd.ExecuteNonQuery()
    }
    catch
    {
        
    }
}
else
{
    $scriptfile = $scriptdir + "deploy.json"
    $arg2 = "/o:""" + $scriptfile + """"
    $arg3 = "/d"

    try
    {
        Start-Process -FilePath $exe -ArgumentList $modelfile, $arg2, $arg3 -Wait #-RedirectStandardOutput d:\temp\stdout.txt -RedirectStandardError d:\temp\stderr.txt

        (Get-Content $scriptfile) | Foreach-Object {$_ -replace $testsqlserver, $prodsqlserver} | Out-File $scriptfile

        $a = Get-Content $scriptfile | Out-String | ConvertFrom-Json
        $b = $a.createOrReplace
        $c = $b.database
        $d = $c.model
        $e = $d.dataSources
        $f = $e[0]

        $f.account = $username
        Add-Member -InputObject $f -MemberType NoteProperty -Name 'password' -Value $password

        $s2 = $a | ConvertTo-Json -Depth 64
        $s2 |out-file -filepath $scriptfile

        $serverName = “Data Source=” + $prodserver
        $serverAS = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection $serverName
        $serverAS.open()

        $query = $a | ConvertTo-Json -Depth 64
        $cmd = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdCommand $query, $serverAS
        $cmd.ExecuteNonQuery()
    }
    catch
    {
        
    }
}
