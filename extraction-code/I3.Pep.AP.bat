SETLOCAL

REM move to batch directory
cd /d %~dp0

SET SMASH.DB=PatientSafety_Records
SET ZIP.EXE="C:\Program Files\7-Zip\7z.exe"

SET BLEED_CODES='76125','761D5','761D500','761D6','761J.','761J0','761J000','761J1','761J111','761Jy','761Jz','761Jz00','7627.','76270','7627000','76271','76272','7627200','7627y','7627y00','7627z','7627z00','14C1.','J1020','J102000','J11..','J110.','J1100','J1101','J110100','J110111','J1102','J110200','J1103','J1104','J110y','J110z','J110z00','J111.','J1110','J1111','J1112','J111200','J111211','J1113','J1114','J111y','J111z','J111z00','J112.','J112z','J113.','J113z','J11y.','J11y0','J11y1','J11y2','J11y3','J11y4','J11yy','J11yz','J11yz00','J11z.','J12..','J120.','J1200','J1201','J120100','J1202','J120200','J1203','J120300','J1204','J120y','J120z','J120z00','J121.','J1210','J121000','J1211','J121100','J121111','J1212','J121200','J121211','J1213','J1214','J121400','J121y','J121z','J121z00','J122.','J123.','J124.','J125.','J125z','J126.','J126z','J12y.','J12y0','J12y1','J12y100','J12y2','J12y200','J12y3','J12y4','J12yy','J12yz','J12z.','J13..','J130.','J1300','J1301','J130100','J1302','J130200','J1303','J1304','J130y','J130y00','J130z','J130z00','J131.','J1310','J1311','J1312','J1313','J1314','J131y','J131y00','J131z','J131z00','J13y.','J13y0','J13y1','J13y2','J13y3','J13y4','J13yy','J13yz','J13z.','J14..','J140.','J1400','J1401','J1402','J1403','J1404','J140y','J140z','J141.','J1410','J1411','J1412','J1413','J1414','J141y','J141z','J14y.','J14y0','J14y1','J14y2','J14y3','J14y4','J14yy','J14yz','J14z.','J1733','J173300','J17y7','J17y700','J17y8','J17y800','ZV127','ZV12711','ZV12712','ZV12C','ZV12C00'

REM Get GP and AP meds for all patients with a PEP/GiB
bcp "select s.PatID, EntryDate, ReadCode, CodeValue, s.CodeUnits from [%SMASH.DB%].[dbo].SIR_ALL_Records_No_Rubric s inner join (select PatID, min(EntryDate) as [date] from [%SMASH.DB%].[dbo].SIR_ALL_Records_Narrow where ReadCode in (%BLEED_CODES%) group by PatID) sub on sub.date < s.EntryDate and s.PatID = sub.PatID inner join [%SMASH.DB%].[dbo].drugCodes d on d.code = s.ReadCode where d.id in ('GAST1','PPI_COD','ASPIRINRX','CLOP_PRA_TIC') and CodeValue is not null and CodeValue > 0 order by sub.PatID, EntryDate" queryout I3\gast_and_ap_bleed_patients.txt -c -T  -b 10000000

REM goto med alg directory to process the data
cd medalgs
node index.js -a ..\I3\gast_and_ap_bleed_patients.txt
CALL npm run -s sort ..\I3\gast_and_ap_bleed_patients.txt.done > ..\I3\gast_and_ap_bleed_patients.txt.done.sorted

perl parse_drug_file.pl ..\I3\gast_and_ap_bleed_patients.txt.done.sorted

cd ..

REM now get bleed events
bcp "select s.PatID, EntryDate, 'BLEED' from [%SMASH.DB%].[dbo].SIR_ALL_Records_Narrow s inner join (select PatID, min(EntryDate) as [date] from [%SMASH.DB%].[dbo].SIR_ALL_Records_Narrow where ReadCode in (%BLEED_CODES%) group by PatID) sub on sub.date <= s.EntryDate and s.PatID = sub.PatID where ReadCode in (%BLEED_CODES%) or ReadCode like 'J68%%' group by s.PatID, EntryDate order by s.PatID, EntryDate" queryout I3\gast_and_ap_bleed_patients_bleed_events.txt -c -T  -b 10000000

REM now get died events
bcp "SELECT PatID, max(EntryDate) as [date], 'DIED' as [event] FROM [%SMASH.DB%].[dbo].SIR_ALL_Records_Narrow WHERE ReadCode IN (SELECT code FROM [%SMASH.DB%].[dbo].drugCodes WHERE id = 'Death') GROUP BY PatID UNION SELECT p.patid, CAST(year_of_death as varchar) + '-' + CAST(month_of_death as varchar) + '-28', 'DIED' FROM [%SMASH.DB%].[dbo].patients p INNER JOIN ( SELECT patid FROM [%SMASH.DB%].[dbo].patients WHERE dead = 1 EXCEPT SELECT PatID FROM [%SMASH.DB%].[dbo].SIR_ALL_Records_Narrow WHERE ReadCode IN (SELECT code FROM [%SMASH.DB%].[dbo].drugCodes WHERE id = 'Death') 	GROUP BY PatID ) sub on sub.patid = p.patid" queryout I3\gast_and_ap_bleed_patients_died_events.txt -c -T  -b 10000000

REM TIDY UP
del I3\gast_and_ap_bleed_patients.txt
del I3\gast_and_ap_bleed_patients.txt.done
del I3\gast_and_ap_bleed_patients.txt.done.sorted

move I3\gast_and_ap_bleed_patients.txt.done.sorted.processed I3\med-events.txt
copy I3\gast_and_ap_bleed_patients_bleed_events.txt + I3\gast_and_ap_bleed_patients_died_events.txt I3\other-events.txt

%ZIP.EXE% a I3.tar I3\med-events.txt I3\other-events.txt
%ZIP.EXE% a -tzip I3.tgz I3.tar

del I3.tar

pause