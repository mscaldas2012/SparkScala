Create table incidentPlus2(
   State char(2),
   patID varchar(18),
   serotypeSummary varchar(20),
   dtSpec datetime,
   tempSS varchar(20),
   firstDtSpec datetime,
   expectedSS varchar(20)
)


--Insert Test Data...
Insert into incidentPlus2(state,patID,serotypeSummary,dtSpec,expectedSS)
values('GA','P123','Typii','2018/03/05','Typii')
Insert into incidentPlus2(state,patID,serotypeSummary,dtSpec,expectedSS)
values('GA','P123',null,'2018/03/07','Typii')
Insert into incidentPlus2(state,patID,serotypeSummary,dtSpec,expectedSS)
values('GA','P123','SS2','2018/04/01','SS2')
Insert into incidentPlus2(state,patID,serotypeSummary,dtSpec,ExpectedSS)
values('GA','P123',null,'2018/03/04','Typii')
Insert into incidentPlus2(state,patID,serotypeSummary,dtSpec,ExpectedSS)
values('GA','P123',null,'2018/03/28','SS2')
Insert into incidentPlus2(state,patID,serotypeSummary,dtSpec,ExpectedSS)
values('GA','P456',null,'2018/03/12','null')


--View:
--Rulue: If serotype is null, find the record with closest dtSpec date and assign that serotype to it.
--  Use only matching records for state and patId.

Select o.state,o.patId,o.dtSpec, o.serotypeSummary, o.expectedSS,
    Case when serotypeSummary!=null then serotypeSummary
        else(select top 1 serotypeSummary
               From incidentPlus2 int
              Where o.state=int.state and o.patID=int.patID
                    and int.serotypeSummary is not null
              Order by ABS(DATEDIFF(d,o.dtSpec,int.dtSpec)))
    end as TEMPSS
From incidentPlus2 o

