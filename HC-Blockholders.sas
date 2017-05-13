libname mylib 'H:\Block\';	

%let wrds=wrds.wharton.upenn.edu 4016;
options comamid=TCP remote=WRDS;
signon username=_prompt_;
options nodate nocenter nonumber ps=max ls=72 fullstimer;
title ' ';
rsubmit;

libname temp '/sastemp7/';

* Upload the SDC data;
proc upload data=mylib.sdcdata out=temp.sdcdata;
run;

data temp.sdcdata (drop=anndate);
	set temp.sdcdata;
	anndate_sdc = anndate;
	format anndate_sdc YYMMDDn8.;
run;

* Keep ftv_match in work directory on WRDS servers;
proc upload data=mylib.ftv_crsp_match out=ftv_match;
run;

* Get list of target and acquirer CUSIPs;
data temp.tcusip(keep = cusip6);
	set temp.sdcdata;
	cusip6 = tcusip;
run;

proc sort data=temp.tcusip nodupkey;
	by tcusip;
run;

data temp.acusip(keep = acusip);
	set temp.sdcdata;
	cusip6 = acusip;
run;

proc sort data=temp.acusip nodupkey;
	by acusip;
run;

* Extract the relevant holdings data - shrout2 is total shares outstanding in thousands;
* SHARES, SOLE, SHARED, NO, and CHANGE is in units so we convert to thousands;
* SHARES is sum of SOLE, SHARED, and NO;
* No is for non-voting shares so we use only the sum of sole voting shares and shared voting shares;

*extract the s34 data that does not have missing values for shrout2 and is more recent than 2001 - this is mainly for speed;

data holdings(keep=fdate mgrno mgrname typecode rdate prdate cusip shares sole shared no change stkname prc shrout2);
        set tfn.s34;
        if year(fdate) > 2001 and not missing(shrout2) and shrout2 > 0 then output;
run;

* Update the holdings data with cusip6 numbers and percentage voting - we may want to check if data is set to zero or missing;
data holdings(keep=fdate mgrno mgrname typecode rdate prdate cusip shares sole shared no change stkname prc cusip6 shrout2 pctvotingsharesheld);
        set holdings;
        cusip6 = substr(cusip,1,6);
		shares = shares / 1000;
		sole = sole / 1000;
		shared = shared /1000;
		change = change / 1000;
		votingshares = sole + shared;
		no = no / 1000;
		pctvotingsharesheld = votingshares / shrout2;
        if pctvotingsharesheld >= .05 and not missing(pctvotingsharesheld) then output;
run;

* Now, get blockholder data for targets;
proc sql;
        create table temp.targetblockholderdata as select
        a.*, b.*
        from temp.tcusip as a inner join holdings as b
        on a.cusip6=b.cusip6
		order by cusip6, fdate;
quit;

proc sort data=temp.targetblockholderdata;
	by cusip6 fdate;
run;

* And blockholder data for acquirers;
proc sql;
        create table temp.acquirerblockholderdata as select
        a.*, b.*
        from temp.acusip as a inner join holdings as b
        on a.cusip6=b.cusip6
		order by cusip6, fdate;
quit;

proc sort data=temp.acquirerblockholderdata;
	by cusip6 fdate;
run;

* Merge the blockholder data with the SDC data for targets;
proc sql;
      create table temp.matchedtargetblockholders as select
      a.*, b.*
      from temp.targetblockholderdata as a inner join temp.sdcdata as b
      on a.cusip6 = b.tcusip
      order by tcusip;
  quit;

proc sort data=temp.matchedtargetblockholders;
	by tcusip fdate;
run;

* Merge the blockholder data with the SDC data for acquirers;
proc sql;
      create table temp.matchedacquirerblockholders as select
      a.*, b.*
      from temp.acquirerblockholderdata as a inner join temp.sdcdata as b
      on a.cusip6 = b.acusip
      order by acusip;
quit;

proc sort data=temp.matchedacquirerblockholders;
	by acusip fdate;
run;

* Keep all observations with target blockholder data reporting dates in the prior four quarters of the announcement date;
data temp.matchedtargetblockholders1;
	set temp.matchedtargetblockholders;
	*will return negative if announcement date is before reporting date;
	datediff = INTCK('MONTH',fdate,anndate_sdc); 
	*filter applied to keep only previous 4 quarters of data;
	if datediff > 0 and datediff < 13 and not missing(datediff) then output;
run;

proc sort data=temp.matchedtargetblockholders1;
	by tcusip anndate_sdc mgrno fdate;
run;

* Keep all observations with acquirer blockholder data reporting dates in the prior four quarters of the announcement date;
data temp.matchedacquirerblockholders1;
	set temp.matchedacquirerblockholders;
	*will return negative if announcement date is before reporting date;
	datediff = INTCK('MONTH',fdate,anndate_sdc); 
	*filter applied to keep only previous 4 quarters of data;
	if datediff > 0 and datediff < 13 and not missing(datediff) then output;
run;

proc sort data=temp.matchedacquirerblockholders1;
	by acusip anndate_sdc mgrno fdate;
run;

* Now finalising the data for target blockholders...;
proc sql;
	create table temp.targetblockholders as select
	a.*, b.*
	from temp.matchedtargetblockholders1 as a left outer join ftv_match as b
	on a.anndate_sdc=b.anndate_sdc and a.tcusip = b.targetcusip ;
quit;

proc sort data=temp.targetblockholders;
	by anndate_sdc dealnumber fdate pctvotingsharesheld;
run;

* proc download data=temp.targetblockholders out=mylib.targetblockholders;
* run;

* Now finalising the data for acquirer blockholders...;
proc sql;
	create table temp.acquirerblockholders as select
	a.*, b.*
	from temp.matchedacquirerblockholders1 as a left outer join ftv_match as b
	on a.anndate_sdc=b.anndate_sdc and a.acusip = b.acquirercusip ;
quit;

proc sort data=temp.acquirerblockholders;
	by anndate_sdc dealnumber fdate pctvotingsharesheld;
run;

* proc download data=temp.acquirerblockholders out=mylib.acquirerblockholders;
* run;

* Data cleaning;
* Acquirer stuff;
data temp.zblockacq;
	set temp.acquirerblockholders;
	if status = "Completed";
	if acusip ^= tcusip;
run;

proc sort data = temp.zblockacq;
	by tcusip;
quit;

proc sort data = temp.zblockacq nodupkey out = temp.zblockacqlist;
	by mgrno tcusip;
quit;

data temp.zblockacqlist;
	set temp.zblockacqlist;
	keep acusip tcusip mgrno;
run;

proc sort data = temp.zblockacqlist;
	by mgrno tcusip acusip;
quit;

proc means data = temp.zblockacqlist noprint;
	var mgrno;
	by mgrno;
	output out = temp.temporaryacq n = mgrdealcount;
quit;

data temp.temporaryacq;
	set temp.temporaryacq;
	drop _type_ _freq_;
run;

proc sort data = temp.zblockacq;
	by mgrno;
quit;

data temp.zblockacq2;
	merge temp.zblockacq temp.temporaryacq;
	by mgrno;
run;

proc sort data = temp.zblockacq2;
	by mgrno tcusip acusip;
run;

* Target stuff;
data temp.zblocktar;
	set temp.targetblockholders;
	if status = "Completed";
	if acusip ^= tcusip;
run;

proc sort data = temp.zblocktar;
	by tcusip;
quit;

proc sort data = temp.zblocktar nodupkey out = temp.zblocktarlist;
	by mgrno tcusip;
quit;

data temp.zblocktarlist;
	set temp.zblocktarlist;
	keep acusip tcusip mgrno;
run;

proc sort data = temp.zblocktarlist;
	by mgrno tcusip acusip;
quit;

proc means data = temp.zblocktarlist noprint;
	var mgrno;
	by mgrno;
	output out = temp.temporarytar n = mgrdealcount;
quit;

data temp.temporarytar;
	set temp.temporarytar;
	drop _type_ _freq_;
run;

proc sort data = temp.zblocktar;
	by mgrno;
quit;

data temp.zblocktar2;
	merge temp.zblocktar temp.temporarytar;
	by mgrno;
run;

proc sort data = temp.zblocktar2;
	by mgrno tcusip acusip;
run;

* Generate list of deals where blockholder is on both sides;
data temp.zblocktarlist2;
	set temp.zblocktarlist;
	drop acusip;
run;

* List of both;
data temp.zblockbothlist;
	merge temp.zblocktarlist2 (in = intar) temp.zblockacqlist (in = inacq);
	by mgrno tcusip;
	if intar;
	if inacq;
	bothsides = 1;
run;

* Final data?;
data temp.zblockacq3;
	merge temp.zblockacq2 temp.zblockbothlist;
	by mgrno tcusip acusip;
run;

proc sort data = temp.zblockacq3 nodupkey;
	by mgrno tcusip acusip fdate;
quit;

data temp.zblocktar3;
	merge temp.zblocktar2 temp.zblockbothlist;
	by mgrno tcusip acusip;
run;

proc sort data = temp.zblocktar3 nodupkey;
	by mgrno tcusip acusip fdate;
quit;
