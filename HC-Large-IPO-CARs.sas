*** Create libraries;
libname ipos '/data/users/harveyc/ipos';
libname ipostemp '/data/extra/data7/harveyc';

*** Including macros.;
%include "&_macros/event.sas";
%include "&_macros/find_crsp_day.sas";
%include "&_macros/utility.sas";

*** Macro for renaming;
%macro rename(lib,dsn,newname); 
proc contents data=&lib..&dsn noprint; run; 

proc sql noprint; 
	select nvar into :num_vars 
	from dictionary.tables 
	where libname="&LIB" and memname="&DSN";
	select distinct(name) into :var1-:var%trim(%left(&num_vars)) 
	from dictionary.columns 
	where libname="&LIB" and memname="&DSN"; 
quit; run; 

proc datasets library = &LIB noprint; 
	modify &DSN; 
	rename
		%do i = 1 %to &num_vars.; 
		&&var&i = &newname._&&var&i.
%end;; 
quit; run; 

proc contents data=&lib..&dsn. noprint; run; 
%mend rename; 
*** Macro end;








*** First take the IPOs file and merge with Lew's history file to get PERMNOs;
*** NOTE: Very rough matching based on CUSIP6, no real check here;
proc sql;
	create table temporary as
	select ipos.*, history.permno, history.ncusip
	from ipos.ipos, crsp.history
	where ipos.cusip = substr(history.ncusip,1,6);
quit;

*** Delete the repeated values...;
*** NOTE: Again, very rough...;
proc sort data = temporary nodupkey; by cusip permno; quit;
proc sort data = temporary; by issuedate cusip; quit;

*** Here create 3 digit NAICS, and delete financials and those with no exchange according to SDC...;
data iposall;
	set temporary;
	if substr(SIC,1,1) ^= 6;
	if exchange ^= "";
	NAICS3 = substr(NAICS,1,3);
	windowminus120date = issuedate;
	windowminus10date = issuedate;
run;

*** Running the find_crsp_day macros here for later use;
%crsp_day( iposall, iposall2, issuedate, dxmz = dx);
%crsp_day( iposall2, basisdates, windowminus120date, offset = -120, dxmz=dx);
%crsp_day( basisdates, ipos.basisdates, windowminus10date, offset = -10, dxmz=dx);

data lefthandsidefirms;
	set ipos.basisdates;
	if year(issuedate) => 1990;
run;

%rename(WORK, LEFTHANDSIDEFIRMS, BASE);

*** Now create the portfolios.;
proc sql;
	create table ipos.basisall as
	select *
	from work.lefthandsidefirms, ipos.basisdates
	where lefthandsidefirms.base_naics3 = basisdates.naics3
	and lefthandsidefirms.base_windowminus120date le basisdates.issuedate
	and lefthandsidefirms.base_windowminus10date ge basisdates.issuedate
	and lefthandsidefirms.base_permno ne basisdates.permno
	order by lefthandsidefirms.base_issuedate, lefthandsidefirms.base_issuer;
quit;

*** Get the (-5, -1) returns for the portfolio firms.;
%event( ipos.basisall, portfirmrets, base_issuedate, 5, -1, vwretd, ex_vars=prc shares, dxmz=dx, need_all=y );

data ipos.portfirmrets;
	set portfirmrets;
run;

*** Now to add the Fama-French factors for the issue date;
proc sql;
	create table portfirmretsandfactors as
	select portfirmrets.*, factors_daily.*
	from work.portfirmrets, ff.factors_daily
	where portfirmrets.caldt = factors_daily.date;
quit;

*** Getting the abnormal return (here, a simple return - market return);
data portfirmretsandfactors;
	set portfirmretsandfactors;
	cars = ret - mktrf - rf;
run;

proc sort data = portfirmretsandfactors;
	by permno base_issuedate;
quit;

*** Summing up the abnormal returns;
proc means data = portfirmretsandfactors noprint;
	by permno base_issuedate;
	var cars;
	output out = portfirmcars sum = portfirmcars;
quit;

*** So here is a file detailing from left to right (ostensibly)...;
*** LEFT HAND SIDE IPO FIRM, PORTFOLIO IPO FIRM, PORTFOLIO FIRM [-5, -1] CAR;
proc sql;
	create table ipos.preportpostipo as
	select basisall.*, portfirmcars.portfirmcars
	from ipos.basisall, work.portfirmcars
	where basisall.base_issuedate = portfirmcars.base_issuedate
	and basisall.permno = portfirmcars.permno;
quit;









*** So start the other long chain file here...;
*** First, the LHS firms.;
%_index(ipos.basisall, base_permno);

data longipos;
	set ipos.basisall;
	keep base_issuedate base_permno;
	by base_permno;
	if (first.base_permno) then output;
run;

data longipos;
	set longipos;
	rename base_permno = permno;
run;

*** Get the (+5, +120) returns for the LHS firms.;
%event( longipos, ipos.longiporets, base_issuedate, -5, 120, vwretd, ex_vars=prc shares, dxmz=dx, need_all=y );

data longiporets;
	set ipos.longiporets;
	rename permno = base_permno;
	rename caldt = portdate;
	rename ret = ipofirmret;
run;

data basisall;
	set ipos.basisall;
	keep base_issuedate base_permno permno;
run;

*** index by base_permno first.;

%_index(longiporets, base_permno);

data ipos.unique;
	set ipos.basisall;
	by base_permno;
		if (first.base_permno) then output;
run;

%num_obs(ipos.unique, num);

%macro runit;
	%do i = 1% to &num;
		data _null_;
			i = &i;
			set ipos.unique point = i;
			call symput('firmid', base_permno);
			stop;
		run;
		
		data foo1;
			set ipos.basisall (where = (base_permno = &firmid));
			keep base_permno base_issuedate permno;
		run;
		
		%event( foo1, foo2, base_issuedate, -5, 120, vwretd, ex_vars=prc shares, dxmz=dx, need_all=n );
		proc sort data = foo2; by permno caldt;	quit;
		
		proc sql;
			create table work.foo3 as
			select foo1.*, foo2.*
			from work.foo1, work.foo2
			where foo1.permno = foo2.permno
			and foo1.base_issuedate = foo2.base_issuedate
			order by foo2.caldt;
		quit;
		
		* getting sum.;
		data foo4(rename = (sum = sumret));
			sum = 0;
			do until (last.caldt);
				set foo3;
				by caldt;
				if (ret > .z) then sum = sum + ret;
			end;
		run;
		proc append base = all data = foo4; run;
	%end;
	data ipos.all;
		set all;
	run;
%mend;	
	
%runit;
	
	
proc sort data = ipos.longiporets; by permno base_issuedate caldt; quit;
proc sort data = ipos.all (keep = sumret base_issuedate base_permno caldt) out = tempall; by base_permno base_issuedate caldt; quit;
proc sort data = ipos.unique; by base_permno base_issuedate; quit;

data temp1;
	merge ipos.unique (in = inlhs) ipos.longiporets(rename = (permno = base_permno prc = base_prc ret = base_ret shares = base_shares) in = inrhs);
	by base_permno base_issuedate;
	if inlhs;
	if inrhs;
	drop vwretd;
run;

proc sort data = temp1; by base_permno base_issuedate caldt; quit;

data ipos.longchain;
	merge temp1 (in = inlhs) tempall (in = inrhs);
	by base_permno base_issuedate caldt;
	if inlhs;
	if inrhs;
	keep base_issuedate base_issuer base_sic base_naics base_cusip base_cusip9 base_princamt base_totalshares base_mkvmkcap base_permno base_ncusip
		 issuedate issuer sic naics cusip cusip9 princamt totalshares mkvmkcap permno
		 caldt base_prc base_ret base_shares sumret;
run;

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
