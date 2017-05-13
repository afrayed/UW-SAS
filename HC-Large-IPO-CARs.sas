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
	from ipos.ipos, ipos.history
	where ipos.cusip = substr(history.ncusip,1,6);
quit;

*** Delete the repeated values...;
*** NOTE: Again, very rough...;
proc sort data = temporary nodupkey; by cusip permno; quit;
proc sort data = temporary; by issuedate cusip; quit;

*** Here create 2 digit NAICS, and delete financials and those with no exchange according to SDC...;
data iposall;
	set temporary;
	if floor(sic / 1000) ^= 6;
	if exchange ^= "";
	sic2 = floor(sic / 100);
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


*** At this point what we have is a list of IPO firms from 1990 onwards;
*** Next we need to find the firms that will be part of the industry portfolio based on SIC2;
*** Get this list from the history file;
data history;
	set ipos.history;
	sic2 = floor(hsiccd / 100);
run;

*** Now create the portfolios.;
*** Not sure how to ensure date matching, but...;
proc sql;
	create table work.basisall as
	select *
	from work.lefthandsidefirms, work.history
	where lefthandsidefirms.base_sic2 = history.sic2
	/* and lefthandsidefirms.base_windowminus120date le history.cal_date_stop */
	and lefthandsidefirms.base_windowminus120date ge history.cal_date_start
	and lefthandsidefirms.base_permno ne history.permno
	order by lefthandsidefirms.base_issuedate, history.permno;
quit;

*** Just in case, proc sort nodupkey here (Not recommended, very rough);
*** in order to remove possible dupes.;
proc sort data = basisall out = basisall nodupkey;
	by base_permno permno;
quit;

*** Next, get the (-5, -1) returns for the portfolio firms.;
%event( basisall, portfirmrets, base_issuedate, 5, -1, vwretd, ex_vars=prc shares, dxmz=dx, need_all=y );

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
	create table work.preportpostipo as
	select basisall.*, portfirmcars.portfirmcars
	from work.basisall, work.portfirmcars
	where basisall.base_issuedate = portfirmcars.base_issuedate
	and basisall.permno = portfirmcars.permno
	order by basisall.base_permno, basisall.base_issuer, basisall.base_issuedate, basisall.sic2;
quit;

*** Next, average the portfolio firm abnormal returns;
proc means data = work.preportpostipo noprint;
	by base_permno base_issuer base_issuedate sic2;
	var portfirmcars;
	output out = ipos.portcars mean  = portcars;
quit;



*** So next is to figure out the post IPO abnormal returns of the IPO firm;
data ipofirms;
	set work.basisall;
	keep base_permno base_issuedate base_issuer sic2;
run;

data ipofirms;
	set ipofirms;
	rename base_permno = permno;
run;

proc sort data = ipofirms nodupkey;
	by permno base_issuer base_issuedate sic2;
quit;

*** Next, get the (0, 5) returns for the portfolio firms.;
%event( ipofirms, ipofirmrets, base_issuedate, 0, 4, vwretd, ex_vars=prc shares, dxmz=dx, need_all=y );

data ipos.ipofirmrets;
	set ipofirmrets;
run;

*** Now to add the Fama-French factors for the issue date;
proc sql;
	create table ipofirmretsandfactors as
	select ipofirmrets.*, factors_daily.*
	from work.ipofirmrets, ff.factors_daily
	where ipofirmrets.caldt = factors_daily.date;
quit;

*** Getting the abnormal return (here, a simple return - market return);
data ipofirmretsandfactors;
	set ipofirmretsandfactors;
	cars = ret - mktrf - rf;
	rename permno = base_permno;
run;

proc sort data = ipofirmretsandfactors;
	by base_permno base_issuedate;
quit;

*** Summing up the abnormal returns;
proc means data = ipofirmretsandfactors noprint;
	by base_permno base_issuedate;
	var cars;
	output out = ipos.ipocars sum = portfirmcars;
quit;

