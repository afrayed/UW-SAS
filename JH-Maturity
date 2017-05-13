data history;
	set harford.history;
	cnum = substr(ncusip, 1, 6);
run;

proc sql;
	create table match as select distinct
		a.*, b.permco
	from harford.maturity as a left join history as b
	on (a.cusip = b.cnum)
		and (b.startdt <= a.issuedate or b.startdt = .B)
		and (a.issuedate <= b.lastdt or b.lastdt = .E);
quit;

proc sql;
	create table harford.matched as select distinct
		a.*, b.gvkey
	from match as a left join harford.ccmxpf_linktable (where=(linktype in ('LU', 'LC', 'LD', 'LF', 'LN', 'LO', 'LS', 'LX'))) as b
	on (a.permco = b.lpermco)  
		and (b.linkdt <= a.issuedate or b.linkdt = .B)
		and (a.issuedate <= b.linkenddt or b.linkenddt = .E);
quit;

/* Clean data... */
data harford.maturityclean;
	set harford.matched;
	tltm = (maturity - issuedate) / 365;
	if issuer ^= "Fannie Mae";
	if issuer ^= "Freddie Mac";
	if issuer ^= "Federal Farm Cr Banks Funding";
	year = year(issuedate);
	if tltm > 5 then fiveyear = 1;
		else fiveyear = 0;
	if tltm > 7 then sevenyear = 1;
		else sevenyear = 0;
	if tltm ^= .;
run;

/* Generate the five year sums. */
proc sort data = harford.maturityclean;
	by gvkey year fiveyear;
quit;

proc means data = harford.maturityclean noprint;
	by gvkey year fiveyear;
	var principal;
	output out = fiveyear sum = totalfiveyear;
quit;

data belowfive abovefive;
	set fiveyear;
	if fiveyear = 1 then output abovefive;
	if fiveyear = 0 then output belowfive;
run;

data belowfive;
	set belowfive;
	drop _TYPE_ _FREQ_ fiveyear;
	rename totalfiveyear = belowfive;
run;

data abovefive;
	set abovefive;
	drop _TYPE_ _FREQ_ fiveyear;
	rename totalfiveyear = abovefive;
run;

/* Generate the seven year sums. */
proc sort data = harford.maturityclean;
	by gvkey year sevenyear;
quit;

proc means data = harford.maturityclean noprint;
	by gvkey year sevenyear;
	var principal;
	output out = sevenyear sum = totalsevenyear;
quit;

data belowseven aboveseven;
	set sevenyear;
	if sevenyear = 1 then output aboveseven;
	if sevenyear = 0 then output belowseven;
run;

data belowseven;
	set belowseven;
	drop _TYPE_ _FREQ_ sevenyear;
	rename totalsevenyear = belowseven;
run;

data aboveseven;
	set aboveseven;
	drop _TYPE_ _FREQ_ sevenyear;
	rename totalsevenyear = aboveseven;
run;

/* Merge in... */
data merged;
	merge abovefive belowfive aboveseven belowseven;
	by gvkey year;
	if abovefive  = . then abovefive  = 0;
	if belowfive  = . then belowfive  = 0;
	if aboveseven = . then aboveseven = 0;
	if belowseven = . then belowseven = 0;
run;

data companyid;
	set harford.maturityclean;
	keep issuer ticker cusip permco gvkey year;
run;

proc sort data = companyid nodupkey;
	by gvkey year;
quit;

data harford.merged;
	merge companyid merged;
	by gvkey year;
run;
