*** set libname.;
libname harford '/data/users/harveyc/Harford';
option ls = 125;

*** call macros.;
%include "&_macros/event.sas";
%include "&_macros/find_crsp_day.sas";
%include "&_macros/get_permnos.sas";

* remove check digit from cusip, and generate cusip list for the get_permnos macro.;
data wantedcusips;
	set harford.wcbargainsbuys;
	keep cusip;
	cusip = substr(cusip, 1, 8);
	format datadate date9.;
run;

* get permnos.;
%get_permnos(wantedcusips, wantedpermnos);

* now to clean up the original data to just what we need.;
data wantedcompanydata;
	set harford.wcbargainsbuys;
	keep gvkey datadate fyear cusip;
	cusip = substr(cusip, 1, 8);
run;

* define a new start date to begin in june of the next year.;
data wantedcompanydata;
	set wantedcompanydata;
	nextyear = year(datadate) + 1;
	startreturndate = mdy(6, 1, nextyear);
	format startreturndate date9.;
run;

* presorting before merge.;
proc sort data = wantedcompanydata;
	by cusip;
quit;

proc sort data = wantedpermnos;
	by cusip;
quit;

* merge cleaned data with permnos list.;
data wanteddata;
	merge wantedcompanydata (in = incompany) wantedpermnos (in = inpermnos);
	by cusip;
	if (incompany & inpermnos);
	drop nextyear;
run;

* get crsp dates for the forward returns.;
%crsp_day(wanteddata, wantedforward, startreturndate, dxmz = mz);

* get crsp dates for the data date for obtaining the beta for the past 60 months.;
%crsp_day(wantedforward, harford.wantedbase, datadate, dxmz = mz, orig_dt = originaldatadate);

* now for the one year returns.;
%event(harford.wantedbase, oneyear, startreturndate, 0, 11, vwretd, dxmz = mz, need_all = n);

data oneyear;
	set oneyear;
	lnvwretd = log(1 + vwretd);
	lnret    = log(1 + ret);
	drop vwretd ret;
run;

proc means data = oneyear noprint;
	by permno startreturndate;
	var lnvwretd lnret;
	output out = oneyeardata sum = sumlnvwretd sumlnret;
quit;

data harford.oneyeardata;
	set oneyeardata;
	oneyearreturn = exp(sumlnret) - 1;
	oneyearmarket = exp(sumlnvwretd) - 1;
	drop _type_ _freq_ sumlnvwretd sumlnret;
run;

* the two year returns...;
%event(harford.wantedbase, twoyear, startreturndate, 0, 23, vwretd, dxmz = mz, need_all = n);

data twoyear;
	set twoyear;
	lnvwretd = log(1 + vwretd);
	lnret    = log(1 + ret);
	drop vwretd ret;
run;

proc means data = twoyear noprint;
	by permno startreturndate;
	var lnvwretd lnret;
	output out = twoyeardata sum = sumlnvwretd sumlnret;
quit;

data harford.twoyeardata;
	set twoyeardata;
	twoyearreturn = exp(sumlnret) - 1;
	twoyearmarket = exp(sumlnvwretd) - 1;
	drop _type_ _freq_ sumlnvwretd sumlnret;
run;

* the three year returns...;
%event(harford.wantedbase, threeyear, startreturndate, 0, 35, vwretd, dxmz = mz, need_all = n);

data threeyear;
	set threeyear;
	lnvwretd = log(1 + vwretd);
	lnret    = log(1 + ret);
	drop vwretd ret;
run;

proc means data = threeyear noprint;
	by permno startreturndate;
	var lnvwretd lnret;
	output out = threeyeardata sum = sumlnvwretd sumlnret;
quit;

data harford.threeyeardata;
	set threeyeardata;
	threeyearreturn = exp(sumlnret) - 1;
	threeyearmarket = exp(sumlnvwretd) - 1;
	drop _type_ _freq_ sumlnvwretd sumlnret;
run;

* the four year returns...;
%event(harford.wantedbase, fouryear, startreturndate, 0, 47, vwretd, dxmz = mz, need_all = n);

data fouryear;
	set fouryear;
	lnvwretd = log(1 + vwretd);
	lnret    = log(1 + ret);
	drop vwretd ret;
run;

proc means data = fouryear noprint;
	by permno startreturndate;
	var lnvwretd lnret;
	output out = fouryeardata sum = sumlnvwretd sumlnret;
quit;

data harford.fouryeardata;
	set fouryeardata;
	fouryearreturn = exp(sumlnret) - 1;
	fouryearmarket = exp(sumlnvwretd) - 1;
	drop _type_ _freq_ sumlnvwretd sumlnret;
run;

* the five year returns...;
%event(harford.wantedbase, fiveyear, startreturndate, 0, 59, vwretd, dxmz = mz, need_all = n);

data fiveyear;
	set fiveyear;
	lnvwretd = log(1 + vwretd);
	lnret    = log(1 + ret);
	drop vwretd ret;
run;

proc means data = fiveyear noprint;
	by permno startreturndate;
	var lnvwretd lnret;
	output out = fiveyeardata sum = sumlnvwretd sumlnret;
quit;

data harford.fiveyeardata;
	set fiveyeardata;
	fiveyearreturn = exp(sumlnret) - 1;
	fiveyearmarket = exp(sumlnvwretd) - 1;
	drop _type_ _freq_ sumlnvwretd sumlnret;
run;

* now for the betas.;
%event(harford.wantedbase, betasdata, datadate, 60, -1, vwretd, dxmz = mz, need_all = n);

proc reg data = betasdata outest = betasregression noprint;
	model ret = vwretd;
	by permno datadate;
quit;

data harford.betas;
	set betasregression;
	keep permno datadate vwretd;
run;

* now to merge everything together...;
proc sort data = harford.wantedbase;
	by permno startreturndate;
quit;

data mergedreturns;
	merge harford.wantedbase (in = base) harford.oneyeardata (in = one) harford.twoyeardata (in = two) harford.threeyeardata (in = three) harford.fouryeardata (in = four) harford.fiveyeardata (in = five);
	by permno startreturndate;
	if (base & one & two & three & four & five);
run;

proc sort data = mergedreturns;
	by permno datadate;
quit;

data mergeddata;
	merge mergedreturns (in = returns) harford.betas (in = betas rename = (vwretd = beta));
	by permno datadate;
	if (returns & betas);
	format datadate date9.;
run;

data harford.mergeddata;
	retain  gvkey cusip permno
		originaldatadate fyear datadate startreturndate
		beta
		oneyearreturn oneyearmarket
		twoyearreturn twoyearmarket
		threeyearreturn threeyearmarket
		fouryearreturn fouryearmarket
		giveyearreturn fiveyearmarket;
	set mergeddata;
run;

proc datasets lib = harford;
	modify mergeddata;
	attrib _all_ label = ' ';
quit;
