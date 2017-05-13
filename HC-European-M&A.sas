* Macro.;
%macro RenameList(dsn=,pre=,suf=,except=);
  %local i dsid varlist numvars numexcept;

  /* Make sure that the dataset exists */
  %if %sysfunc(exist(&dsn)) eq 0 %then
    %do;
     %put WARNING: (RenameList) &DSN does not exist.;
     %let varlist = ;
     %goto Quit;
    %end;

  /* Make sure that a prefix or suffix was passed.              */
  %if &Pre eq %str() and &Suf eq %str() %then
    %do;
     %put WARNING: (RenameList) A prefix and/or suffix value must be
passed.;
     %let varlist = ;
     %goto Quit;
    %end;

  /* If it does exist, open the dataset.                        */
  %let dsid=%sysfunc(open(&dsn,i));

  /* Look through the variables in the dataset and create a macro*/
  /* variable called VarList.  It will contain rename pairs for  */
  /* all the variables in the dataset, adding the passed prefix  */
  /* and/or suffix.                                              */
  %let NumVars = %sysfunc(attrn(&dsid,nvars));
  %do i = 1 %to &NumVars;
    %let varlist =  &varlist%str(
)%sysfunc(varname(&dsid,&i))=&Pre.%sysfunc(varname(&dsid,&i))&Suf;
  %end;

  /* Close the dataset.                                          */
  %let rc = %sysfunc(close(&dsid));

  /* Clean out the variables that are not to be renamed.         */
  %if %length(&except) gt 0 %then
    %do;

  /* Upper case everything so there is no inadvertant misses.    */
      %let VarList = %upcase(&VarList);
      %let Except = %upcase(&Except);
      %let Pre = %upcase(&Pre);
      %let Suf = %upcase(&Suf);

  /* We want to count how many hits we get (Found) and how many     */
  /* Except= variables there were (NumExcept).  When we're done, if */
  /* these two variables don't have the same value, then we know a  */
  /* variable that doesn't exist in the dataset was passed in the   */
  /* Except= list.                                                  */
      %let Found = 0;
      %let NumExcept = %eval((%length(&except) -
%length(%sysfunc(compress(&except,%str( )))) + 1);


  /* Loop through all the Except= variables.  Set up a string (remove) */
  /* that contains the "oldname=newname" value for that variable and   */
  /* convert it to a blank space with the TRANWRD function.  Check the */
  /* value of VarList before and after the remove operation.  If it    */
  /* changed, increment the Found counter.                             */


      %do i = 1 %to &NumExcept;
          %let remove = %scan(&except,&i,%str(
))=&pre.%scan(&except,&i,%str( ))&Suf;
        %let PreRemove = &VarList;
          %let varlist = %sysfunc(tranwrd(&varlist,&remove,%str()));
        %if %quote(&PreRemove) ne %quote(&VarList) %then %let Found =
%eval(&Found + 1);
        %end;


  /* If excepted variables are passed and do not exist in the dataset, */
  /* write a warning to the log and reset the varlist to null.         */


        %if &Found ne &NumExcept %then
          %do;
            %put WARNING: (RenameList) Except list contains variables not in
the dataset.;
            %put WARNING: (RenameList) Rename aborted.;
            %let varlist = ;
            %goto Quit;
          %end;
    %end;
  %if &varlist ne %str( ) %then %let VarList = RENAME=(&VarList);
  %Quit:


  /* Write out the variable name list.                           */


  &varlist
%mend;
* End Macro.;

* Start next macro.;
%macro cars(filter);
proc sort data = mergeddatapv;
	by &filter SEDOL TIME;
quit;

proc means data = mergeddatapv noprint;
	where TIME >= &estimationend. and TIME <= %eval(&daysmonth. * &estimationstartmonths.);
	by &filter SEDOL;
	var capmar;
	output out = varars var = varcapmar;
quit;

proc means data = varars noprint;
	var varcapmar;
	by &filter;
	output out = totalvarars n = nfirms sum = totalvarcapmar;
quit;

data stdars;
	set totalvarars;
	by &filter;
	drop _TYPE_ _FREQ_ totalvarcapmar nfirms marketvar0capmar;
	marketvar0capmar = totalvarcapmar / (nfirms ** 2);
	capmar0 = sqrt(marketvar0capmar);
	call symput ('stdcapmar', capmar0);
	capmar1  = sqrt(3 * marketvar0capmar);
	capmar2  = sqrt(5 * marketvar0capmar);
	capmar5  = sqrt(11 * marketvar0capmar);
	capmar10 = sqrt(21 * marketvar0capmar);
run;

proc sort data = mergeddatapv;
	by &filter SEDOL TIME;
quit;

proc means data = mergeddatapv noprint;
	where TIME = 0;
	by &filter SEDOL;
	var capmar;
	output out = zero sum = sum0capmar;
quit;

proc means data = mergeddatapv noprint;
	where TIME >= -1 and TIME <= 1;
	by &filter SEDOL;
	var capmar;
	output out = one sum = sum1capmar;
quit;

proc means data = mergeddatapv noprint;
	where TIME >= -2 and TIME <= 2;
	by &filter SEDOL;
	var capmar;
	output out = two sum = sum2capmar;
quit;

proc means data = mergeddatapv noprint;
	where TIME >= -5 and TIME <= 5;
	by &filter SEDOL;
	var capmar;
	output out = five sum = sum5capmar;
quit;

proc means data = mergeddatapv noprint;
	where TIME >= -10 and TIME <= 10;
	by &filter SEDOL;
	var capmar;
	output out = ten sum = sum10capmar;
quit;

/* Combining the cumulative abnormal return numbers. */
data combinedars;
	merge zero one two five ten;
	by &filter SEDOL;
	drop _TYPE_ _FREQ_;
run;

/* Summing up the cumulative abnormal return numbers. */
proc means data = combinedars noprint;
	by &filter;
	vars sum0capmar sum1capmar sum2capmar sum5capmar sum10capmar;
	output out = totalars n = nfirms sum = totalsum0capmar totalsum1capmar totalsum2capmar totalsum5capmar totalsum10capmar;
quit;

/* Calculating the average cumulative abnormal return numbers. */
data meanars;
	set totalars;
	drop _TYPE_ _FREQ_ nfirms totalsum0capmar totalsum1capmar totalsum2capmar totalsum5capmar totalsum10capmar;
	capmar0 = totalsum0capmar / nfirms;
	capmar1 = totalsum1capmar / nfirms;
	capmar2 = totalsum2capmar / nfirms;
	capmar5 = totalsum5capmar / nfirms;
	capmar10 = totalsum10capmar / nfirms;
run;

/* Formatting to obtain table of results... */
proc transpose data = meanars out = meanmerge;
quit;

proc transpose data = stdars out = stdmerge;
quit;

proc sort data = stdmerge;
	by _NAME_;
quit;

proc sort data = meanmerge;
	by _NAME_;
quit;

/* Calculating t-statistics and p-values... */
data third.&filter.carsstatistics;
	merge meanmerge (in = in1 rename = (col1 = meancarpp col2 = meancarpv)) stdmerge (in = in2 rename = (col1 = stdcarpp col2 = stdcarpv));
	by _NAME_;
	rename _NAME_ = measure;
	if in1 and in2;
	tstatisticpp = meancarpp / stdcarpp;
	pvaluepp = (1 - probt(abs(tstatisticpp), %eval((&daysmonth. * &estimationstartmonths.) - &estimationend. - 1)));
	tstatisticpv = meancarpv / stdcarpv;
	pvaluepv = (1 - probt(abs(tstatisticpv), %eval((&daysmonth. * &estimationstartmonths.) - &estimationend. - 1)));
run;
%mend;
*End macro.;


*** CALCULATING CARS.;
* Shaping acquirer return data from wide to long.;
proc transpose data = third.publicprivate out = publicprivate;
	by DATE;
quit;

proc sort data = publicprivate;
	by _NAME_ DATE;
quit;

proc transpose data = third.publicpublic out = publicpublic;
	by DATE;
quit;

proc sort data = publicpublic;
	by _NAME_ DATE;
quit;

* Merging all acquirer data.;
data acquirerreturns;
	set publicprivate (in = PV) publicpublic (in = PP);
	if PV = 1 then
		do;
			DEALTYPE = "PV";
		end;
	if PP = 1 then
		do;
			DEALTYPE = "PP";
		end;
	rename _NAME_ = SEDOL;
	label _NAME_ = ' ';
	rename COL1 = RET;
	* Ensuring the leading zeros are restored.;
	if substr(_NAME_, 1, 1) = "v" and length(_NAME_) = 8 then _NAME_ = substr(_NAME_, 2, 7);
	if substr(_NAME_, 1, 1) = "v" and length(_NAME_) = 7 then _NAME_ = cats("0", substr(_NAME_, 2, 6));
	if substr(_NAME_, 1, 1) = "v" and length(_NAME_) = 6 then _NAME_ = cats("00", substr(_NAME_, 2, 5));
	if substr(_NAME_, 1, 1) = "v" and length(_NAME_) = 5 then _NAME_ = cats("000", substr(_NAME_, 2, 4));
	* Removing missing data STK that cannot seem to be extracted from Datastream.;
	if _NAME_ ne "v_NA";	
run;

proc sort data = acquirerreturns;
	by SEDOL DATE;
quit;

* Create list of deals.;
data publicpublicdeals;
	set third.publicpublicdeals;
	DEALNO = 0;
run;

data deals;
	set publicpublicdeals third.publicprivatedeals;
	drop NAME CUSIP TICKER;
	rename DATE = DEALDATE;
run;

proc sort data = deals;
	by SEDOL DEALDATE;
quit;

data carcalculations;
	merge acquirerreturns (in = stockdata) deals (in = deals);
	by SEDOL;
	if stockdata = deals = 1;
	* Since DEALDATE seems to be on a Sunday...;
	if SEDOL = "B8KF9B4" then DEALDATE = mdy(5, 5, 2008);
run;

* Creating time counter.;
data carcalculations;
	set carcalculations;
	count + 1;
	by SEDOL;
	if first.SEDOL then count = 1;
run;

data timezero;
	set carcalculations;
	if DEALDATE = DATE;
	keep SEDOL count;
	rename COUNT = TIMEZERO;
run;

data time;
	merge carcalculations timezero;
	by SEDOL;
run;

data timeline;
	set time;
	TIME = COUNT - TIMEZERO;
	drop COUNT TIMEZERO;
	if TIME >= -300;
	if TIME <= 300;
	NATION = upcase(NATION);
	rename NATION = COUNTRY;	
run;

* Now add country market returns.;
proc sort data = timeline;
	by DATE COUNTRY;
quit;

proc sort data = third.countryindices;
	by DATE COUNTRY;
quit;

data addingcountry;
	merge timeline (in = needed) third.countryindices;
	by DATE COUNTRY;
	if needed;
run;

proc sort data = addingcountry out = carbase;
	by SEDOL DATE;
quit;

* Adding stay on indicators.;
proc sort data = third.sample out = stayonsample nodupkey;
	by ACQSDCSEDOL TYPE;
quit;

data stayondata;
	set stayonsample;
	keep ACQSDCSEDOL v0MSTAY
		 v0MSTAY2_5 v0MSTAY5 v0MSTAY10
		 v6MSTAY2_5 v6MSTAY5 v6MSTAY10
		 v12MSTAY2_5 v12MSTAY5 v12MSTAY10
		 v24MSTAY2_5 v24MSTAY5 v24MSTAY10;
	if TYPE = "AQ";
	rename ACQSDCSEDOL = SEDOL;
run;

data third.carbase;
	merge carbase stayondata;
	by SEDOL;
	v0MSTAYABOVE2_5   = v0MSTAY2_5  + v0MSTAY5  + v0MSTAY10;
	v0MSTAYABOVE5     = v0MSTAY5  + v0MSTAY10;
	v6MSTAYABOVE2_5   = v6MSTAY2_5  + v6MSTAY5  + v6MSTAY10;
	v6MSTAYABOVE5     = v6MSTAY5  + v6MSTAY10;
	v12MSTAYABOVE2_5  = v12MSTAY2_5 + v12MSTAY5 + v12MSTAY10;
	v12MSTAYABOVE5    = v12MSTAY5 + v12MSTAY10;
	v24MSTAYABOVE2_5  = v24MSTAY2_5 + v24MSTAY5 + v24MSTAY10;
	v24MSTAYABOVE5    = v24MSTAY5 + v24MSTAY10;
	v0MSTAYE          = (v0MSTAY          > 0);
	v0MSTAYABOVE2_5E  = (v0MSTAY2_5       > 0);
	v0MSTAYABOVE5E    = (v0MSTAY5         > 0);
	v0MSTAYABOVE10E   = (v0MSTAY10        > 0);
	v6MSTAYABOVE2_5E  = (v6MSTAY2_5       > 0);
	v6MSTAYABOVE5E    = (v6MSTAY5         > 0);
	v6MSTAYABOVE10E   = (v6MSTAY10        > 0);
	v12MSTAYABOVE2_5E = (v12MSTAY2_5      > 0);
	v12MSTAYABOVE5E   = (v12MSTAY5        > 0);
	v12MSTAYABOVE10E  = (v12MSTAY10       > 0);
	v24MSTAYABOVE2_5E = (v24MSTAY2_5      > 0);
	v24MSTAYABOVE5E   = (v24MSTAY5        > 0);
	v24MSTAYABOVE10E  = (v24MSTAY10       > 0);
run;

* Actual calculation of CARs using the entire sample.;
* Use this to get the Public Target CARs.;
* Define macro variables for ease of future modification.;
%let daysmonth = 21;
%let estimationstartmonths = -3;
%let estimationend = -250;
%let datafile = third.carbase;

proc reg data = &datafile.
	outest = capmreturns
	(rename = (intercept = alpha MARKETRETURN = beta) keep = SEDOL intercept MARKETRETURN _type_ _rmse_)
	outseb
	noprint;
	by SEDOL;
	where TIME >= &estimationend. and TIME <= %eval(&daysmonth. * &estimationstartmonths.);
	model RET = MARKETRETURN;
quit;

/* Splitting output into two to re-merge, since the estimates and the variances are on seperate lines. */
data capmestimates capmstd;
	set capmreturns;
	if _TYPE_ = "PARMS" then output capmestimates;
	if _TYPE_ = "SEB" then output capmstd;
run;

data capmstd;
	set capmstd;
	rename beta = betastd;
	drop _TYPE_ _RMSE_ alpha;
run;

/* Merging the two seperate datasets. */
data capmreturns;
	merge capmestimates capmstd;
	by SEDOL;
	drop _TYPE_;
	label beta = ' ';
	label betastd = ' ';
run;

/* Merging all estimates with original data. */
data mergeddata;
	merge &datafile. (in = in1)
		  capmreturns (in = in2);
	by SEDOL;
	if in1 = in2 = 1;
run;

/* Calculating abnormal returns. */
data mergeddata;
	set mergeddata;
	label MARKETRETURN = ' ';
	label RET = ' ';
	capmar = RET - alpha - (beta * MARKETRETURN);
run;

/* Calculating the appropriate variances for the t-statistic. */
proc sort data = mergeddata;
	by DEALTYPE SEDOL TIME;
quit;

/* Calculating the variances of the daily abnormal return of each firm  for the estimation period. */
proc means data = mergeddata noprint;
	where TIME >= &estimationend. and TIME <= %eval(&daysmonth. * &estimationstartmonths.);
	by DEALTYPE SEDOL;
	var capmar;
	output out = varars var = varcapmar;
quit;

/* Summing up the variances of the daily abnormal return of each firm for the estimation period. */
proc means data = varars noprint;
	var varcapmar;
	by DEALTYPE;
	output out = totalvarars n = nfirms sum = totalvarcapmar;
quit;

/* Calculating the standard errors of the daily abnormal return (market wide) for the estimation period. */
data stdars;
	set totalvarars;
	by DEALTYPE;
	drop _TYPE_ _FREQ_ totalvarcapmar nfirms marketvar0capmar;
	marketvar0capmar = totalvarcapmar / (nfirms ** 2);
	capmar0 = sqrt(marketvar0capmar);
	call symput ('stdcapmar', capmar0);
	capmar1  = sqrt(3 * marketvar0capmar);
	capmar2  = sqrt(5 * marketvar0capmar);
	capmar5  = sqrt(11 * marketvar0capmar);
	capmar10 = sqrt(21 * marketvar0capmar);
run;

/* Now calculating the cumulative abnormal returns for day 0. */
proc sort data = mergeddata;
	by DEALTYPE SEDOL TIME;
quit;

proc means data = mergeddata noprint;
	where TIME = 0;
	by DEALTYPE SEDOL;
	var capmar;
	output out = zero sum = sum0capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-1, +1). */
proc means data = mergeddata noprint;
	where TIME >= -1 and TIME <= 1;
	by DEALTYPE SEDOL;
	var capmar;
	output out = one sum = sum1capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-2, +2). */
proc means data = mergeddata noprint;
	where TIME >= -2 and TIME <= 2;
	by DEALTYPE SEDOL;
	var capmar;
	output out = two sum = sum2capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-5, +5). */
proc means data = mergeddata noprint;
	where TIME >= -5 and TIME <= 5;
	by DEALTYPE SEDOL;
	var capmar;
	output out = five sum = sum5capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-10, +10). */
proc means data = mergeddata noprint;
	where TIME >= -10 and TIME <= 10;
	by DEALTYPE SEDOL;
	var capmar;
	output out = ten sum = sum10capmar;
quit;

/* Combining the cumulative abnormal return numbers. */
data combinedars;
	merge zero one two five ten;
	by DEALTYPE SEDOL;
	drop _TYPE_ _FREQ_;
run;

/* Output CARs with original data. */
data mergeddatabase;
	set mergeddata;
	drop RET MARKETRETURN _RMSE_ BETASTD;
run;

proc sort data = mergeddatabase;
	by DEALTYPE SEDOL;
quit;

data publicprivatecars;
	merge mergeddatabase combinedars;
	by DEALTYPE SEDOL;
	if TIME = 0;
	if DEALNO > 0;
	drop TIME ALPHA BETA DATE CAPMAR;
run;

data third.publicprivatecars;
	merge publicprivatecars (in = existing) deals;
	by SEDOL;
	if existing;
run;

/* Summing up the cumulative abnormal return numbers. */
proc means data = combinedars noprint;
	by DEALTYPE;
	vars sum0capmar sum1capmar sum2capmar sum5capmar sum10capmar;
	output out = totalars n = nfirms sum = totalsum0capmar totalsum1capmar totalsum2capmar totalsum5capmar totalsum10capmar;
quit;

/* Calculating the average cumulative abnormal return numbers. */
data meanars;
	set totalars;
	drop _TYPE_ _FREQ_ nfirms totalsum0capmar totalsum1capmar totalsum2capmar totalsum5capmar totalsum10capmar;
	capmar0 = totalsum0capmar / nfirms;
	capmar1 = totalsum1capmar / nfirms;
	capmar2 = totalsum2capmar / nfirms;
	capmar5 = totalsum5capmar / nfirms;
	capmar10 = totalsum10capmar / nfirms;
run;

/* Formatting to obtain table of results... */
proc transpose data = meanars out = meanmerge;
quit;

proc transpose data = stdars out = stdmerge;
quit;

proc sort data = stdmerge;
	by _NAME_;
quit;

proc sort data = meanmerge;
	by _NAME_;
quit;

/* Calculating t-statistics and p-values... */
data third.carsstatistics;
	merge meanmerge (in = in1 rename = (col1 = meancarpp col2 = meancarpv)) stdmerge (in = in2 rename = (col1 = stdcarpp col2 = stdcarpv));
	by _NAME_;
	rename _NAME_ = measure;
	if in1 and in2;
	tstatisticpp = meancarpp / stdcarpp;
	pvaluepp = (1 - probt(abs(tstatisticpp), %eval((&daysmonth. * &estimationstartmonths.) - &estimationend. - 1)));
	tstatisticpv = meancarpv / stdcarpv;
	pvaluepv = (1 - probt(abs(tstatisticpv), %eval((&daysmonth. * &estimationstartmonths.) - &estimationend. - 1)));
run;











/* Now if we want the CARs for stay and no stay and so on?;
/* First set the base dataset. */
/*
data mergeddatapv;
	set mergeddata;
	if DEALNO > 0;
	keep DATE SEDOL RET DEALTYPE COUNTRY DEALDATE DEALNO TIME MARKETRETURN
		 v0MSTAYABOVE2_5E v0MSTAYABOVE5E v0MSTAYABOVE10E
		 v6MSTAYABOVE2_5E v6MSTAYABOVE5E v6MSTAYABOVE10E
		 v12MSTAYABOVE2_5E v12MSTAYABOVE5E v12MSTAYABOVE10E
		 v24MSTAYABOVE2_5E v24MSTAYABOVE5E v24MSTAYABOVE10E
		 _RMSE_ ALPHA BETA BETASTD CAPMAR;
run;
*/

/*
%cars(v0MSTAYABOVE2_5E)
%cars(v0MSTAYABOVE5E)
%cars(v0MSTAYABOVE10E)
%cars(v6MSTAYABOVE2_5E)
%cars(v6MSTAYABOVE5E)
%cars(v6MSTAYABOVE10E)
%cars(v12MSTAYABOVE2_5E)
%cars(v12MSTAYABOVE5E)
%cars(v12MSTAYABOVE10E)
%cars(v24MSTAYABOVE2_5E)
%cars(v24MSTAYABOVE5E)
%cars(v24MSTAYABOVE10E)
*/




*** REGRESSIONS.;
* Get sample data - basically the SEDOL for later matching.;
proc sort data = third.sample out = sedols nodupkey;
	by DEALNO;
quit;

data sedols;
	set sedols;
	keep DEALNO ACQSDCSEDOL;
	rename ACQSDCSEDOL = SEDOL;
run;

* Merge balance sheet and profit & loss...;
proc sort data = third.balancesheet;
	by DEALNO TYPE DATADATE;
quit;

proc sort data = third.profitandloss;
	by DEALNO TYPE DATADATE;
quit;

data combinedaccounting;
	merge third.profitandloss (in = prnlo) third.balancesheet (in = balsht);
	by DEALNO TYPE DATADATE;
	drop NAME DATASTANDARD DATATYPE;
run;

* Get SEDOLS.;
data combined;
	merge combinedaccounting sedols;
	by DEALNO;
run;

* Split data into acquirers and targets.;
data acqfinancials;
	set combined;
	if TYPE = "AQ";
run;

data tarfinancials;
	set combined;
	if TYPE = "TG";
run;

* Batch rename of variables.;
data third.acquirerfinancials;
	set acqfinancials(%RenameList(dsn = acqfinancials, pre = ACQ, except = SEDOL DEALNO));
run;

data third.targetfinancials;
	set tarfinancials(%RenameList(dsn = tarfinancials, pre = TAR, except = SEDOL DEALNO));
run;

* Now, to match the acquirer financial data to the public-private CARs file...;
proc sql;
	create table matchacq as
	select a.*, b.*, (a.DEALDATE - b.ACQDATADATE) as DATEDIFFERENCE
	from third.publicprivatecars as a left join third.acquirerfinancials as b
	on a.SEDOL = b.SEDOL AND a.DEALDATE - b.ACQDATADATE > 0 AND b.ACQDATALENGTH > 8;
quit;

proc sort data = matchacq;
	by DEALNO ACQDATADATE DATEDIFFERENCE;
quit;

data matchacq;
	set matchacq;
	by DEALNO;
	if last.DEALNO;
	rename DATEDIFFERENCE = ACQDATEDIFFERENCE;
run;


* Now, to match the target financial data to the public-private CARs file...;
proc sql;
	create table matchacqtar as
	select a.*, b.*, (a.DEALDATE - b.TARDATADATE) as DATEDIFFERENCE
	from matchacq as a left join third.targetfinancials as b
	on a.SEDOL = b.SEDOL AND a.DEALDATE - b.TARDATADATE > 0 AND b.TARDATALENGTH > 8;;
quit;

proc sort data = matchacqtar;
	by DEALNO TARDATADATE DATEDIFFERENCE;
quit;

data lastbeforefinalcars;
	set matchacqtar;
	by DEALNO;
	if last.DEALNO;
	rename DATEDIFFERENCE = TARDATEDIFFERENCE;
	*if ACQDATEDIFFERENCE le 365;
	*if DATEDIFFERENCE le 365;
run;

proc sort data = lastbeforefinalcars;
	by DEALNO;
quit;

data third.finalcars;
	set lastbeforefinalcars;
	drop NATION TARNATION;
	TARCOUNTRY = upcase(TARNATION);
	UKDUMMY  = (COUNTRY = "UNITED KINGDOM");
	RELATIVEVALUE = DEALVALUE / ACQMV4WEEK;
	FACCIOPCT = DEALVALUE / (DEALVALUE + ACQMV4WEEK);
	FACCIOPCT5 = (FACCIOPCT ge 0.05);
	FACCIOPCT10 = (FACCIOPCT ge 0.10);
	ACQTARTOTALASSETDIFF = ACQTOTALASSET - TARTOTALASSET;
	ACQTARTOTALASSETDIFFTA = ACQTARTOTALASSETDIFF / ACQTOTALASSET;
	ACQNITA = ACQNI / ACQTOTALASSET;
	TARNITA = TARNI / TARTOTALASSET;
	ACQEBITDATA = ACQEBITDA / ACQTOTALASSET;
	TAREBITDATA = TAREBITDA / TARTOTALASSET;
	ACQPLEBITTA = ACQPLEBIT / ACQTOTALASSET;
	TARPLEBITTA = TARPLEBIT / TARTOTALASSET;
	TARACQPLEEBITTADIFF = TARPLEBITTA - ACQPLEBITTA;
	TARACQROADIFF = (TARNI / TARTOTALASSET) - (ACQNI / ACQTOTALASSET);
	if ACQDATADATE ne .;
run;

/* ACQNI, ACQEBITDA, ACQTOTALASSETS, ACQCASHANDEQ, ACQSALES */

* Table of summary statistics.;
proc means data = third.finalcars noprint;
	var DEALVALUE ACQMV4WEEK FRACISSUED RELATIVEVALUE ACQTARTOTALASSETDIFFTA TARACQROADIFF;
	output out = third.financialstats n = mean = std = p25 = median = p75 = min = max = /autoname;
quit;

* Create list of private target acquisitions.;
data sedolslist;
	set third.finalcars;
	keep sedol dealno;
run;

*** 01: Ownership Statistics.;
* Acquirers.;
proc sort data = sedolslist out = dealnolist;
	by dealno;
quit;

data third.acquirerownershipfinal;
	merge third.acquirerownership dealnolist (in = final);
	by dealno;
	if final;
run;

proc means data = third.acquirerownershipfinal noprint;
	by dealno file;
	var holding;
	output out = acquirerownershipstats n = mean = sum = std = min = max = p25 = median = p75 = /autoname;
quit;

data third.acquirerownershipstats;
	set acquirerownershipstats;
	drop _TYPE_ _FREQ_;
	if HOLDING_Sum le 100;
	TYPE = "AQ";
run;

*************************************** NEED TO RUN AVERAGES.;

* Targets.;
data third.targetownershipfinal;
	merge third.targetownership dealnolist (in = final);
	by dealno;
	if final;
run;

proc means data = third.targetownershipfinal noprint;
	by file;
	var holding;
	output out = targetownershipstats n = mean = sum = std = min = max = p25 = median = p75 = /autoname;
quit;

data third.targetownershipstats;
	set targetownershipstats;
	drop _TYPE_ _FREQ_;
	if HOLDING_Sum le 100;
	TYPE = "TG";
run;

*************************************** NEED TO RUN AVERAGES.;











* Actual calculation of CARs using the entire sample.;
proc sort data = sedolslist out = sedolssorted;
	by sedol;
quit;

data workingworking;
	merge third.carbase sedolssorted (in = final);
	by sedol;
	if final;
run;

* Define macro variables for ease of future modification.;
%let daysmonth = 21;
%let estimationstartmonths = -3;
%let estimationend = -250;
%let datafile = workingworking;

proc reg data = &datafile.
	outest = capmreturns
	(rename = (intercept = alpha MARKETRETURN = beta) keep = SEDOL intercept MARKETRETURN _type_ _rmse_)
	outseb
	noprint;
	by SEDOL;
	where TIME >= &estimationend. and TIME <= %eval(&daysmonth. * &estimationstartmonths.);
	model RET = MARKETRETURN;
quit;

/* Splitting output into two to re-merge, since the estimates and the variances are on seperate lines. */
data capmestimates capmstd;
	set capmreturns;
	if _TYPE_ = "PARMS" then output capmestimates;
	if _TYPE_ = "SEB" then output capmstd;
run;

data capmstd;
	set capmstd;
	rename beta = betastd;
	drop _TYPE_ _RMSE_ alpha;
run;

/* Merging the two seperate datasets. */
data capmreturns;
	merge capmestimates capmstd;
	by SEDOL;
	drop _TYPE_;
	label beta = ' ';
	label betastd = ' ';
run;

/* Merging all estimates with original data. */
data mergeddata;
	merge &datafile. (in = in1)
		  capmreturns (in = in2);
	by SEDOL;
	if in1 = in2 = 1;
run;

/* Calculating abnormal returns. */
data mergeddata;
	set mergeddata;
	label MARKETRETURN = ' ';
	label RET = ' ';
	capmar = RET - alpha - (beta * MARKETRETURN);
run;

/* Calculating the appropriate variances for the t-statistic. */
proc sort data = mergeddata;
	by DEALTYPE SEDOL TIME;
quit;

/* Calculating the variances of the daily abnormal return of each firm  for the estimation period. */
proc means data = mergeddata noprint;
	where TIME >= &estimationend. and TIME <= %eval(&daysmonth. * &estimationstartmonths.);
	by DEALTYPE SEDOL;
	var capmar;
	output out = varars var = varcapmar;
quit;

/* Summing up the variances of the daily abnormal return of each firm for the estimation period. */
proc means data = varars noprint;
	var varcapmar;
	by DEALTYPE;
	output out = totalvarars n = nfirms sum = totalvarcapmar;
quit;

/* Calculating the standard errors of the daily abnormal return (market wide) for the estimation period. */
data stdars;
	set totalvarars;
	by DEALTYPE;
	drop _TYPE_ _FREQ_ totalvarcapmar nfirms marketvar0capmar;
	marketvar0capmar = totalvarcapmar / (nfirms ** 2);
	capmar0 = sqrt(marketvar0capmar);
	call symput ('stdcapmar', capmar0);
	capmar1  = sqrt(3 * marketvar0capmar);
	capmar2  = sqrt(5 * marketvar0capmar);
	capmar5  = sqrt(11 * marketvar0capmar);
	capmar10 = sqrt(21 * marketvar0capmar);
run;

/* Now calculating the cumulative abnormal returns for day 0. */
proc sort data = mergeddata;
	by DEALTYPE SEDOL TIME;
quit;

proc means data = mergeddata noprint;
	where TIME = 0;
	by DEALTYPE SEDOL;
	var capmar;
	output out = zero sum = sum0capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-1, +1). */
proc means data = mergeddata noprint;
	where TIME >= -1 and TIME <= 1;
	by DEALTYPE SEDOL;
	var capmar;
	output out = one sum = sum1capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-2, +2). */
proc means data = mergeddata noprint;
	where TIME >= -2 and TIME <= 2;
	by DEALTYPE SEDOL;
	var capmar;
	output out = two sum = sum2capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-5, +5). */
proc means data = mergeddata noprint;
	where TIME >= -5 and TIME <= 5;
	by DEALTYPE SEDOL;
	var capmar;
	output out = five sum = sum5capmar;
quit;

/* Now calculating the cumulative abnormal returns for (-10, +10). */
proc means data = mergeddata noprint;
	where TIME >= -10 and TIME <= 10;
	by DEALTYPE SEDOL;
	var capmar;
	output out = ten sum = sum10capmar;
quit;

/* Combining the cumulative abnormal return numbers. */
data combinedars;
	merge zero one two five ten;
	by DEALTYPE SEDOL;
	drop _TYPE_ _FREQ_;
run;

/* Output CARs with original data. */
data mergeddatabase;
	set mergeddata;
	drop RET MARKETRETURN _RMSE_ BETASTD;
run;

proc sort data = mergeddatabase;
	by DEALTYPE SEDOL;
quit;

data publicprivatecars;
	merge mergeddatabase combinedars;
	by DEALTYPE SEDOL;
	if TIME = 0;
	if DEALNO > 0;
	drop TIME ALPHA BETA DATE CAPMAR;
run;

data third.publicprivatecars;
	merge publicprivatecars (in = existing) deals;
	by SEDOL;
	if existing;
run;

/* Summing up the cumulative abnormal return numbers. */
proc means data = combinedars noprint;
	by DEALTYPE;
	vars sum0capmar sum1capmar sum2capmar sum5capmar sum10capmar;
	output out = totalars n = nfirms sum = totalsum0capmar totalsum1capmar totalsum2capmar totalsum5capmar totalsum10capmar;
quit;

/* Calculating the average cumulative abnormal return numbers. */
data meanars;
	set totalars;
	drop _TYPE_ _FREQ_ nfirms totalsum0capmar totalsum1capmar totalsum2capmar totalsum5capmar totalsum10capmar;
	capmar0 = totalsum0capmar / nfirms;
	capmar1 = totalsum1capmar / nfirms;
	capmar2 = totalsum2capmar / nfirms;
	capmar5 = totalsum5capmar / nfirms;
	capmar10 = totalsum10capmar / nfirms;
run;

/* Formatting to obtain table of results... */
proc transpose data = meanars out = meanmerge;
quit;

proc transpose data = stdars out = stdmerge;
quit;

proc sort data = stdmerge;
	by _NAME_;
quit;

proc sort data = meanmerge;
	by _NAME_;
quit;

/* Calculating t-statistics and p-values... */
data third.carsstatisticspv;
	merge meanmerge (in = in1 rename = (col1 = meancarpv)) stdmerge (in = in2 rename = (col1 = stdcarpv));
	by _NAME_;
	rename _NAME_ = measure;
	if in1 and in2;
	tstatisticpv = meancarpv / stdcarpv;
	pvaluepv = (1 - probt(abs(tstatisticpv), %eval((&daysmonth. * &estimationstartmonths.) - &estimationend. - 1)));
run;

/* NUMBER OF STAY ONS ANY SIZE */
data samplesample;
	set third.sample;
	keep DEALNO ACQSDCSEDOL v0MSTAY v6MSTAY v12MSTAY v24MSTAY;
	rename ACQSDCSEDOL = SEDOL;
run;

proc sort data = samplesample nodupkey;
	by DEALNO SEDOL;
quit;

data third.zzz;
	merge dealnolist (in = in) samplesample;
	by DEALNO;
	if in;
run;
