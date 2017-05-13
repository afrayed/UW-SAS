

%let wrds = wrds.wharton.upenn.edu 4016;
options comamid = TCP remote = wrds;
signon username = _prompt_;
rsubmit;

libname ibes '/wrds/ibes/sasdata';

/* EXTRACT SUMMARY STATISTICS DATA */
data summarybase;
	set ibes.statsum_epsus (keep = TICKER CUSIP OFTIC CNAME STATPERS FPI NUMEST MEDEST MEANEST FPEDATS ACTUAL ANNDATS_ACT);
	if FPI in (1 2 6 7 8 9);
	if month(STATPERS) = 10;
run;

/* PRELIMINARY SORTING */
proc sort data = summarybase;
	by TICKER STATPERS FPI;
quit;

/* EXTRACTING SHARES DATA */
data sharesbase;
	set ibes.actpsum_epsus (keep = TICKER STATPERS SHOUT PRICE);
	if month(STATPERS) = 10;
run;

/* PRELIMINARY SORTING */
proc sort data = sharesbase;
	by TICKER STATPERS;
quit;

/* MERGING THE BASE DATA */
data mergebase;
	merge summarybase (in = SUMMARY)
		  sharesbase (in = SHARES);
	by TICKER STATPERS;
	if SUMMARY and SHARES;
	drop SUMMARY SHARES;
	if ACTUAL ne .;
	if ANNDATS_ACT ne .;
run;

/* SPLITTING THE DATA INTO QUARTERLY AND YEARLY DATA */
data yearlybase biyearlybase quarterlybase;
	set mergebase;
	if NUMEST > 1;
	if FPI = 1 then output yearlybase;
		else if FPI = 2 then output biyearlybase;
		else if FPI in (6 7 8 9) then output quarterlybase;
run;

/* CREATING THE QUAD-QUARTERLY MEAN AND MEDIAN ESTIMATES */
/* NO ADJUSTMENT FOR DIFFERENT NUMBER OF ESTIMATES IN EACH QUARTER */
data quarterlymatching;
	set quarterlybase;
	by TICKER STATPERS;
	if first.STATPERS and STATPERS > FPEDATS and month(STATPERS) = month(FPEDATS) then MEDVAR = ACTUAL;
		else MEDVAR = MEDEST;
	if first.STATPERS and STATPERS > FPEDATS and month(STATPERS) = month(FPEDATS) then ACTDUMMY = 1;
		else ACTDUMMY = 0;
	if first.STATPERS and STATPERS > FPEDATS and month(STATPERS) = month(FPEDATS) then MEANVAR = ACTUAL;
		else MEANVAR = MEANEST;
	label MEDVAR = "Median Estimate (Holding)";
	label MEANVAR = "Mean Estimate (Holding)";
	label ACTDUMMY = "Dummy for Using Actual in Mean and Median Estimate (Holding)";
run;

proc sort data = quarterlymatching;
	by TICKER STATPERS CUSIP;
quit;

proc means data = quarterlymatching sum noprint;
	var MEDVAR MEANVAR;
	by TICKER STATPERS CUSIP SHOUT PRICE;
	output out = quarterlystatistics n = Q4NQTR sum(MEDVAR) = ADJMED sum(MEANVAR) = ADJMEAN sum(ACTDUMMY) = Q4MOD sum(NUMEST) = TOTALEST;
quit;

data quarterly;
	set quarterlystatistics;
	drop _TYPE_ _FREQ_ ADJMED ADJMEAN TOTALEST;
	label Q4NQTR = "Number of Quarters with Estimates";
	label Q4MOD = "Dummy for Using Actual in Mean and Median Estimate";
	Q4MED = ADJMED * (4 / Q4NQTR);
	label Q4MED = "Scaled Median EPS for 4 Quarters";
	Q4MEAN = ADJMEAN * (4 / Q4NQTR);
	label Q4MEAN = "Scaled Mean EPS for 4 Quarters";
	Q4NUMEST = TOTALEST * (4 / Q4NQTR);
	label Q4NUMEST = "Adjusted Number of Estimates for 4 Quarters";
run;

/* CREATING THE YEARLY MEAN AND MEDIAN ESTIMATES */
proc sort data = yearlybase;
	by TICKER STATPERS CUSIP;
quit;

data yearly;
	set yearlybase;
	drop OFTIC CNAME FPI ACTUAL ANNDATS_ACT;
	if FPEDATS - STATPERS => 90 then Y1NEWINFO = 1;
		else if FPEDATS - STATPERS < 90 then Y1NEWINFO = 0;
	label Y1NEWINFO = "Dummy if more than 3 Months of New Information in 1 Year Estimate";
	rename NUMEST = Y1NUMEST;
	label NUMEST = "Number of 1 Year Estimates";
	rename MEDEST = Y1MEDEST;
	label MEDEST = "Median of 1 Year Estimates";
	rename MEANEST = Y1MEANEST;
	label MEANEST = "Mean of 1 Year Estimates";
	rename FPEDATS = Y1FPEDATS;
	label FPEDATS = "Forecast Period End Date of 1 Year Estimates";
run;
	
/* CREATING THE BIYEARLY MEAN AND MEDIAN ESTIMATES */
proc sort data = biyearlybase;
	by TICKER STATPERS CUSIP;
quit;

data biyearly;
	set biyearlybase;
	drop OFTIC CNAME FPI ACTUAL ANNDATS_ACT;
	rename NUMEST = Y2NUMEST;
	label NUMEST = "Number of 2 Year Estimates";
	rename MEDEST = Y2MEDEST;
	label MEDEST = "Median of 2 Year Estimates";
	rename MEANEST = Y2MEANEST;
	label MEANEST = "Mean of 2 Year Estimates";
	rename FPEDATS = Y2FPEDATS;
	label FPEDATS = "Forecast Period End Date of 2 Year Estimates";
run;

/* MERGING... */
data mergednumbers;
	merge biyearly yearly quarterly;
	by TICKER STATPERS CUSIP;
run;

/* CREATING 12 MONTH FORWARD NUMBERS AS PER THOMSON REUTERS METHOD */
data merged;
	set mergednumbers;
	drop LEFTYM LEFTM;
	LEFTYM = 12 * (YEAR(Y1FPEDATS) - YEAR(STATPERS));
	LEFTM = MONTH(Y1FPEDATS) - MONTH(STATPERS);
	LEFTMONTH = max(0, LEFTM + LEFTYM + 1);
	label LEFTMONTH = "Number of Month Ends Left for the Y1 Forecast";
	if Y1FPEDATS = . then M12MEANEST = .;
		else M12MEANEST = (LEFTMONTH * Y1MEANEST + (12 - LEFTMONTH) * Y2MEANEST) / 12;
	label M12MEANEST = "Months Weighted Mean of 12 Month Estimates (TR)";
	if Y1FPEDATS = . then M12MEDEST = .;
		else M12MEDEST = (LEFTMONTH * Y1MEDEST + (12 - LEFTMONTH) * Y2MEDEST) / 12;
		label M12MEDEST = "Months Weighted Median of 12 Month Estimates (TR)";
run;

proc download data = merged out = zero.merged;
quit;











/* EXTRA JUST IN CASE ITEMS

data zero.epdata;
	set zero.merged;
	Y2MEANEP = Y2MEANEST / PRICE;
	Y2MEDEP = Y2MEDEST / PRICE;
	Y1MEANEP = Y1MEANEST / PRICE;
	Y1MEDEP = Y1MEDEST / PRICE;
	Q4MEANEP = Q4MEAN / PRICE;
	Q4MEDEP = Q4MED / PRICE;
	M12MEANEP = M12MEANEST / PRICE;
	M12MEDEP = M12MEDEST / PRICE;
	DATAYEAR = year(STATPERS);
run;

proc means data = zero.epdata mean median min max p5 p95 std;
	class DATAYEAR;
	var Y2MEANEP Y2MEDEP Y1MEANEP Y1MEDEP Q4MEANEP Q4MEDEP M12MEANEP M12MEDEP;
quit;

data zero.berkshire;
	set zero.merged;
	if TICKER in ("BKHT" "BKHT/1");
run;
*/

