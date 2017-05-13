/* including the self explanatory thingy. */
%include includes;

/* including the appropriate macros. */
/* utility is required by mk_history, and is therefore also referenced. */
%include "&_macros/utility.sas";
%include "&_macros/mk_history.sas";

/* creating library for miscellaneous use. */
libname trial '/data/users/harveyc/Siegel';






/* 00 - basic testing... */
/* trying out the expand history macro, figuring out the where statement took some time. -_-;;; */
/* if i had a gun i might have shot myself for being that dumb. */
*%expand_history(crsp.history, history, NCUSIP COMNAM PERMCO, _where = where = (PERMNO in (10107)));

/* printing out the output of the expand history macro test. */
*proc print data = history (obs = 10);
*quit;

/* all is well. now for the real thing. with famous last words and all that. */





/* 01 - create working base ibes file. */
/* opted to do this via the detailed file rather than the summary file... */
/* simply because we are trying to be careful in case since we could either over ... */
/* or under count the number of analysts if we use the summary file. */
/* note that the excluded file was not included since a preliminary check showed that none of the... */
/* firm-analyst-year observations were obtained from the excluded file. */
/* this preliminary check not done here since the excluded file is not on mead. */
/* the stopped file does not have analyst data, and was therefore also excluded. */

/* extract data. */
data analysts;
	set ibes.detu_epsus (keep = TICKER CUSIP ACTDATS ANALYS);
	YEAR = year(ACTDATS);
	label YEAR = "Year";
	/* removing 00000000 cusips since some of these are wrong... */
	if CUSIP = "00000000" then CUSIP = '';
run;





/* 02 - backfilling. how fun! */
/* note that this is probably more efficient using arrays... need to find time to work it out. */
/* checked the data file size first, am pretty sure it will not get to crazy levels yet with the sorts. */

/* first sort the data by ticker and analyst report date... */
proc sort data = analysts;
	by TICKER ACTDATS;
quit;

/* now fill those empty forward looking cells... */
data backfill (drop = CUSIP rename = (NCUSIP = CUSIP));
	set analysts;
	by TICKER;
	retain NCUSIP;
	if first.TICKER then NCUSIP = CUSIP;
	if CUSIP ^= '' then NCUSIP = CUSIP;
run;

/* now the same for the empty backward looking cells... */
proc sort data = backfill;
	by TICKER descending ACTDATS;
quit;

data backfill (drop = CUSIP rename = (NCUSIP = CUSIP));
	set backfill;
	by TICKER;
	retain NCUSIP;
	if first.TICKER then NCUSIP = CUSIP;
	if CUSIP ^= '' then NCUSIP = CUSIP;
run;

/* sorting for merging later on... */
proc sort data = backfill;
	by CUSIP ACTDATS;
quit;






/* 03 - now to create the woolly mammoth in the corner of the room. */
/* create the super large history file to merge with the backfilled file. */
/* we want to match on NCUSIP (CUSIP) and CALDTS (ACTDATS). */
%expand_history(crsp.history, history, NCUSIP COMNAM PERMCO);

/* renaming variables to match with the ibes data.*/
data history;
	set history;
	rename NCUSIP = CUSIP;
	rename CALDT = ACTDATS;
run;

/* sorting for merge. */
proc sort data = history;
	by CUSIP ACTDATS;
quit;

/* now merge. */
data analystfull;
	merge backfill (in = a keep = TICKER YEAR CUSIP ANALYS ACTDATS) history (in = b);
	by CUSIP ACTDATS;
	if a and b;
run;






/* 04 - now to sort and remove duplicates... */
proc sort data = analystfull nodupkey out = analystbase;
	by PERMCO CUSIP YEAR ANALYS;
quit;







/* 05 - now to get the numbers... */
/* get the number of unique analysts in each year. */
proc means data = analystbase n noprint;
	output out = analystcontrol n = NANALYS;
	by PERMCO CUSIP YEAR;
quit;

/* clean up the data. */
data analystcontrol;
	set analystcontrol;
	keep PERMCO YEAR CUSIP NANALYS;
	label NANALYS = "Number of Analysts"
run;

/* now to control for firms with multiple covered issues... */
/* chose to keep whichever issue had more analysts covering it. */

proc sort data = analystcontrol;
	by PERMCO YEAR descending NANALYS;
quit;

data trial.analystcoverage;
	set analystcontrol;
	by PERMCO YEAR descending NANALYS;
	if first.YEAR;
run;

