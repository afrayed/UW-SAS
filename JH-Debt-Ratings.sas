proc sort data = harford.ratings;
	by gvkey datadate;
quit;

data availableratingsonly;
	set harford.ratings;
	if splticrm ^= "";
run;

proc sort data = harford.compustatdebt;
	by gvkey datadate;
quit;

data debtclean;
	set harford.compustatdebt;
	by gvkey;
	if DD1 = . then DD1 = 0;
		if DD2 = . then DD2 = 0;
		if DD3 = . then DD3 = 0;
		if DD4 = . then DD4 = 0;
		if DD5 = . then DD5 = 0;
		if DLTT = . then DLTT = 0;
		if DLTIS = . then DLTIS = 0;
		if DLTR = . then DLTR = 0;
	purechangeDD1 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD1 - lag(DD1), .));
		purechangeDD2 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD2 - lag(DD2), .));
		purechangeDD3 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD3 - lag(DD3), .));
		purechangeDD4 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD4 - lag(DD4), .));
		purechangeDD5 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD5 - lag(DD5), .));
	yearchangeDD1 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD1 - lag(DD2), .));
		yearchangeDD2 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD2 - lag(DD3), .));
		yearchangeDD3 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD3 - lag(DD4), .));
		yearchangeDD4 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, DD4 - lag(DD5), .));
	sumfivebelow = DD1 + DD2 + DD3 + DD4 + DD5;
	sumfiveabove = DLTT - sumfivebelow;
	purechangeDD1to5 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, sumfivebelow - lag(sumfivebelow), .));
	purechangeDDabove5 = ifn(first.gvkey, ., ifn(fyear - lag(fyear) = 1, sumfiveabove - lag(sumfiveabove), .));
	trialabove4 = DLTIS - DLTR - (yearchangeDD1 + yearchangeDD2 + yearchangeDD3 + yearchangeDD4);
	trialbelow4 = (yearchangeDD1 + yearchangeDD2 + yearchangeDD3 + yearchangeDD4);
	comparetrialbelow4andyearchange = (trialbelow4 / (DLTIS - DLTR));
	if comparetrialbelow4andyearchange > 1.1 and trialbelow4 ^= . and trialbelow4 ^= 0
		then abovecheck = 1;
	if comparetrialbelow4andyearchange < 0.9 and trialbelow4 ^= . and trialbelow4 ^= 0
		then belowcheck = 1;
	if comparetrialbelow4andyearchange ^= 0 and comparetrialbelow4andyearchange ^= . and (abovecheck or belowcheck)
		then tenpercentoffcheck = 1;
		else tenpercentoffcheck = 0;
	drop sumfivebelow sumfiveabove abovecheck belowcheck;
	if fyear => 1980 then weighteddebtmaturity = (1 * DD1 / (DLTT)) + (2 * DD2 / (DLTT)) + (3 * DD3 / (DLTT)) + (4 * DD4 / (DLTT)) + (5 * DD5 / (DLTT)) + (6 * (DLTT - (DD1 + DD2 + DD3 + DD4 + DD5)) / (DLTT));
	if fyear => 1980 then weighteddebtmaturityDD1mod = (1 * DD1 / (DLTT + DD1)) + (2 * DD2 / (DLTT + DD1)) + (3 * DD3 / (DLTT + DD1)) + (4 * DD4 / (DLTT + DD1)) + (5 * DD5 / (DLTT + DD1)) + (6 * (DLTT + DD1 - (DD1 + DD2 + DD3 + DD4 + DD5)) / (DLTT + DD1));
run;

data harford.debtchanges;
	merge debtclean (in = available) availableratingsonly;
	by gvkey datadate;
	if available;
run;
