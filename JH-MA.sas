data base;
	set harford.jhma;
	format announcedate YYMMDDN8.;
	format effectivedate YYMMDDN8.;
	format uncoeffectivedate YYMMDDN8.;
	sdcdvtoamv = dealvalue / acquirormvfourweeksprior;
	drop uncoeffectivedate
		 sourceborrowing sourcebridgeloan sourcecommonstockissue sourceforeign sourcedebtissue
		 sourcecropfunds sourcejunkbondissue sourcelineofcredit sourcemezza sourcepreferredstockissue
		 sourcerightsissue sourcestapleoffering;
	if SN ^= .;
	if financingknown = 1;
	monthcount = ((year(announcedate) - 1982) * 12) + month(announcedate);
run;

data basecrsp;
	set harford.crsp;
	drop ncusip;
	acquirorcusip = substr(cusip, 1, 6);
	rename cusip = cusipcrsp;
	monthcount = ((year(date) - 1982) * 12) + month(date) + 1;
	if prc >= 0 then crspmv = prc * shrout / 1000;
		else if prc < 0 then crspmv = .;
run;

proc sql;
	create table basecrspsummed as
	select *, sum(crspmv) as crspmvtotal
	from basecrsp
	group by acquirorcusip, monthcount;
quit;

proc sort data = base;
	by acquirorcusip monthcount;
quit;

proc sort data = basecrspsummed nodupkey;
	by acquirorcusip monthcount;
quit;

data harford.baseandcrsp;
	merge base(in = inbase) basecrspsummed;
	by acquirorcusip monthcount;
	if inbase = 1;
	drop cusipcrsp;
	crspdvtoamv = dealvalue / crspmvtotal;
run;

data trial;
	set harford.baseandcrsp;
	if sdcdvtoamv >= 0.05;
	if status in ("C", "U");
	if targetpublic = "Public";
run;

data basecompustat;
	set harford.compustat;
	drop fyr fyearq fqtr tic;
	monthcompustat = ((year(datadate) - 1982) * 12) + month(datadate);
	cusip6 = substr(cusip, 1, 6);
	compustatmv = cshtrq * prccq / 1000000;
	drop cshtrq prccq;
run;

proc sql;
	create table baseacqcompustat as
	select *
	from trial as a
	left join basecompustat as b
	on a.acquirorcusip = b.cusip6 
	where a.monthcount - b.monthcompustat > 0
	and a.monthcount - b.monthcompustat <= 3;
quit;

proc sql;
	create table baseallcompustat as
	select a.*, b.atq as targettotalassets
	from baseacqcompustat as a
	left join basecompustat as b
	on a.targetcusip = b.cusip6 
	where a.monthcount - b.monthcompustat > 0
	and a.monthcount - b.monthcompustat <= 3;
quit;

data testing;
	set baseallcompustat;
	if atq ^= .;
	if targettotalassets ^= .;
run;
