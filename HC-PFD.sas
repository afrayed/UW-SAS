*** Basic setup: First, remove the dupe entries that OpenSecrets says to remove;
data pfd.pfdagree; set pfd.pfdagree; if dupe = ""; drop dupe; run;
data pfd.pfdasset; set pfd.pfdasset; if dupe = ""; AssetTypeCRP = upcase(AssetTypeCRP); drop dupe; run;
data pfd.pfdcomp; set pfd.pfdcomp; if dupe = ""; drop dupe; run;
data pfd.pfdgift; set pfd.pfdgift; if dupe = ""; drop dupe; run;
data pfd.pfdhonoraria; set pfd.pfdhonoraria; if dupe = ""; drop dupe; run;
data pfd.pfdincome; set pfd.pfdincome; if dupe = ""; drop dupe; run;
data pfd.pfdliability; set pfd.pfdliability; if dupe = ""; drop dupe; run;
data pfd.pfdposition; set pfd.pfdposition; if dupe = ""; drop dupe; run;
data pfd.pfdtrans; set pfd.pfdtrans; if dupe = ""; drop dupe; run;
data pfd.pfdtravel; set pfd.pfdtravel; if dupe = ""; drop dupe; run;

*** Cleaning liabilities;
proc sql;
	create table pfd.pfdliability as
		select a.*, b.minvalue as MinLiabilityValue, b.maxvalue as MaxLiabilityValue
		from pfd.pfdliability as a left join pfd.pfdrangesliabilities as b
		on a.LiabilityAmt = b.code and
		   a.chamber = b.chamber;
quit;

*** Cleaning transactions;
* First, combine the appropriate ranges;
data transrangesasset;
	set pfd.pfdrangesasset;
	if chamber ne "H";
run;

data transrangesasset;
	set transrangesasset pfd.pfdrangestranshouseonly;
run;

* Second, merge in the ranges;
proc sql;
	create table pfd.pfdtrans as
		select a.*, b.minvalue as MinTransValue, b.maxvalue as MaxTransValue
		from pfd.pfdtrans as a left join transrangesasset as b
		on a.Asset4TransAmt = b.code and
		   a.chamber = b.chamber;
quit;

* Third, apply the Elizabeth Dole modifications;
proc sql;
	create table pfd.pfdtrans as
		select a.*, b.factor
		from pfd.pfdtrans as a left join pfd.pfddoletransfactors as b
		on a.id = b.id and
		   a.CalendarYear = b.CalendarYear;
quit;

data pfd.pfdtrans;
	set pfd.pfdtrans;
	if factor = . then factor = 1;
	MinTransValue = MinTransValue * factor;
	MaxTransValue = MaxTransValue * factor;
	drop factor;
run;

*** Cleaning assets;
* First, merge the asset type codes;
proc sql;
	create table pfd.pfdasset as
		select a.*, b.AssetTypeDescription, b.useind as AssetTypeUseInd, b.comments as AssetTypeComments
		from pfd.pfdasset as a left join pfd.pfdassettypecodes as b
		on a.AssetTypeCRP = b.AssetTypeCode;
quit;

* Second, merge the ranges for asset income;
proc sql;
	create table pfd.pfdasset as
		select a.*, b.minvalue as MinAssetIncomeValue, b.maxvalue as MaxAssetIncomeValue, b.display as AssetIncomeDisplay
		from pfd.pfdasset as a left join pfd.pfdrangesassetincome as b
		on a.AssetIncomeAmtRange = b.code and
		   a.chamber = b.chamber;
quit;

* Third, merge the ranges for asset value;
proc sql;
	create table pfd.pfdasset as
		select a.*, b.minvalue as MinAssetValue, b.maxvalue as MaxAssetValue
		from pfd.pfdasset as a left join pfd.pfdrangesasset as b
		on a.AssetValue = b.code and
		   a.chamber = b.chamber;
quit;

* Fourth, apply the Elizabeth Dole modifications;
proc sql;
	create table pfd.pfdasset as
		select a.*, b.MinValue as MinDoleValue, b.MaxValue as MaxDoleValue
		from pfd.pfdasset as a left join pfd.pfddoleassetfactors as b
		on a.id = b.id and
		   a.CalendarYear = b.CalendarYear;
quit;

data pfd.pfdasset;
	set pfd.pfdasset;
	if MinDoleValue ne . then MinAssetValue = MinDoleValue;
	if MaxDoleValue ne . then MaxAssetValue = MaxDoleValue;
	drop MinDoleValue MaxDoleValue;
run;


*** Merging in personnel data;
proc sql;
	create table pfd.pfdagree as
		select a.*, b.*
		from pfd.pfdagree as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdasset as
		select a.*, b.*
		from pfd.pfdasset as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdcomp as
		select a.*, b.*
		from pfd.pfdcomp as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdgift as
		select a.*, b.*
		from pfd.pfdgift as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdhonoraria as
		select a.*, b.*
		from pfd.pfdhonoraria as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdincome as
		select a.*, b.*
		from pfd.pfdincome as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdliability as
		select a.*, b.*
		from pfd.pfdliability as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdposition as
		select a.*, b.*
		from pfd.pfdposition as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdtrans as
		select a.*, b.*
		from pfd.pfdtrans as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;

proc sql;
	create table pfd.pfdtravel as
		select a.*, b.*
		from pfd.pfdtravel as a left join pfd.pfdgovtracklegislators as b
		on a.cid = b.opensecrets_id;
quit;







*** Some summary statistics;
* First, the asset values - taking the average of the bounds as the value when the exact value is not available;
data pfd.baseasset;
	set pfd.pfdasset;
	if (AssetExactValue ne "") 		then EstAssetValue = AssetExactValue * 1;
	if (AssetValue ne "") 			then EstAssetValue = (MinAssetValue + MaxAssetValue) / 2;
	if (AssetIncomeAmt ne "")		then EstAssetIncomeValue = AssetIncomeAmt * 1;
	if (AssetIncomeAmtRange ne "") 	then EstAssetIncomeValue = (MinAssetIncomeValue + MaxAssetIncomeValue) / 2;
	if reporttype = "Y";
	if chamber in ("H" "S");
	if RealCode = . then RealCode = RealCode2;
	if AssetTypeCRP = "" then AssetTypeCRP = "NA";
run;

* Adding the Catorder codes;
proc sort data = pfd.baseasset out = temp03; by RealCode; quit;
proc sort data = pfd.pfdcrpindustries(keep = Catcode Catorder) out = temp03ind; by Catcode Catorder; quit;
data temp03ind; set temp03ind; rename Catcode = RealCode; run;

data pfd.baseassetwithindustry;
	merge temp03 (in = inone) temp03ind;
	by RealCode;
	if inone;
	if Catorder = "" then Catorder = "NA";
	if Catorder ^= "NA" then BaseInd = substr(Catorder, 1, 1);
		else BaseInd = "NA";
run;

proc sort data = pfd.baseassetwithindustry; by cid calendaryear chamber party; quit;

proc means data = pfd.baseassetwithindustry noprint;
	by cid calendaryear chamber party;
	var EstAssetValue EstAssetIncomeValue;
	output out = pfd.outassetstats sum = /autoname;
quit;

proc sort data = pfd.outassetstats; by calendaryear chamber party; quit;

proc means data = pfd.outassetstats noprint;
	by calendaryear chamber party;
	var EstAssetValue_sum EstAssetIncomeValue_sum;
	output out = pfd.outassetyearstats mean = /autoname;
quit;

proc sort data = pfd.outassetyearstats; by chamber calendaryear party; quit;

data pfd.outassetyearstats;
	set pfd.outassetyearstats;
	drop _TYPE_;
	rename _freq_ = numobs;
run;

* Second, the transactions - taking the average of the bounds as the value when the exact value is not available;
data pfd.basetrans;
	set pfd.pfdtrans;
	if (Asset4ExactAmt ne "" and Asset4Purchased = "X")		then EstTransValue = Asset4ExactAmt * -1;
	if (Asset4ExactAmt ne "" and Asset4Sold = "X")			then EstTransValue = Asset4ExactAmt * 1;
	if (Asset4TransAmt ne "" and Asset4Purchased = "X") 	then EstTransValue = (MinTransValue + MaxTransValue) / -2;
	if (Asset4TransAmt ne "" and Asset4Sold = "X") 			then EstTransValue = (MinTransValue + MaxTransValue) / 2;
	if reporttype = "Y";
	if chamber in ("H" "S");
run;

proc sort data = pfd.basetrans; by cid calendaryear chamber party; quit;

proc means data = pfd.basetrans noprint;
	by cid calendaryear chamber party;
	var EstTransValue;
	output out = pfd.outtransstats sum =  /autoname;
quit;

proc sort data = pfd.outtransstats; by calendaryear chamber party; quit;

proc means data = pfd.outtransstats noprint;
	by calendaryear chamber party;
	var EstTransValue_sum;
	output out = pfd.outtransyearstats mean =  /autoname;
quit;

proc sort data = pfd.outtransyearstats; by chamber calendaryear party; quit;

data pfd.outtransyearstats;
	set pfd.outtransyearstats;
	drop _TYPE_;
	rename _freq_ = numobs;
run;


* Third, the liability values;
data pfd.baseliability;
	set pfd.pfdliability;
	EstLiabilityValue = (MinLiabilityValue + MaxLiabilityValue) / 2;
	if reporttype = "Y";
	if chamber in ("H" "S");
run;

proc sort data = pfd.baseliability; by cid calendaryear chamber party; quit;

proc means data = pfd.baseliability noprint;
	by cid calendaryear chamber party;
	var EstLiabilityValue;
	output out = pfd.outliabilitystats sum =  /autoname;
quit;

proc sort data = pfd.outliabilitystats; by calendaryear chamber party; quit;

proc means data = pfd.outliabilitystats noprint;
	by calendaryear chamber party;
	var EstLiabilityValue_sum;
	output out = pfd.outliabilityyearstats mean =  /autoname;
quit;

proc sort data = pfd.outliabilityyearstats; by chamber calendaryear party; quit;

data pfd.outliabilityyearstats;
	set pfd.outliabilityyearstats;
	drop _TYPE_;
	rename _freq_ = numobs;
run;

* Finally, combine for wealth;
data assetyear;
	set pfd.outassetstats;
	keep chamber cid calendaryear party EstAssetValue_Sum;
run;

data liabilityyear;
	set pfd.outliabilitystats;
	keep chamber cid calendaryear party EstLiabilityValue_Sum;
run;

proc sort data = assetyear; by cid calendaryear chamber party; quit;
proc sort data = liabilityyear; by cid calendaryear chamber party; quit;

data pfd.pfdwealth;
	merge assetyear (in = inasset) liabilityyear (in = inliability);
	by cid calendaryear chamber party;
	if inasset;
	if inliability;
	EstWealth = EstAssetValue_Sum - EstLiabilityValue_Sum;
run;



*** Other variables;
* Year-on-year Wealth changes;
proc sort data = pfd.pfdwealth out = temp01; by cid CalendarYear party; quit;

data pfd.basewealthgrowth;
	set temp01;
	by cid;
	lagcid = lag(cid);
	lagwealth = lag(EstWealth);
	if cid = lagcid and lagwealth > 0 then wealthgrowth = (EstWealth / lagwealth) - 1;
		else wealthgrowth = .;
run;

proc sort data = pfd.basewealthgrowth out = temp02; by chamber party CalendarYear; quit;

proc means data = temp02 noprint;
	by chamber party CalendarYear;
	var wealthgrowth;
	weight EstWealth;
	output out = pfd.outwealthgrowthyearstats n = mean = p1 = p25 = median = p75 = p99 = /autoname;
quit;

proc means data = temp02 noprint;
	by chamber party;
	var wealthgrowth;
	weight EstWealth;
	output out = pfd.outwealthgrowthyearstatsvw n = mean = p1 = p25 = median = p75 = p99 = /autoname;
quit;

proc means data = temp02 noprint;
	by chamber party;
	var wealthgrowth;
	output out = pfd.outwealthgrowthyearstatsew n = mean = p1 = p25 = median = p75 = p99 = /autoname;
quit;

*** Now asset types by portfolio weights...;
proc sort data = pfd.baseassetwithindustry out = temp03b; by cid calendaryear chamber party AssetTypeCRP; quit;

proc means data = temp03b noprint;
	by cid calendaryear chamber party AssetTypeCRP;
	var EstAssetValue;
	output out = pfd.outassettypeyearstats n = sum = /autoname;
quit;

proc sort data = pfd.outassettypeyearstats; by chamber party assettypecrp; quit;

proc means data = pfd.outassettypeyearstats noprint;
	by chamber party assettypecrp;
	var EstAssetValue_N EstAssetValue_Sum;
	output out = pfd.outassettypestats sum = /autoname;
quit;

* Second industries;
proc sort data = pfd.baseassetwithindustry out = temp04; by cid calendaryear chamber party Catorder; quit;

proc means data = temp04(where = (AssetTypeCRP in ("S" "P" "C" "FI" "R"))) noprint;
	by cid calendaryear chamber party Catorder;
	var EstAssetValue;
	output out = pfd.outassetindustryyear n = sum = /autoname;
quit;

proc sort data = pfd.outassetindustryyear; by chamber party Catorder; quit;

proc means data = pfd.outassetindustryyear noprint;
	by chamber party Catorder;
	var EstAssetValue_N EstAssetValue_Sum;
	output out = pfd.outassetindustry sum = /autoname;
quit;



*** Other variable summary stats - basically by portfolios here;
* Wealth;
proc sort data = pfd.pfdwealth out = wealth; by CalendarYear Chamber Party; quit;

proc means data = wealth noprint;
	var EstWealth EstAssetValue_Sum EstLiabilityValue_Sum;
	by CalendarYear Chamber Party;
	output out = pfd.outwealthyearstats n = mean = p1 = p25 = median = p75 = p99 = std = /autoname;
quit;

proc sort data = pfd.pfdwealth out = wealth; by Chamber Party; quit;

proc means data = wealth noprint;
	var EstWealth EstAssetValue_Sum EstLiabilityValue_Sum;
	by Chamber Party;
	output out = pfd.outwealthstats n = mean = p1 = p25 = median = p75 = p99 = std = /autoname;
quit;





*** Now portfolio weights by asset type...;
proc sort data = pfd.baseassetwithindustry out = assetstemp; by cid calendaryear chamber party; quit;
proc sort data = pfd.outassetstats out = assetssumtemp; by cid calendaryear chamber party; quit;

data pfd.baseassetsumtype;
	merge assetstemp assetssumtemp (keep = cid calendaryear chamber party EstAssetValue_Sum);
	by cid calendaryear chamber party;
	percentageofasset = EstAssetValue / EstAssetValue_Sum;
run;

proc sort data = pfd.baseassetsumtype out = sumfortypetemp; by cid calendaryear chamber party AssetTypeCRP; quit;

proc means data = sumfortypetemp noprint;
	by cid calendaryear chamber party AssetTypeCRP;
	var percentageofasset;
	output out = zzzassetsumfortypecid sum = /autoname;
quit;

proc sort data = sumfortypetemp out = sumfortypetemp02 nodupkey; by cid calendaryear chamber party; quit;

data zzzassetsumfortypecidmerge;
	merge zzzassetsumfortypecid (in = inassets) sumfortypetemp02 (keep = EstAssetValue_Sum);
	drop _TYPE_ _FREQ_;
	if inassets;
run;

proc sort data = zzzassetsumfortypecidmerge; by cid calendaryear chamber party EstAssetValue_sum AssetTypeCRP; quit;

proc transpose data = zzzassetsumfortypecidmerge out = zzzassetsumfortypecidwide prefix = AssetTypeCRP;
	by cid calendaryear chamber party;
	id AssetTypeCRP;
	var percentageofasset_sum;
quit;

data pfd.baseassetsumtypewide;
	set zzzassetsumfortypecidwide;
	drop _NAME_;
	if AssetTypeCRPF = . then AssetTypeCRPF = 0;
	if AssetTypeCRPM = . then AssetTypeCRPM = 0;
	if AssetTypeCRPP = . then AssetTypeCRPP = 0;
	if AssetTypeCRPR = . then AssetTypeCRPR = 0;
	if AssetTypeCRPS = . then AssetTypeCRPS = 0;
	if AssetTypeCRPU = . then AssetTypeCRPU = 0;
	if AssetTypeCRPK = . then AssetTypeCRPK = 0;
	if AssetTypeCRPN = . then AssetTypeCRPN = 0;
	if AssetTypeCRPFI = . then AssetTypeCRPFI = 0;
	if AssetTypeCRPNA = . then AssetTypeCRPNA = 0;
	if AssetTypeCRPI = . then AssetTypeCRPI = 0;
	if AssetTypeCRPO = . then AssetTypeCRPO = 0;
	if AssetTypeCRPB = . then AssetTypeCRPB = 0;
	if AssetTypeCRPC = . then AssetTypeCRPC = 0;
run;

proc sort data = pfd.baseassetsumtypewide; by cid calendaryear chamber party; quit;

* Value weighted percentages of portfolio by asset type;
proc sort data = pfd.outassetstats out = assetstats; by cid calendaryear chamber party; quit;

data valueweightedbaseassetsumtype;
	merge pfd.baseassetsumtypewide assetstats (drop = _TYPE_ _FREQ_);
	by cid calendaryear chamber party;
run;

proc sort data = valueweightedbaseassetsumtype; by chamber party; quit;

proc means data = valueweightedbaseassetsumtype noprint;
	by chamber party;
	weight EstAssetValue_Sum;
	var AssetTypeCRPF AssetTypeCRPM AssetTypeCRPP AssetTypeCRPR AssetTypeCRPS
		AssetTypeCRPU AssetTypeCRPK AssetTypeCRPN AssetTypeCRPFI AssetTypeCRPNA
		AssetTypeCRPI AssetTypeCRPO AssetTypeCRPB AssetTypeCRPC;
	output out = pfd.outassetsumfortypestatsvw mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;

* Equal weighted percentages of portfolio by asset type;
proc sort data = pfd.baseassetsumtypewide; by chamber party cid calendaryear; quit;

proc means data = pfd.baseassetsumtypewide noprint;
	by chamber party;
	var AssetTypeCRPF AssetTypeCRPM AssetTypeCRPP AssetTypeCRPR AssetTypeCRPS
		AssetTypeCRPU AssetTypeCRPK AssetTypeCRPN AssetTypeCRPFI AssetTypeCRPNA
		AssetTypeCRPI AssetTypeCRPO AssetTypeCRPB AssetTypeCRPC;
	output out = pfd.outassetsumfortypestatsew mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;





*** Now portfolio weights by industry...;
proc sort data = pfd.baseassetwithindustry(where = (AssetTypeCRP in ("S" "P" "C" "FI" "R"))) out = assetstemp; by cid calendaryear chamber party; quit;

proc means data = assetstemp noprint;
	by cid calendaryear chamber party;
	var EstAssetValue EstAssetIncomeValue;
	where AssetTypeCRP in ("S" "P" "C" "FI" "R");
	output out = assetssumtemp sum = /autoname;
quit;

proc sort data = assetssumtemp; by cid calendaryear chamber party; quit;

data pfd.baseassetsumindustry;
	merge assetstemp assetssumtemp (keep = cid calendaryear chamber party EstAssetValue_Sum);
	by cid calendaryear chamber party;
	percentageofasset = EstAssetValue / EstAssetValue_Sum;
run;

proc sort data = pfd.baseassetsumindustry out = sumforindustrytemp; by cid calendaryear chamber party BaseInd; quit;

proc means data = sumforindustrytemp noprint;
	by cid calendaryear chamber party BaseInd;
	var percentageofasset;
	output out = zzzassetsumforindustrycid sum = /autoname;
quit;

proc sort data = sumforindustrytemp out = sumforindustrytemp02 nodupkey; by cid calendaryear chamber party; quit;

data zzzassetsumforindustrycidmerge;
	merge zzzassetsumforindustrycid (in = inassets) sumforindustrytemp02 (keep = EstAssetValue_Sum);
	drop _TYPE_ _FREQ_;
	if inassets;
run;

proc sort data = zzzassetsumforindustrycidmerge; by cid calendaryear chamber party EstAssetValue_sum BaseInd; quit;

proc transpose data = zzzassetsumforindustrycidmerge out = zzzassetsumforindustrywide prefix = BaseInd;
	by cid calendaryear chamber party;
	id BaseInd;
	var percentageofasset_sum;
quit;

data pfd.baseassetsumindustrywide;
	set zzzassetsumforindustrywide;
	drop _NAME_;
	if BaseIndNA = . then BaseIndNA = 0;
	if BaseIndY = . then BaseIndY = 0;
	if BaseIndM = . then BaseIndM = 0;
	if BaseIndF = . then BaseIndF = 0;
	if BaseIndE = . then BaseIndE = 0;
	if BaseIndN = . then BaseIndN = 0;
	if BaseIndA = . then BaseIndA = 0;
	if BaseIndB = . then BaseIndB = 0;
	if BaseIndH = . then BaseIndH = 0;
	if BaseIndD = . then BaseIndD = 0;
	if BaseIndC = . then BaseIndC = 0;
	if BaseIndW = . then BaseIndW = 0;
	if BaseIndK = . then BaseIndK = 0;
	if BaseIndQ = . then BaseIndQ = 0;
run;

proc sort data = pfd.baseassetsumindustrywide; by cid calendaryear chamber party; quit;

* Value weighted percentages of portfolio by asset type;
proc sort data = assetssumtemp out = assetstats; by cid calendaryear chamber party; quit;

data valueweightedbaseassetsumind;
	merge pfd.baseassetsumindustrywide assetstats (drop = _TYPE_ _FREQ_);
	by cid calendaryear chamber party;
run;

proc sort data = valueweightedbaseassetsumind; by chamber party; quit;

proc means data = valueweightedbaseassetsumind noprint;
	by chamber party;
	weight EstAssetValue_Sum;
	var BaseIndNA BaseIndY BaseIndM BaseIndF BaseIndE
		BaseIndN BaseIndA BaseIndB BaseIndH BaseIndD
		BaseIndC BaseIndW BaseIndK BaseIndQ;
	output out = pfd.outassetsumforindstatsvw mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;

* Equal weighted percentages of portfolio by asset type;
proc sort data = pfd.baseassetsumindustrywide; by chamber party cid calendaryear; quit;

proc means data = pfd.baseassetsumindustrywide noprint;
	by chamber party;
	var BaseIndNA BaseIndY BaseIndM BaseIndF BaseIndE
		BaseIndN BaseIndA BaseIndB BaseIndH BaseIndD
		BaseIndC BaseIndW BaseIndK BaseIndQ;
	output out = pfd.outassetsumforindstatsew mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;



*** Here create base datasets to check for asset / transaction PERMCO / PERMNO / CUSIPs...;
data checkassets;
	set pfd.baseassetwithindustry;
	keep assetsource orgname ultorg realcode source 
		 assetdescrip orgname2 ultorg2 realcode2 source2 
		 assetsourcelocation assettypecrp assetnotes assettypedescription catorder
		 baseind
		 calendaryear;
run;

proc sort data = checkassets out = pfd.checkassets nodupkey;
	by assetsource orgname orgname2 calendaryear;
quit;

data pfd.checkassetslimited (where = (AssetTypeCRP in ("NA" "P" "S" "U")));
	set pfd.checkassets;
run;

data checktrans;
	set pfd.basetrans;
	keep asset4transacted orgname ultorg realcode source 
		 asset4descrip orgname2 ultorg2 realcode2 source2
		 calendaryear;
run;

proc sort data = checktrans out = pfd.checktrans nodupkey;
	by asset4transacted orgname orgname2 calendaryear;
quit;






*** Now taking the matched PERMNO dataset and adding industry, as well as filtering on share code...;
*** The latter two are from the history file;
*** After which, match this for location?;
* Take history file and clean;
proc sort data = pfd.history out = history;
	by permno startdt;
quit;

data history;
	set history;
	if shrcd in (10 11);
	* here keep only stocks;
run;

* Now take the matched file...;
data dataassets;
	set pfd.dataassets;
	permnomatch = permno * 1;
	if permnomatch ne .;
	date = mdy(12, 31, (calendaryear + 2000));
run;

proc sort data = dataassets out = dataassets; by permno date; quit;

* Now including industry into the matched file...;
proc sql;
	create table siccodes as
		select a.*, b.siccd, b.ncusip
		from dataassets as a left join history as b
		on a.permnomatch = b.permno and
		   a.date gt b.cal_date_start and
		   a.date lt b.cal_date_stop;
quit;

* Dropping unnecessary items;
data siccodesmatch;
	set siccodes;
	drop realcode2 source2 assetsourcelocation assettypecrp assetnotes 
		 assettypedescription catorder baseind realcode source
		 permno number;
run;

data assetsmatch;
	set pfd.baseassetwithindustry;
	drop realcode source realcode2 source2 assetsourcelocation 
		 assettypedescription assettypeuseind assettypecomments url address 
		 phone contact_form rss_url twitter facebook
		 facebook_id youtube youtube_id wikipedia_id;
run;

proc sort data = assetsmatch; by assetsource orgname ultorg assetdescrip orgname2 ultorg2; quit;
proc sort data = siccodesmatch; by assetsource orgname ultorg assetdescrip orgname2 ultorg2; quit;

* Now merging with the politician level file;
data assetsmatch02;
	merge assetsmatch(in = in1) siccodesmatch(in = in2);
	by assetsource orgname ultorg assetdescrip orgname2 ultorg2;
	if in1;
	if permnomatch ne .;
	if siccd ne .;
	rename permnomatch = permno;
	fyear = calendaryear + 2000;
run;

proc sort data = assetsmatch02; by permno fyear; quit;

* Using compustat crsp merged file for the state and incorp data;
* Taken direct from WRDS;
proc sort data = pfd.compustatcrspextract out = compustatcrspextract; by lpermno fyear; quit;

data compustatcrspextract;
	set compustatcrspextract;
	rename lpermno = permno;
	rename state = statecorp;
run;

* Now we merge the politicians file with the state and incorp data file;
data pfd.sicbaseassets;
	merge assetsmatch02(in = in1) compustatcrspextract(in = in2);
	by permno fyear;
	drop datadate liid;
	if in1;
	siccdwide = floor(siccd / 100);
	siccdvwide = floor(siccd / 1000);
run;













*** Now portfolio weights by siccdwide industry...;
* First with no limits on where the company is located;
proc sort data = pfd.sicbaseassets out = assetstemp; by cid calendaryear party; quit;

proc means data = assetstemp noprint;
	by cid calendaryear party;
	var EstAssetValue EstAssetIncomeValue;
	output out = assetssumtemp sum = /autoname;
quit;

proc sort data = assetssumtemp; by cid calendaryear party; quit;

data pfd.sicassetsumindustry;
	merge assetstemp assetssumtemp (keep = cid calendaryear party EstAssetValue_Sum);
	by cid calendaryear party;
	percentageofasset = EstAssetValue / EstAssetValue_Sum;
run;

proc sort data = pfd.sicassetsumindustry out = sumforindustrytemp; by cid calendaryear party siccdvwide; quit;

proc means data = sumforindustrytemp noprint;
	by cid calendaryear party siccdvwide;
	var percentageofasset;
	output out = zzzassetsumforindustrycid sum = /autoname;
quit;

proc sort data = sumforindustrytemp out = sumforindustrytemp02 nodupkey; by cid calendaryear party; quit;

data zzzassetsumforindustrycidmerge;
	merge zzzassetsumforindustrycid (in = inassets) sumforindustrytemp02 (keep = EstAssetValue_Sum);
	drop _TYPE_ _FREQ_;
	if inassets;
run;

proc sort data = zzzassetsumforindustrycidmerge; by cid calendaryear party EstAssetValue_sum siccdvwide; quit;

proc transpose data = zzzassetsumforindustrycidmerge out = zzzassetsumforindustrywide prefix = siccdvwide;
	by cid calendaryear party;
	id siccdvwide;
	var percentageofasset_sum;
quit;

data pfd.sicassetsumindustrywide;
	set zzzassetsumforindustrywide;
	array change _numeric_;
		do over change;
		if change = . then change = 0;
		end;
run;

proc sort data = pfd.sicassetsumindustrywide; by cid calendaryear party; quit;

* Value weighted percentages of portfolio by asset type;
proc sort data = assetssumtemp out = assetstats; by cid calendaryear party; quit;

data valueweightedbaseassetsumind;
	merge pfd.sicassetsumindustrywide assetstats (drop = _TYPE_ _FREQ_);
	by cid calendaryear party;
run;

proc sort data = valueweightedbaseassetsumind; by party; quit;

proc means data = valueweightedbaseassetsumind noprint;
	by party;
	weight EstAssetValue_Sum;
	var _numeric_;
	output out = pfd.outsicassetsumforindstatsvw mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;

* Equal weighted percentages of portfolio by asset type;
proc sort data = pfd.sicassetsumindustrywide; by party cid calendaryear; quit;

proc means data = pfd.sicassetsumindustrywide noprint;
	by party;
	var _numeric_;
	output out = pfd.outsicassetsumforindstatsew mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;



* Next with limits on where the company is located;
proc sort data = pfd.sicbaseassets out = assetstemp; by cid calendaryear party; quit;

data assetstemp;
	set assetstemp;
	if state ne statecorp;
run;

proc means data = assetstemp noprint;
	by cid calendaryear party;
	var EstAssetValue EstAssetIncomeValue;
	output out = assetssumtemp sum = /autoname;
quit;

proc sort data = assetssumtemp; by cid calendaryear party; quit;

data pfd.sicassetsumindustrynohome;
	merge assetstemp assetssumtemp (keep = cid calendaryear party EstAssetValue_Sum);
	by cid calendaryear party;
	percentageofasset = EstAssetValue / EstAssetValue_Sum;
run;

proc sort data = pfd.sicassetsumindustrynohome out = sumforindustrytemp; by cid calendaryear party siccdvwide; quit;

proc means data = sumforindustrytemp noprint;
	by cid calendaryear party siccdvwide;
	var percentageofasset;
	output out = zzzassetsumforindustrycid sum = /autoname;
quit;

proc sort data = sumforindustrytemp out = sumforindustrytemp02 nodupkey; by cid calendaryear party; quit;

data zzzassetsumforindustrycidmerge;
	merge zzzassetsumforindustrycid (in = inassets) sumforindustrytemp02 (keep = EstAssetValue_Sum);
	drop _TYPE_ _FREQ_;
	if inassets;
run;

proc sort data = zzzassetsumforindustrycidmerge; by cid calendaryear party EstAssetValue_sum siccdvwide; quit;

proc transpose data = zzzassetsumforindustrycidmerge out = zzzassetsumforindustrywide prefix = siccdvwide;
	by cid calendaryear party;
	id siccdvwide;
	var percentageofasset_sum;
quit;

data pfd.sicassetsumindustrywidenohome;
	set zzzassetsumforindustrywide;
	array change _numeric_;
		do over change;
		if change = . then change = 0;
		end;
run;

proc sort data = pfd.sicassetsumindustrywidenohome; by cid calendaryear party; quit;

* Value weighted percentages of portfolio by asset type;
proc sort data = assetssumtemp out = assetstats; by cid calendaryear party; quit;

data valueweightedbaseassetsumind;
	merge pfd.sicassetsumindustrywidenohome assetstats (drop = _TYPE_ _FREQ_);
	by cid calendaryear party;
run;

proc sort data = valueweightedbaseassetsumind; by party; quit;

proc means data = valueweightedbaseassetsumind noprint;
	by party;
	weight EstAssetValue_Sum;
	var _numeric_;
	output out = pfd.outsicassetsumforindstatvwnohome mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;

* Equal weighted percentages of portfolio by asset type;
proc sort data = pfd.sicassetsumindustrywidenohome; by party cid calendaryear; quit;

proc means data = pfd.sicassetsumindustrywidenohome noprint;
	by party;
	var _numeric_;
	output out = pfd.outsicassetsumforindstatewnohome mean = p1 = p25 = median = p75 = p99 = std = /autoname;
run;
