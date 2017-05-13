clear all
set more off

*** Merging Capital IQ files that have already been converted to Stata format.
* NOTE - any files named EXAMPLE(...) must be renamed - eliminate the ().
cd "H:\Siegel\CEO\CapIQ"
! dir *.dat /a-d /b >H:\Siegel\CEO\List.txt

file open base using H:\Siegel\CEO\List.txt, read
file read base line
insheet using `line', delimiter("|")

save `line'.dta, replace
save capitaliq.dta, replace

drop _all

file read base line
while r(eof)==0 {
	insheet using `line', delimiter("|")
	save `line'.dta, replace
	quietly append using capitaliq.dta, force
	quietly save capitaliq.dta, replace
	drop _all
	file read base line
}

renvars v1-v14 \ File CountryA CountryB ComName Ticker IndName ExecOffice ///
	BoardMem EducInstA EducDegA EducInstB EducDegB EducInstC EducDegC
save "H:\Siegel\CEO\CapitalIQ.dta", replace

* Generate matching dataset.
use "H:\Siegel\CEO\CapitalIQ.dta", replace
keep IndName CountryA EducInstA-EducDegC
drop if IndName == ""
replace IndName = upper(IndName)
replace IndName = subinstr(IndName, " (BOARD)", "", .)
replace IndName = subinstr(IndName, " (PRIOR BOARD)", "", .)
replace IndName = subinstr(IndName, " (PRIOR BOARD DECEASED)", "", .)
replace IndName = subinstr(IndName, " (PRIOR)", "", .)
replace IndName = subinstr(IndName, " -PRIOR-", "", .)
replace IndName = subinstr(IndName, "  ", " ", .)
replace IndName = subinstr(IndName, "   ", " ", .)
replace IndName = subinstr(IndName, "    ", " ", .)
replace IndName = subinstr(IndName, "     ", " ", .)
replace IndName = subinstr(IndName, " PH.D.,", "", .)
replace IndName = subinstr(IndName, " M.D.,", "", .)
* Manual editing here...
replace IndName = subinstr(IndName, "TIPPELSKIRCH, ALEXANDER, VON", "VON TIPPELSKIRCH, ALEXANDER", .)
replace IndName = subinstr(IndName, "PALMER, JOHN, W.M.", "PALMER, JOHN W.M.", .)
replace IndName = subinstr(IndName, "PYKE, J, S", "PYKE, J.S.", .)
replace IndName = subinstr(IndName, "TROST, CARLISLE, A.H.", "TROST, CARLISLE A.H.", .)
replace IndName = subinstr(IndName, "FENTENER, VAN VLISSINGEN, J. A.", "FENTENER VAN VLISSINGEN, J. A.", .)
replace IndName = subinstr(IndName, "MARA, SAN MARTN ESPINS, JOS", "SAN MARTN ESPINS, JOS MARA", .)
replace IndName = subinstr(IndName, "LUIS, JOV VINTR, JOS", "JOV VINTR, JOS LUIS", .)
replace IndName = subinstr(IndName, "IGNACIO, GOIRIGOLZARRI TELLAECHE, JOS", "GOIRIGOLZARRI TELLAECHE, JOS IGNACIO", .)
replace IndName = subinstr(IndName, "CARLOS, LVAREZ MEZQURIZ, JUAN", "LVAREZ MEZQURIZ, JUAN CARLOS", .)
* Umlauts and such.
replace IndName = subinstr(IndName, "ü", "UE", .)
replace IndName = subinstr(IndName, "ö", "OE", .)
replace IndName = subinstr(IndName, "ä", "AE", .)
replace IndName = subinstr(IndName, "ë", "EE", .)
replace IndName = subinstr(IndName, "é", "E", .)
replace IndName = subinstr(IndName, "è", "E", .)
replace IndName = subinstr(IndName, "ß", "SS", .)
replace IndName = subinstr(IndName, "å", "A", .)
* Cleaning...
split IndName, p(" (")
drop IndName IndName2-IndName4
rename IndName1 IndName
split IndName, p(", ")
rename IndName1 LastName
rename IndName2 FirstName
replace FirstName = subinstr(FirstName, ".", "", .)
replace FirstName = subinstr(FirstName, ",", "", .)
replace FirstName = subinstr(FirstName, "'", "", .)
replace FirstName = subinstr(FirstName, "-", " ", .)
replace FirstName = subinstr(FirstName, "GNTER", "GUENTER", .)
replace FirstName = subinstr(FirstName, "GNTHER", "GUENTHER", .)
* Problematic, could be JOERG or JUERG.
replace FirstName = subinstr(FirstName, "JRGEN", "JERGEN", .)
replace FirstName = subinstr(FirstName, "JRG", "JERG", .)
replace FirstName = subinstr(FirstName, "RDIGER", "RUEDIGER", .)
replace LastName = subinstr(LastName, ".", "", .)
replace LastName = subinstr(LastName, ",", "", .)
replace LastName = subinstr(LastName, "'", "", .)
replace LastName = subinstr(LastName, "-", " ", .)
replace LastName = subinstr(LastName, "BAUMGRTNER", "BAUMGARTNER", .)
replace LastName = subinstr(LastName, "BCHOLD", "BUECHOLD", .)
replace LastName = subinstr(LastName, "BHRDEL", "BUEHRDEL", .)
replace LastName = subinstr(LastName, "BNNINGHAUSEN", "BOENNINGHAUSEN", .)
replace LastName = subinstr(LastName, "BRGMANN", "BRUEGMANN", .)
replace LastName = subinstr(LastName, "BRNICKE", "BOERNICKE", .)
replace LastName = subinstr(LastName, "BRSIG", "BOERSIG", .)
replace LastName = subinstr(LastName, "BTTNER", "BUETTNER", .)
replace LastName = subinstr(LastName, "DALPAOS GTZ", "DALPAOS GOETZ", .)
replace LastName = subinstr(LastName, "DE MAIZIRE", "DE MAIZIERE", .)
replace LastName = subinstr(LastName, "DRSCHMIDT", "DUERSCHMIDT", .)
replace LastName = subinstr(LastName, "FRSTER", "FOERSTER", .)
replace LastName = subinstr(LastName, "GNTHER", "GUENTHER", .)
replace LastName = subinstr(LastName, "GTZFRIED", "GOETZFRIED", .)
replace LastName = subinstr(LastName, "HLLEN", "HUELLEN", .)
replace LastName = subinstr(LastName, "HLSTRUNK", "HUELSTRUNK", .)
replace LastName = subinstr(LastName, "HTHER", "HUETHER", .)
replace LastName = subinstr(LastName, "JGER", "JAEGER", .)
replace LastName = subinstr(LastName, "KCHLING", "KOECHLING", .)
replace LastName = subinstr(LastName, "KIRCHDRFER", "KIRCHDOERFER", .)
replace LastName = subinstr(LastName, "KLLMER", "KUELLMER", .)
replace LastName = subinstr(LastName, "KLPPER", "KLOEPPER", .)
replace LastName = subinstr(LastName, "KLSGES", "KLOESGES", .)
replace LastName = subinstr(LastName, "KNIG", "KOENIG", .)
replace LastName = subinstr(LastName, "KORTM", "KORTUEM", .)
replace LastName = subinstr(LastName, "KPFER", "KUEPFER", .)
replace LastName = subinstr(LastName, "KRGER", "KRUEGER", .)
replace LastName = subinstr(LastName, "KRPER", "KRUEPER", .)
replace LastName = subinstr(LastName, "KRPICK", "KUERPICK", .)
replace LastName = subinstr(LastName, "KSTLIN", "KOESTLIN", .)
replace LastName = subinstr(LastName, "LBBERT", "LUEBBERT", .)
replace LastName = subinstr(LastName, "LDOZ", "ALDOZO", .)
replace LastName = subinstr(LastName, "LTTGE", "LUETTGE", .)
replace LastName = subinstr(LastName, "LW FRIEDRICH", "LOEW FRIEDRICH", .)
replace LastName = subinstr(LastName, "MALMSTRM", "MALMSTROEM", .)
replace LastName = subinstr(LastName, "MLLEJANS", "MUELLEJANS", .)
replace LastName = subinstr(LastName, "MNDEL", "MUENDEL", .)
replace LastName = subinstr(LastName, "MTHERICH", "MUETHERICH", .)
replace LastName = subinstr(LastName, "NRENBERG", "NOERENBERG", .)
replace LastName = subinstr(LastName, "PFGEN", "PAEFGEN", .)
replace LastName = subinstr(LastName, "PLCKTHUN", "PLUECKTHUN", .)
replace LastName = subinstr(LastName, "RDIGER", "RUEDIGER", .)
replace LastName = subinstr(LastName, "RDLER", "RAEDLER", .)
replace LastName = subinstr(LastName, "SCHFER", "SCHAEFER", .)
replace LastName = subinstr(LastName, "SCHRDER", "SCHROEDER", .)
replace LastName = subinstr(LastName, "SCHRNER", "SCHAERNER", .)
replace LastName = subinstr(LastName, "SHNGEN", "SOEHNGEN", .)
replace LastName = subinstr(LastName, "SLBERG", "SUELBERG", .)
replace LastName = subinstr(LastName, "SLZER", "SAELZER", .)
replace LastName = subinstr(LastName, "STHLIN", "STAEHLIN", .)
replace LastName = subinstr(LastName, "STRER", "STROEER", .)
replace LastName = subinstr(LastName, "STNDEL", "STUENDEL", .)
replace LastName = subinstr(LastName, "STRNGMANN", "STRUENGMANN", .)
replace LastName = subinstr(LastName, "TODENHFER", "TODENHOEFER", .)
replace LastName = subinstr(LastName, "TPFER", "TOEPFER", .)
replace LastName = subinstr(LastName, "VHRINGER", "VOEHRINGER", .)
replace LastName = subinstr(LastName, "WBKING", "WOEBKING", .)
replace LastName = subinstr(LastName, "WIENKENHVER", "WIENKENHOEVER", .)
replace LastName = subinstr(LastName, "WINDMLLER", "WINDMOELLER", .)
replace LastName = subinstr(LastName, "WLFERT", "WUELFERT", .)
replace LastName = subinstr(LastName, "WNSCH", "WUENSCH", .)
replace LastName = subinstr(LastName, "ZLLNER", "ZOELLNER", .)
* This one is problematic, could be MOELLER.
replace LastName = subinstr(LastName, "LBBE", "LOEBBE", .)
replace LastName = subinstr(LastName, "MLLER", "MELLER", .)
* Initial letters matching.
generate FirstNameThree = substr(FirstName, 1, 3)
generate LastNameThree = substr(LastName, 1, 3)
sort IndName FirstName LastName CountryA-EducDegC
quietly by IndName FirstName LastName CountryA-EducDegC: generate duplicate = cond(_N == 1, 0, _n)
drop if duplicate > 1
drop duplicate
generate managerid = _n
save "H:\Siegel\CEO\CapitalIQMatch.dta", replace









*** The Insider Trading data...
cd "H:\Siegel\CEO\"
use "H:\Siegel\CEO\GermanExec.dta", clear

* Keep only unique Company / Name / WKN observations.
sort company name wkn
quietly by company name wkn: generate duplicate = cond(_N == 1, 0, _n)
drop if duplicate > 1
drop duplicate

* Clean names.
replace name = upper(name)
replace name = subinstr(name, "  ", " ", .)
replace name = subinstr(name, "   ", " ", .)
replace name = subinstr(name, "    ", " ", .)
replace name = subinstr(name, "     ", " ", .)

* Umlauts.
replace name = subinstr(name, "ü", "UE", .)
replace name = subinstr(name, "ö", "OE", .)
replace name = subinstr(name, "ä", "AE", .)
replace name = subinstr(name, "ë", "EE", .)
replace name = subinstr(name, "é", "E", .)
replace name = subinstr(name, "è", "E", .)
replace name = subinstr(name, "ß", "SS", .)
replace name = subinstr(name, "å", "A", .)

* Consolidating Vons and other weird names.
replace name = subinstr(name, "VON ", "VON-", .)
replace name = subinstr(name, "ANNEMARIETHOMA", "ANNEMARIE THOMA", .)
replace name = subinstr(name, "SILKETSCHAEGE", "SILKE TSCHAEGE", .)
replace name = subinstr(name, "ADELHEID VON-DER TRENCK - SCHUBERT", "ADELHEID VON-DERTRENCK-SCHUBERT", .)
replace name = subinstr(name, "W. FRANK FOUNTAIN JR.", "W. FRANK JR. FOUNTAIN", .)
replace name = subinstr(name, "ERICH J.LEJEUNE", "ERICH J. LEJEUNE", .)
replace name = subinstr(name, "CHRISTOPH VON-ZUR GATHEN", "CHRISTOPH VON-ZUR-GATHEN", .)
replace name = subinstr(name, "VIVIANA ZU WALDBURG-WOLFEGG UND WALDSEE", "VIVIANA ZU-WALDBURG-WOLFEGG-UND-WALDSEE", .)
replace name = subinstr(name, "S.D.JOHANNES ZU WALDBURG-WOLFEGG UND WALDSEE", "S.D.JOHANNES ZU-WALDBURG-WOLFEGG-UND-WALDSEE", .)
replace name = subinstr(name, "S.D. FURST JOHANNES ZU WALDBURG-WOLFEGG U. WALDSEE", "S.D. FURST JOHANNES ZU-WALDBURG-WOLFEGG-UND-WALDSEE", .)
replace name = subinstr(name, "VON-DER LOCHT", "VON-DER-LOCHT", .)
replace name = subinstr(name, "VON-DEM BUSSCHE", "VON-DEM-BUSSCHE", .)
replace name = subinstr(name, "VON-DER TANN", "VON-DER-TANN", .)
replace name = subinstr(name, "VON-BOHLEN UND HALBACH", "VON-BOHLEN-UND-HALBACH", .)
replace name = subinstr(name, "VON-EISENHART ROTHE", "VON-EISENHART-ROTHE", .)
replace name = subinstr(name, "ALEXANDER VON-ZUR MUHLEN", "ALEXANDER VON-ZUR-MUHLEN", .)
replace name = subinstr(name, "VON-DER SCHULENBURG", "VON-DER-SCHULENBURG", .)
replace name = subinstr(name, " DE ", " DE-", .)
replace name = subinstr(name, "CHRISTIAN WELLER VON-AHLEFELD", "CHRISTIAN WELLER-VON-AHLEFELD", .)
replace name = subinstr(name, "JOHN VON-", "JOHN-VON-", .)
replace name = subinstr(name, "J.-MATTHIAS GRAF VON-DER-SCHULENBURG", "J. MATTHIAS GRAF VON-DER-SCHULENBURG", .)
replace name = subinstr(name, "BILL MC DERMOTT", "WILLIAM MCDERMOTT", .)
replace name = subinstr(name, "ÁLDOZó", "ALDOZO", .)
replace name = subinstr(name, "ANTHONY DI IORIO", "ANTHONY DI-IORIO", .)
replace name = subinstr(name, "HANS VAN BYLEN", "HANS VAN-BYLEN", .)
replace name = "YVONNE DALPAOS-GOTZ" if strpos(name, "ALPAOS") > 0
drop if position == "U"
drop if position == "J"
drop if position == "S"
drop if position == "SF"
drop if position == "S F"

* Removing firms.
generate reverse = strreverse(name)
generate stringposition = strpos(reverse, " ")
generate reversedextract = substr(reverse, 1, stringposition - 1)
generate lastwordextract = strreverse(reversedextract)
drop reverse stringposition reversedextract
drop if lastwordextract == "AG"
drop if lastwordextract == "GBR"
drop if lastwordextract == "GMBH"
drop if lastwordextract == "HERAEUS/LANGE"
drop if lastwordextract == "SCHIFFAHRTS-KG"
sort lastwordextract

* Splitting names.
generate firstname = substr(name, 1, strpos(name, lastwordextract) - 2)

* Further cleanup.
replace lastwordextract = subinstr(lastwordextract, "-", " ", .)
replace firstname = subinstr(firstname, "-", " ", .)
replace firstname = subinstr(firstname, ".", "", .)
rename lastwordextract LastName
rename firstname FirstName
generate execid = _n
generate FirstNameThree = substr(FirstName, 1, 3)
generate LastNameThree = substr(LastName, 1, 3)
generate CountryA = "Germany"
save "H:\Siegel\CEO\GermanExecMatch.dta", replace




* First match on German data only to extract the exact matches.
use "H:\Siegel\CEO\CapitalIQMatch.dta", clear
keep if CountryA == "Germany"
save "H:\Siegel\CEO\CapitalIQDEMatch.dta", replace

use "H:\Siegel\CEO\GermanExecMatch.dta", clear
reclink LastName FirstName using CapitalIQDEMatch, idmaster(execid) idusing(managerid) gen(matchscore) minscore(0.7) wmatch(20 5) wnomatch(20 5) 
save "H:\Siegel\CEO\BaseDEMatch.dta", replace
outsheet using "H:\Siegel\CEO\BaseDEMatch.csv", comma replace

sort LastName ULastName FirstName UFirstName execid managerid
quietly by LastName ULastName FirstName UFirstName execid managerid: generate duplicate = cond(_N == 1, 0, _n)
drop if duplicate > 1
drop duplicate
quietly by LastName ULastName FirstName UFirstName execid: generate duplicate = cond(_N == 1, 0, _n)
save "H:\Siegel\CEO\BaseDEMatchNumbers.dta", replace
outsheet using "H:\Siegel\CEO\BaseDEMatchNumbers.csv", comma replace

/*
* Matching on all Capital IQ
use "H:\Siegel\CEO\GermanExecMatch.dta", clear
reclink LastName FirstName LastNameThree FirstNameThree using CapitalIQMatch, idmaster(execid) idusing(managerid) gen(matchscore) minscore(0.7) wmatch(20 5 5 5) wnomatch(20 5 5 5) 
save "H:\Siegel\CEO\BaseMatch.dta", replace
outsheet using "H:\Siegel\CEO\BaseMatch.csv", comma replace
*/


