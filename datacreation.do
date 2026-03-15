**********************************************
*Merging individual income, hhincome and POI files (wave 11 - wave 18)
*by: Yasar
*Date : 24/02/26
**********************************************

*The below codes constructs individual income measures across waves by combining wage and self-employment earnings. For respondents classified as employees (employment status codes 1, 2, or 4), monthly wage income is corrected by setting missing values to zero, and a wave-level average wage income is calculated as the mean over the four months in that wave. For the self-employed (employment status code 3), household-level business income is first cleaned (dropping values coded as –99), then missing entries are replaced with zero, and the remaining business income is evenly divided across all household members reported as self-employed. Any individual wage income for self-employed persons is added to this share to capture total self-employment earnings, which are again averaged across the four months of each wave. The final wave-level earnings variable assigns employees their average wage income and the self-employed their business-adjusted income, while non-working individuals (code 0) are assigned zero. 

/*
w11 - 41, 42, 43, 44
w12 - 45, 46, 47, 48
w13 - 49, 50, 51, 52
w14 - 53, 54, 55, 56

*/

*ASSUMPTION: if someone is employed as wage worker in wave X, then their earnings for the corresponding 4 months is marked as zero or non zero, not missing
*ASSUMPTION: if someone is engaged in self employment (SE) in wave X, then all corresponding months of hhincbus is either non zero or zero; missing is marked as zero
*total SE income in a month is the per capita hhincbus + earnings from wages since some individuls report wage earnings along with hhincbus


****************************************************
* FOR WAVES 11–18
****************************************************
if (c(username) == "YasarArafathPaleri") {
    global ospath "D:\cse Dropbox\cseteam"
}
global inc "D:/OneDrive - Azim Premji Foundation/Documents/APU- April/isle/incomemerge"
global rawdatahh "${ospath}/data/cleaneddata/cmie/income/hhincome"
global poi "${ospath}/data/cleaneddata/cmie/poi"
global rawdata "${ospath}/data/cleaneddata/cmie/income/memincome"


forvalues w = 11/18 {

    local start = 41 + 4*(`w' - 11)
    local m1 = `start'
    local m2 = `start' + 1
    local m3 = `start' + 2
    local m4 = `start' + 3

    ***********************************************
    * MEMBER INCOME MERGE
    ***********************************************

    foreach m in `m1' `m2' `m3' `m4' {
    *keep member income variables required for analysis
        use "${rawdata}/cmiememincome`m'.dta", clear
        keep hhid pid month_m monthslot_m memincwages_m responsestatus
        sort pid
        tempfile mem`m'
        save `mem`m''
    }

    use `mem`m1''
    foreach m in `m2' `m3' `m4' {
        merge 1:1 pid using `mem`m''
        drop _merge
    }

    * drop non-response
    foreach m in `m1' `m2' `m3' `m4' {
        drop if responsestatus_m`m' == "Non-Response"
    }

    merge 1:1 pid using "${poi}/cmiepoi`w'.dta"
    drop _merge
    drop responsestatus_m*
    drop if gender != "M"

    tempfile memwave
    save `memwave'


    ***********************************************
    * HH INCOME MERGE
    ***********************************************

    foreach m in `m1' `m2' `m3' `m4' {

        use "${rawdatahh}/cmiehhincome`m'.dta", clear
		*keep hh income variables required for analysis
        keep hhid responsestatus totalincome_m hhincbusines_m
        tostring hhid, replace
        tempfile hh`m'
        save `hh`m''
    }

    use `hh`m1''
    foreach m in `m2' `m3' `m4' {
        merge 1:1 hhid using `hh`m''
        drop _merge
    }

    merge 1:m hhid using `memwave'
    drop _merge


    ***********************************************
    * WAVE INCOME CONSTRUCTION
    ***********************************************
    *total income of HH (income of hh from all sources)
    gen totalincome_w`w' = totalincome_m`m1' + totalincome_m`m2' + totalincome_m`m3' + totalincome_m`m4'
    gen monthlyincome_w`w' = totalincome_w`w'/4


    * employment arrangement
    gen emp_arrangement_w`w' = .
    replace emp_arrangement_w`w' = 1 if employmentarrangement_w`w' == "Daily Wage worker/ Casual labour"
    replace emp_arrangement_w`w' = 2 if employmentarrangement_w`w' == "Salaried - Temporary"
    replace emp_arrangement_w`w' = 3 if employmentarrangement_w`w' == "Self-employed"
    replace emp_arrangement_w`w' = 4 if employmentarrangement_w`w' == "Salaried - Permanent"
    replace emp_arrangement_w`w' = 5 if employmentarrangement_w`w' == "Not Applicable"


    ***********************************************
    * EMPLOYEE EARNINGS (CASUAL LABOR, SAL PERM, SAL TEMP)
    ***********************************************

    foreach m in `m1' `m2' `m3' `m4' {
        gen memincwagescorr_m`m' = memincwages_m`m'
        replace memincwagescorr_m`m' = 0 if missing(memincwages_m`m') & inlist(emp_arrangement_w`w',1,2,4)
    }

    gen avgwageearn_w`w' = ///
        (memincwagescorr_m`m1' + memincwagescorr_m`m2' + memincwagescorr_m`m3' + memincwagescorr_m`m4')/4 ///
        if inlist(emp_arrangement_w`w',1,2,4)


    ***********************************************
    * SELF EMPLOYED EARNINGS (SELF-EMP)
    ***********************************************

    gen selfemp`w' = emp_arrangement_w`w'==3
    egen numse`w' = total(selfemp`w'), by(hhid)

    foreach m in `m1' `m2' `m3' `m4' {

        gen hhincbusrev_m`m' = hhincbusines_m`m'
        replace hhincbusrev_m`m' = . if hhincbusines_m`m'==-99

        gen hhincbuscorr_m`m' = hhincbusrev_m`m'
        replace hhincbuscorr_m`m' = 0 if missing(hhincbusrev_m`m') & emp_arrangement_w`w'==3
        replace hhincbuscorr_m`m' = hhincbuscorr_m`m'/numse`w' if emp_arrangement_w`w'==3

        gen hhincbustot_m`m' = hhincbuscorr_m`m'
        replace hhincbustot_m`m' = hhincbuscorr_m`m' + memincwagescorr_m`m' ///
            if emp_arrangement_w`w'==3
    }

    gen avgseearn_w`w' = ///
        (hhincbustot_m`m1' + hhincbustot_m`m2' + hhincbustot_m`m3' + hhincbustot_m`m4')/4 ///
        if emp_arrangement_w`w'==3


    ***********************************************
    * FINAL INCOME (EMPLOYEE EARNINGS + SELF-EMP)
    ***********************************************

    gen avgearn_w`w' = .
    replace avgearn_w`w' = avgwageearn_w`w' if inlist(emp_arrangement_w`w',1,2,4)
    replace avgearn_w`w' = avgseearn_w`w'   if emp_arrangement_w`w'==3
    
	*those who did not report earnings because they were OOLF in any wave is replaced with 0 instead of missing	

    gen avgearn_nomiss_w`w' = avgearn_w`w'
    replace avgearn_nomiss_w`w' = 0 if missing(avgearn_w`w')


    ***********************************************
    * SAVE
    ***********************************************

    save "D:\OneDrive - Azim Premji Foundation\Documents\APU- April\isle\incomemerge\cmiehhincome_w`w'.dta", replace
}

****************************************************
* PANEL CREATION WAVE 10/18
****************************************************


forvalues i=10/18 {
	
use "${inc}/cmiehhincome_w`i'.dta", clear
rename *_w`i' *
destring hhid, replace
gen time="q`i'"
tempfile income`i'
save `income`i''
}

use `income10'
forvalues j = 11/18 {
	append using `income`j''
	}


replace time = "q1" if time=="q10"
replace time = "q2" if time=="q11"
replace time = "q3" if time=="q12"
replace time = "q4" if time=="q13"
replace time = "q5" if time=="q14"
replace time = "q6" if time=="q15"
replace time = "q7" if time=="q16"
replace time = "q8" if time=="q17"
replace time = "q9" if time=="q18"

gen wave = 1 if time=="q1"
replace wave = 2 if time=="q2"
replace wave = 3 if time=="q3"
replace wave = 4 if time=="q4"
replace wave = 5 if time=="q5"
replace wave = 6 if time=="q6"
replace wave = 7 if time=="q7"
replace wave = 8 if time=="q8"
replace wave = 9 if time=="q9"

****************************************************
* CREATING SAMPLE FOR ANALYSIS
****************************************************

gen sampleind = 1 if natureofoccupation=="Student" & inlist(employmentstatus, "Unemployed, not willing and not looking for a job", "Not Applicable")

****************************************************
* CREATING VARIABLES
****************************************************
*education
gen educnum = .
replace educnum = 1 if education=="1st Std. Pass" | education == "Pre School"
replace educnum = 2 if education=="2nd Std. Pass"
replace educnum = 3 if education=="3rd Std. Pass"
replace educnum = 4 if education=="4th Std. Pass"
replace educnum = 5 if education=="5th Std. Pass"
replace educnum = 6 if education=="6th Std. Pass"
replace educnum = 7 if education=="7th Std. Pass"
replace educnum = 8 if education=="8th Std. Pass"
replace educnum = 9 if education=="9th Std. Pass"
replace educnum = 10 if education== "10th Std. Pass"
replace educnum = 11 if education=="11th Std. Pass"
replace educnum = 12 if education=="12th Std. Pass"
replace educnum = 13 if education=="Diploma / certificate course" | education=="Diploma"
replace educnum = 14 if education=="Graduate"
replace educnum = 15 if education=="Post Graduate"
replace educnum = 16 if education=="Ph.D / M.Phil"
replace educnum = 0 if education=="No Education" | education=="Not Applicable"

*father's education
gen educfatherdrop = educnum if relationwithhoh == "HOH" & gender=="M"
bysort hhid: egen educfather = max(educfatherdrop)
gen fe = educfather if inlist(relationwithhoh, "Son", "Daughter")


*father's employment
gen emp_pre = .
replace emp_pre = 1 if employmentarrangement == "Daily Wage worker/ Casual labour"
replace emp_pre = 2 if employmentarrangement == "Salaried - Temporary"
replace emp_pre = 3 if employmentarrangement == "Self-employed"
replace emp_pre = 4 if employmentarrangement == "Salaried - Permanent"
replace emp_pre = 5 if employmentarrangement == "Not Applicable"

gen empfatherdrop = emp_pre if relationwithhoh == "HOH" & gender=="M"
bysort hhid: egen empfather = max(empfatherdrop)
gen emp_father = empfather if inlist(relationwithhoh, "Son", "Daughter")

bysort hhid memid: gen samtime = wave if sampleind==1
*this marks the time when the individual appears with the required features of the sample.
bysort hhid memid: egen samtime1 = min(samtime)
*samtime1 variable identifies the first time the individual appears with the required features of the sample 
gen dropobs = 1 if wave<samtime1 
drop if dropobs == 1
*drop all obs before samtime1, i.e., the first time the individual appears with the required features of the sample 

sort hhid memid wave

gen post_covid = 0

save "D:\OneDrive - Azim Premji Foundation\Documents\APU- April\isle\1\poiinc", replace


********************************
**making the panel balanced
********************************
use "D:\OneDrive - Azim Premji Foundation\Documents\APU- April\isle\1\poiinc", clear
capture drop panelid dup

bysort hhid memid (wave), sort : gen panelid=_n
duplicates tag hhid memid, gen(dup)

count if panelid==1 & dup==8 & wave==1 
count if panelid==1 & dup==7 & wave==2 
count if panelid==1 & dup==6 & wave==3 
count if panelid==1 & dup==5 & wave==4 
count if panelid==1 & dup==4 & wave==5 
count if panelid==1 & dup==3 & wave==6 


bysort hhid memid: gen balpan = 1 if wave==1 & dup==8 & panelid==1 
bysort hhid memid: replace balpan = 1 if wave==2 & dup==7 & panelid==1
bysort hhid memid: replace balpan = 1 if wave==3 & dup==6 & panelid==1
bysort hhid memid: replace balpan = 1 if wave==4 & dup==5 & panelid==1
bysort hhid memid: replace balpan = 1 if wave==5 & dup==4 & panelid==1
bysort hhid memid: replace balpan = 1 if wave==6 & dup==3 & panelid==1

capture drop balancedsample
bysort hhid memid: egen balancedsample = min(balpan)

keep if balancedsample == 1

save "D:\OneDrive - Azim Premji Foundation\Documents\APU- April\isle\1\poiinc_bal", replace