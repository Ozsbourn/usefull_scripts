## For what?

This script allow you migrate data from dump (```*.sql```, ```*.gz```, ```*.xz``` [check that that you already have installed one **'xz'** package on your Linux or **'xz-utils'** on Windows])

##
### Abilities and Restrictions

Safe for PostgreSQL, don't adapt to another RDBMS and didn't check it works on them.
What is abilities of them:
 - [ ] good for huge dumps
 - [ ] don't break INSERT (guarantee that multi-line INSERT will be at one transaction)
 - [ ] support zipped version of dumps in ```.gz/.xz```
 - [ ] there is progress bar for huge dumps and log
 - [ ] support ```...``` and ```$$-quoted strings```
 - [ ] skip ```COPY FROM STDIN```
 - [ ] support DryRun usage
 - [ ] can resume after fail (use w/ flag)
 - [ ] there is JSON log for support CI or automate
 
 What CAN NOT do:
 - WORK W/ ```INSERT ... SELECT```
 - WORK W/ DIFFERENT SETS OF COLUMNS 
 - WORK W/ RESUME BY BYTES ON ```*.gz``` OR ```*.xz``` (only logically)

**_Also, note that Bash veriant don't have DryRun mode, state, --force-utf8 (utf8 w/o BOM is default for Unix), JSON logs._**
| | Bash (resume mode) | PowerShell (resume mode) |
|-----------|-----------|-----------|
| Resume by files  | + | + |
| Resume by bytes  | x | + |
| Resume in INSERT | x | ! |


##
### Dependencies
For windows:
- Powershell 5.1 or PowerShell 7+

For Linux:
- bash >= 4
- ```pv``` for progress bar 

##
### How use

Poweshell variant
```powershell
# pure sql with autodetect encoding
.\split_sql.ps1 -InputFile dump.sql

# gzip and xz examples
.\split_sql.ps1 -InputFile dump.sql.gz
.\split_sql.ps1 -InputFile dump.sql.xz

# auto-detect + dry-run
.\split_sql.ps1 -InputFile dump.sql -DryRun

# if should change encoding to UTF-8
.\split_sql.ps1 -InputFile dump.sql --force-utf8


#parallel import
.\import-parallel.ps1 `
  -Dir transactions `
  -Parallel 6 `
  -PsqlArgs "-h localhost -U app -d prod"
```

Bash variant
```bash
# Bash version
chmod +x split_sql.sh

./split_sql.sh -i dump.sql.gz --dry-run
./split_sql.sh -i dump.sql.gz --resume
```

**_Recommended workflow_**
```
1. Dry-run
2. Split с resume + json log
3. JSON checking
4. Import
5. COPY import
```
