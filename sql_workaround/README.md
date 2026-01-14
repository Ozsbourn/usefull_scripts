## For what?

This script allow you migrate data from dump (.sql, .gz, .xz [check that that you already have installed one 'xz' package on your Linux or 'xz-utils' on Windows])

### Restrictions

Safe for PostgreSQL, don't adapt to another RDBMS and didn't check it works.
 - [ ] good for huge dumps
 - [ ] don't break INSERT (guarantee at one transaction)
 - [ ] support zipped version of dumps in .gz/.xz
 - [ ] there is progress bar for huge dumps and log
 - [ ] support '...' and $$-quoted strings
 - [ ] skip 'COPY FROM STDIN'
 - [ ] support DryRun usage
 - [ ] can resume after fail
 - [ ] there is JSON log for suppport CI
 
 - [ ] DON'T WORK W/ 'INSERT ... SELECT'
 - [ ] DON'T WORK W/ DIFFERENT SETS OF COLUMNS 
 - [ ] DON'T WORK W/ RESUME BY BYTES ON *.gz OR *.xz (only logically)

### How use

Poweshell variant
```powershell
# pure sql
.\split.ps1 -InputFile dump.sql

# gzip
.\split.ps1 -InputFile dump.sql.gz

# xz
.\split.ps1 -InputFile dump.sql.xz

# dry-run
.\split.ps1 -InputFile dump.sql.gz -DryRun

#parallel import
.\import-parallel.ps1 `
  -Dir transactions `
  -Parallel 6 `
  -PsqlArgs "-h localhost -U app -d prod"
```

Bash variant
```bash
# Bash version
chmod +x split-postgres-dump.sh

./split-postgres-dump.sh -i dump.sql.gz --dry-run
./split-postgres-dump.sh -i dump.sql.gz --resume
```

Recommended workflow
```
1 Dry-run
2 Split с resume + json log
3 Проверка JSON
4 Параллельный импорт
5 COPY импорт отдельно
```