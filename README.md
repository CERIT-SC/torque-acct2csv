# Torque accounting logs to CSV

Script converts Torque (https://github.com/CESNET/torque) accounting
logs into a single CSV file. This file can be later easily processed and
visualized, e.g. in Google Fusion Tables.

Accounting logs are usually located on machine with pbs_server in 
/var/spool/torque/server_priv/accounting, one file for every day in format:
http://www.clusterresources.com/torquedocs/9.1accounting.shtml

## Requirements

Perl modules:

* Text::CSV
* DateTime::Format::DateParse

## Usage 

    cat /var/spool/torque/server_priv/accounting/* | \
        ./torque-acct2csv.pl >log.csv

### CSV file format

- Job ID
- Owner
- State
- Exit status
- Queue
- Created at (date)
- Time start [s]
- Time exit [s]
- Time delete [s]
- Node
- Req. nodes
- Req. procs
- Req. mem
- Req. vmem
- Req. walltime
- Used mem
- Used vmem
- Used walltime
- Used cputime
