# Discussion

## Architecture
- constant.py
- spec_reader.py 
- data_reader.py
- builder.py
- main.py

## Extendability 
Support multiple spec reading
Support different spec file format

## Design choice: multi threading vs multi processing vs single thread

## Test case
### Spec:
Empty header column
Empty row data
Incorrect header vs row data
Incorrect ording
Missing required column
Incorrect data type
Extra column

### Data:
Doesnt honor with spec
missing data
in correct data type

### Filename:
format in correct
format with extra stash "abc-asc_2020-01-01"
format with filename_yyyy-m-d

## scale
[s3] -> [get file list] -> [message queue] -> [fanout] -> [worker] 