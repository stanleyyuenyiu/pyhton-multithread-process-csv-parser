# Readme

## Pre requisite
- Python 3.6.x

## Description
The applicaton is design to be a single small script application, 

it read the data file from ./data

it then extrack the filename and get the spec from ./spec,

and the spec data will be cached in memory and using LRU cache eviction policy , which allow us to keep small size of memory

and final the file as .ndjson output to ./output

## File Structure
- constant.py -------------- store all constant value, including the column data type
- spec_reader.py ----------- responsible to read the spec file and cache it in memory
- data_reader.py ----------- responsible to read the data file and return file lines
- builder.py --------------- responsible to coordinate with spec_reader and data_reader, hence to generate the parsed content into .ndjson
- main.py ------------------ the main program, responsible to start multithreading to build the output

## How to run
```
pip3 main.py
```