# Wal Reader Utility

[//]: # (THIS UTILITY MUST BE LAUNCHED ON PERSISTENT STORE WHICH IS NOT UNDER RUNNING GRID! )

## Run

`./wal-reader.sh` or `./wal-reader.bat`: run script from `{IGNITE_HOME}/bin` directory:.

## Parameters

`--root`: Root pds directory.

`--folder-name`: Node specific folderName.

`--page-size`: Size of pages, which was selected for file store (1024, 2048, 4096, etc.). Default value: 4096

`--keep-binary`: Keep binary flag Default value: true

`--record-types`: Comma-separated WAL record types (TX_RECORD, DATA_RECORD, etc.). Default value: all

`--wal-time-from-millis`: The start time interval for the record time in milliseconds. Default value: null

`--wal-time-to-millis`: The end time interval for the record time in milliseconds. Default value: null

`--record-contains-text`: Filter by substring in the WAL record. Default value: null

`--process-sensitive-data`: Strategy for the processing of sensitive data (SHOW, HIDE, HASH, MD5) Default value: SHOW

`--print-stat`: Write summary statistics for WAL Default value: false

`--skip-crc`: Skip CRC calculation/check flag. Default value: false

`--pages`: Comma-separated pages or path to file with pages on each line in grpId:pageId format. Default value: null


### Usage

`./wal-reader.sh|bat --root --folder-name [--page-size] [--keep-binary] [--record-types] [--wal-time-from-millis] [--wal-time-to-millis] [--record-contains-text] [--process-sensitive-data] [--print-stat] [--skip-crc] [--pages]`