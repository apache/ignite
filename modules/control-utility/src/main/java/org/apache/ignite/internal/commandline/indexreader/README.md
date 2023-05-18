# Index Reader Utility

THIS UTILITY MUST BE LAUNCHED ON PERSISTENT STORE WHICH IS NOT UNDER RUNNING GRID!

## Run

`./index-reader.sh` or `./index-reader.bat`: run script from `{IGNITE_HOME}/bin` directory:.

## Parameters

`--dir`: partition directory, where index.bin and (optionally) partition files are located.

`--part-cnt`: total partitions count in cache group. Default value: 0.

`--page-size`: page size (in bytes). Default value: 4096.

`--page-store-ver`: page store version. Default value: 2.

`--indexes`: you can specify index tree names that will be processed, separated by comma without spaces, other index trees will be skipped. Default value: `[]`.

`--check-parts`: check cache data tree in partition files and its consistency with indexes. Default value: `false`.

### Usage

`./index-reader.sh|bat --dir [--part-cnt] [--page-size] [--page-store-ver] [--indexes] [--check-parts]`