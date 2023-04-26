# Ignite index-reader utility

## Run

`./index-reader.sh` or `./index-reader.bat`: run script from Ignite bin directory.

## Parameters

`--dir`: partition directory where index.bin and optionally partition files are located.

`--part-cnt`: full partitions count in cache group. Default value: 0.

`--page-size`: page size. Default value: 4096.

`--page-store-ver`: page store version. Default value: 2.

`--indexes`: index tree names to be processed. Separate them by comma without spaces. Default value: null.

`--dest-file`: file to print the report to. Default value: null. By default, the report is printed to console.

`--check-parts`: check cache data tree in partition files on its consistency with indexes. Default value: false.

### Command line examples

To analyze files from `.../corrupted_idxs` with 1024 partitions in the cache group, with pageSize=4096 and page store 
version 2, the report goes to report.txt:

```code
./index-reader.sh --dir ".../corrupted_idxs" --part-cnt 1024 --page-size 4096 --page-store-ver 2 --dest-file "report.txt"
```

To read only SQL indexes:

```code
./index-reader.sh --dir ".../corrupted_idxs" --dest-file "report.txt"
```

To read SQL indexes and check cache data tree in partitions:

```code
./index-reader.sh --dir ".../corrupted_idxs" --part-cnt 1024 --check-parts --dest-file "report.txt"
```

### Output examples

RECURSIVE and HORIZONTAL output sections contain information about index trees: tree name, root page id, page type 
statistics, the count of items.

```
<RECURSIVE> Index tree: I [idxName=2654_-1177891018__key_PK##H2Tree%0, pageId=0202ffff00000066]
<RECURSIVE> -- Page stat:
<RECURSIVE> H2ExtrasLeafIO: 2
<RECURSIVE> H2ExtrasInnerIO: 1
<RECURSIVE> BPlusMetaIO: 1
<RECURSIVE> -- Count of items found in leaf pages: 200
<RECURSIVE> No errors occurred while traversing.

...

<RECURSIVE> Total trees: 19
<RECURSIVE> Total pages found in trees: 49
<RECURSIVE> Total errors during trees traversal: 2
```

Page lists section contains bucket data with list meta, bucket number and page statistics:

```
<PAGE_LIST> Page lists info.
<PAGE_LIST> ---Printing buckets data:
<PAGE_LIST> List meta id=844420635164675, bucket number=0, lists=[844420635164687]
<PAGE_LIST> -- Page stat:
<PAGE_LIST> H2ExtrasLeafIO: 32
<PAGE_LIST> H2ExtrasInnerIO: 1
<PAGE_LIST> BPlusMetaIO: 1
<PAGE_LIST> ---No errors.
```

The sequential scan info output:

```
---These pages types were encountered during sequential scan:
H2ExtrasLeafIO: 165
H2ExtrasInnerIO: 19
PagesListNodeIO: 1
PagesListMetaIO: 1
MetaStoreLeafIO: 5
BPlusMetaIO: 20
PageMetaIO: 1
MetaStoreInnerIO: 1
TrackingPageIO: 1
---
Total pages encountered during sequential scan: 214
Total errors occurred during sequential scan: 0
```

Index reader compares the results of the both traversals and the sizes of indexes of the same cache. An error message
about index size inconsistency may look as follows:

```
<ERROR> Index size inconsistency: cacheId=2652, typeId=885397586
<ERROR>      Index name: I [idxName=2652_885397586_T0_F0_F1_IDX##H2Tree%0, pageId=0002ffff0000000d], size=1700
<ERROR>      Index name: I [idxName=2652_885397586__key_PK##H2Tree%0, pageId=0002ffff00000005], size=0
<ERROR>      Index name: I [idxName=2652_885397586_T0_F1_IDX##H2Tree%0, pageId=0002ffff00000009], size=1700
<ERROR>      Index name: I [idxName=2652_885397586_T0_F0_IDX##H2Tree%0, pageId=0002ffff00000007], size=1700
<ERROR>      Index name: I [idxName=2652_885397586_T0_F2_IDX##H2Tree%0, pageId=0002ffff0000000b]
```