# B-plus-Tree

### Project Description

The main task of this project is the implementation of generic 2-field disk file, using B+ tree, based on a given BlockFile library. For better file management Access method - AM - functions were implemented. It supports string, integer and floating point data. Specifically, the functions asked:

* ```AM_Init()``` : initialize a global internal data structures.
* ```AM_CreateIndex()``` : create a file , based on a B+ tree index.
* ```AM_DestroyIndex()``` : destroy a file , based on a B+ tree.
* ```AM_OpenIndex()``` : open a previously created file , based on a B+ tree.
* ```AM_CloseIndex ()``` : close an opened file , based on a B+ tree.
* ```AM_InsertEntry()``` : insert a new entry to a file, based on a B+ tree.
* ```AM_OpenIndexScan()``` : open a scan of the file, in order to find the entries that, satisfy a relationship between them indicated by a comparison operator. An example of comparison operator is ```EQUAL``` (key field == value of value).
* ```AM_FindNextEntry()``` : find the next entry that satisfies the comparison operator of a previous scan.
* ```AM_CloseIndexScan()``` : close a scan of the file, based on a B+ tree.
* ```AM_PrintError()``` : print the text shown by the errString parameter and then prints the message corresponding to the last error resulting from any of the AM level routines.
* ```AM_Close()``` : destroy any data structures created.

