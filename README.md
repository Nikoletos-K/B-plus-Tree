![](https://img.shields.io/badge/C-00599C?style=for-the-badge&logo=c&logoColor=white)


---

# B-plus-Tree

__Compile__: ```make```

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

### Directories

* **bin**: The code of the executables that are created.
* **build**: Contains all object files created during compilation.
* **include**: Contains all the header files project has. 
* **lib**: Contains all the libraries you needed for project. It includes the libbf.so file, which is the library for the BF level.
* **src**: The code files (.c) of the application.
* **examples**: It contains a main file (bf_main.c) that uses the BF level as well as the main (main1.c, main2.c, main3.c) which use the AM level.

---

© Myrto Iglezou && Konstantinos Nikoletos
