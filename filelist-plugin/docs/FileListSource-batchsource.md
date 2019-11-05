# File List Batch Source

Description
-----------
The File Metadata plugin is a source plugin that allows users to read file metadata from a local HDFS or a local filesystem.


Use Case
--------
Use this source to extract the metadata of files under specified paths.

Properties
----------
| Configuration          | Required | Default   | Description                                                                                                                                                                                                                            |
| :--------------------- | :------: | :------   | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Reference Name**     |  **Y**   | None      | This will be used to uniquely identify this source for lineage, annotating metadata, etc.                                                                                                                                              |
| **Scheme**             |  **Y**   | file      | The scheme of the local destination filesystem. Use "file" for writing to the local filesystem and "hdfs" for local HDFS.                                                                                                              |
| **Source Paths**       |  **Y**   | None      | Path(s) to file(s) to be read. If a directory is specified, end the path name with a '/'.                                                                                                                                              |
| **Max Split Size**     |  **Y**   | None      | Specifies the number of files that are controlled by each split. The number of splits created will be the total number of files divided by Max Split Size. The InputFormat will assign roughly the same number of bytes to each split. |
| **Copy Recursively**   |  **Y**   | True      | Whether or not to copy recursively. Similar to the `-r` option in the `cp` terminal command. Set this to true if you want to copy the entire directory recursively.                                                                    |

Usage Notes
-----------
This source plugin only reads fileName and fullPath from a local source filesystem.
A StructuredRecord with the following schema is emitted for each file it reads.

| Field                  | Type   | Description                                                                                                                                    |
| :--------------------- | :----- | :-------------------------                                                                                                                     |
| **fileName**           | String | Only contains the name of the file.                                                                                                            |
| **fullPath**           | String | Contains the full path of the file in the source file system.                                                                                  |
