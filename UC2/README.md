Description
-----------
The Pipeline is used to Anonymize and Persist Files from Local FS to GCS


Deployment
----------
1. Import the FileList PlugIn
2. Set the JAVA LIB PATH to pick up the Voltage Libs. Ensure the voltage libs are deployed on all the nodes of the cluster in exact same location and with user permissions. Ensure YARN user has access to this file system 
eg:
<property>
    <name>app.program.jvm.opts</name>
    <value>-Djava.library.path=/opt/DA/javasimpleapi/lib</value>
  </property>

3. Import Anonymizer PlugIn.

Since the Anonymizer plugin is file based , the schema file schema has to be provided so it can detect which fileds to Anaonymize. 

Eg:
CSV File format 
ID, Name, SSN, County   - and you need to anonymize the 3rd field which is SSN. 

So you neeed to ensure to list all the 4 fields and indicate which to anaonymize and the format to use for anonymization. 

eg:
"FIELD_LIST": "ID:No:AUTO,Name:No:AUTO,SSN:yes:SSN,Country:No:AUTO"



4. Import the Enclosed Pipeline JSON 
