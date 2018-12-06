# MapReduce_Final_Project
CS 4650 Final Project. Performing MapReduce jobs on H-1B dataset from 2011-2016

## Files
__In MapReduce_Jobs directory :__
* __TopEmployers.java__ - Java file used to run the MapReduce job to determine the top ten employers that submitted the most applications
* __TopJobTypesApproved.java__ - Java file used to run the MapReduce job to determine the 20 most common job types among CERTIFIED applications.

__In JobOutputs directory :__
* __TopEmployers_Output.txt__ - Output from TopEmployers job. Contains employer name and the number of applications submitted, separated by a "-".
* __TopJobTypesApproved_Output.txt__ - Output from TopJobTypesApproved job. Contains job titles and the number of applications with that job type that were approved, separated by a ":".

__In Scripts directory :__
* __project4.sh__ - A script used on bridges when testing changes to the MapReduce jobs. Only for reference, grader should not run this because it won't work with default file structure.
* __demo.sh__ - Script used to demo the project on presentation day. Requires specific file structure already set up on my bridges workspace. Skips compiling MapReduce jobs because this is done before-hand.

## Execution Times and Output Description:
__"Employers With Top Application Submissions" (TopEmployers.java) job Execution Time :__
* Total time spent by all map tasks (ms)=31266
* Total time spent by all reduce tasks (ms)=23212  
Total = 54.478 seconds

* __Output__ : See TopEmployer_Output.txt. Outputs the top ten employers in descending order of the number of applications submitted. Employer name and number of applications is separated by a "-".

__"Most Common Job Types Approved" (TopJobTypesApproved.java) job Execution Time:__
* Total time spent by all map tasks (ms)=43443
* Total time spent by all reduce tasks (ms)=21430  
Total = 64.873 seconds

* __Output__ : See TopJobTypesApproved_Output.txt. Outputs the twenty most common job titles in applications that were CERTIFIED (including CERTIFIED-WITHDRAWN) in descending order of the number of certified applications for the given job title. Job title and the frequency count are separated by a ":".
