# MapReduce_Final_Project
CS 4650 Final Project. Performing MapReduce jobs on H-1B dataset from 2011-2016

## Dataset (Github does not allow files of this size to be uploaded)
[H-1B Visa Applications from 2011-2016 (LINK)](https://www.kaggle.com/nsharan/h-1b-visa)

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

__In Globe directory :__
  __In CSS directory :__
  * __globe.css__ - Style sheets for use in index.html.
  
  __In DATA directory :__
  * __acc_coords.csv__ - CSV file for Accenture geo coords.
  * __cog_coords.csv__ - CSV file for Cognizant geo coords.
  * __delo_coords.csv__ - CSV file for Deliotte geo coords.
  * __ernst_coords.csv__ - CSV file for Ernst & Young geo coords.
  * __hcl_coords.csv__ - CSV file for HCL America geo coords.
  * __ibm_coords.csv__ - CSV file for IBM geo coords.
  * __info_coords.csv__ - CSV file for Infosys geo coords.
  * __mic_coords.csv__ - CSV file for Microsoft geo coords.
  * __tata_coords.csv__ - CSV file for Tata geo coords.
  * __wip_coords.csv__ - CSV file for Wipro geo coords.
  * __emp_data.json__ - JSON file of coordinates for use in geo data visualization.
  * __h1b_kaggle.csv__ - CSV data file used in the Map/Reduce jobs.
  * __top_employers.csv__ - output of top 10 employers Map/Reduce job.
  * __top_jobs.csv__ - output of top 10 jobs Map/Reduce job.
  
  __In IMG directory :__ 
  * __apps_by_year.jph__ - Image of applications by year.
  * __certs_by_year.jpg__ - Image of certifications by year.
  * __top_emp.jpg__ - Image of top employers output.
  * __top_jobs.jpg__ - Image of top jobs output.
  * __wage_breakdown.jpg__ - Image of wage breakdown.
  * __world.jpg__ - Image overlayed over WebGL mesh object displayed in index.html.
  
  __In IPY directory :__
  * __data_vis.ipynb__ - IPython code file for data visualization.
  * __get_coords.ipynb__ - IPython code file for aquiring geo coordinate data.
  
  __In JS directory :__
  * __Detector.js__ - Environment detection javascript file. Used for globe.
  * __globe.js__ - Uses geo coordinatesa to create objects placed on globe.
  * __three.min.js__ - Javascript library used to create and display animated 3D computer graphics in browser.
  * __Tween.js__ - Javascript library used for animating HTML5 and Javascript properties.
  
* __index.html__ - HTML file used to create containers for globe and data visualization.

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

## System Setup :
1. Navigate into your directory that contains the two MapReduce java files and the dataset file, and connect to bridges via sftp using `sftp username@data.bridges.psc.edu`. Substitute your username.
1. Upload __TopJobTypesApproved.java__, __TopEmployers.java__, and __h1b_kaggle.csv__ using the put command.  You will have to have downloaded the dataset from the link provided since github does not allow uploads for files of that size.
Example: `put TopJobTypesApproved.java`
1. You may close the that terminal, or open a new terminal tab/window, and connect to bridges using...  
`ssh -p 2222 -l username bridges.psc.edu`. Substitute your username.
1. Once connected, you'll be in your home directory. Enter the following commands in order to create your directory structure in $SCRATCH and move the files into their appropriate directories.  
`cd $SCRATCH`  
`mkdir proj4`  
`cd proj4`  
`mkdir input output`  
`cd`  
`mv TopJobTypesApproved.java TopEmployers.java $SCRATCH/proj4`  
`mv h1b_kaggle.csv $SCRATCH/proj4/input`  
1. Request resources using `interact -N 4 -t 00:10:00`  
You will have to wait until your resources are allocated. This may take anywhere from a couple of seconds to 10 minutes. It depends on how busy bridges is at the moment.  
It should not take more than 10 minutes to run both jobs. Because of frequent datanode failures, I've found it's best to use at least 4 nodes.
1. Once your resources load hadoop with the following 2 commands.
`module load hadoop`  
`start-hadoop.sh`  
This may take a little while.
1. Once hadoop is done loading, cd into your project 4 directory.  
`cd $SCRATCH/proj4`  
1. Compile TopEmployers.java.  
`hadoop com.sun.tools.javac.Main TopEmployers.java`
1. Create the jar for TopJobTypesApproved.java.  
`jar cf TopEmployers.jar TopEmployers*.class`
1. Compile TopEmployers.java.  
`hadoop com.sun.tools.javac.Main TopJobTypesApproved.java`
1. Create the jar for TopJobTypesApproved.java.  
`jar cf TopJobTypesApproved.jar TopJobTypesApproved*.class`
1. Make the input directory on HDFS.  
`hadoop fs -mkdir -p input`
1. Distribute the dataset to the HDFS input directory you created.  
`hadoop fs -put input/h1b_kaggle.csv input`  
Note that the first path "input/h1b_kaggle.csv" is from your input dir within your current dir (proj4), while the second path is the path to the input directory on HDFS.
1. Run the first job.  While the map and reduce tasks shouldn't take more than a minute, this process may take a 3-4 minutes presumably because of communication and I/O being handled remotely.  
`hadoop jar TopEmployers.jar TopEmployers input/h1b_kaggle.csv TopEmployers_output`  
Output will be written to a new HDFS directory called "TopEmployers_output"
1. OPTIONAL: Once the job is finished, you may display the output.  
`hadoop fs -cat TopEmployers_output/part-r-00000`
1. Save the output file to $SCRATCH/proj4/output since your HDFS will be destroyed after your interact session is over.  
`hadoop fs -get TopEmployers_output/part-r-00000 output/TopEmployers_Output.txt`  
NOTE: This will not work if your $SCRATCH/proj4/output directory already contains files by these names.
1. Run the second job.  Again, the process may take a few minutes.  
`hadoop jar TopJobTypesApproved.jar TopJobTypesApproved input/h1b_kaggle.csv TopJobTypesApproved_output`
1. OPTIONAL: Once the job is finished, you may display the output.  
`hadoop fs -cat TopJobTypesApproved_output/part-r-00000`
1. Save the output file to $SCRATCH/proj4/output since your HDFS will be destroyed after your interact session is over.  
`hadoop fs -get TopJobTypesApproved_output/part-r-00000 output/TopJobTypesApproved_Output.txt`
1. Type `exit` to end the session once both jobs are complete.
1. Navigate to $SCRATCH/proj4/output to see the output files.
1. To get them to your local machine, do the following. Otherwise you are done.  
From the home directory in bridges...  
`mv $SCRATCH/proj4/output/TopJobTypesApproved_Output.txt $SCRATCH/proj4/output/TopEmployers_Output.txt .`  
This will move the output files into the home directory so that you can use sftp get to move them to your local machine.  
1. In a separate terminal, connect to bridges via sftp.  
`sftp username@data.bridges.psc.edu` Substitute your username.  
Once connected, run the following commands to get the output files from bridges.  
`get TopEmployers_Output.txt`  
`get TopJobTypesApproved_Output.txt` 
1. The output files will be stored in the directory you were in when you connected to bridges via sftp.

## GLOBE SETUP :
# This setup assumes that NodeJS and the Node Package Manager(npm) is already installed.
1. Install http-server globally.
`npm install http-server -g`
2. Navigate to directory containing the "globe" directory.
3. Run http-server
`http-server`
4. Open browser go to the localhost:8080 page (The port will be different if you didn't use the default port #).
5. Click on "/globe/" directory in browser.

### END
