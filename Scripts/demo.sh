#ssh -p 2222 -l ejkrapil bridges.psc.edu
#interact -N <# of nodes> -t HH:MM:SS
#Run this script (Should not be run unless package structure is correct within $SCRATCH/proj4/)

module load hadoop
start-hadoop.sh
sleep 10s #Wait a bit for nodes to be ready, this is a hack to prevent previous failures where hadoop had not loaded fully yet
echo "DONE LOADING HADOOP"
cd $SCRATCH/proj4

hadoop fs -mkdir -p input
echo "~Hadoop input directory created."
sleep 1s
hadoop fs -put input/h1b_kaggle.csv input
sleep 1s
echo "~h1b_kaggle.csv distributed to HDFS"

echo "~Running Top Employers job"
hadoop jar TopEmployers.jar TopEmployers input/h1b_kaggle.csv TopEmployers_output
hadoop fs -cat TopEmployers_output/part-r-00000
hadoop fs -get TopEmployers_output/part-r-00000 output/TopEmployers_Output.txt

echo "~Running Most Common Job Types Certified job"
hadoop jar TopJobTypesApproved.jar TopJobTypesApproved input/h1b_kaggle.csv TopJobTypesApproved_output
hadoop fs -cat TopJobTypesApproved_output/part-r-00000
hadoop fs -get TopJobTypesApproved_output/part-r-00000 output/TopJobTypesApproved_Output.txt
