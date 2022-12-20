-- Load input file from HDFS separated by tab
inputFile = LOAD 'hdfs:///user/root/projectFiles' USING PigStorage('\t') AS (name:chararray,line:chararray);
-- Combine the lines for a Name
grpd = GROUP inputFile BY name;
-- Count the occurence of each Line (Reduce)
cntd = FOREACH grpd GENERATE group,COUNT(inputFile.$1) AS numLines;
--Sort the above results for Count
result = ORDER cntd BY numLines DESC;
--force remove projectResults folder
rmf hdfs:///user/root/projectResults;
-- Store the result in HDFS
STORE result INTO 'hdfs:///user/root/projectResults';
