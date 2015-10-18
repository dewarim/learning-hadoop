-- using the output from the LogMapper class for a simple Apache Hive table:
-- in the hive console, write:
create table urls(url STRING, hits int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
-- then load the data:
--  note1: it looks like this deletes the source file!
--  note2: you will need to change the inpath to your hdfs user and the appropriate path.
load data inpath '/user/ingo/output/o8/part-r-00000' overwrite into table urls;
-- select the urls with the most hits:
select * from urls order by hits desc limit 10;
