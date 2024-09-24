DROP TABLE IF EXISTS commodities;

create external table commodities( 
price_date date, 
price_1 float, 
Price_2 float 
) 
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
with serdeproperties( 
"separatorChar" = ",", 
"quoteChar"="\"",
"escapseChar"="\\"
) 
STORED AS TEXTFILE
location '/user/cloudera/project'
TBLPROPERTIES ("skip.header.line.count"="0");
