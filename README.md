
# binlogback
Program Name:binlogback<br/>
Summary:Analyze mysql binlog(only for row format)  in reverse order ,to restore incorrect operation<br/>

#usage:
./binlogback.py -f mysql-bin.000001 -m dsec -B unit -t t1 -c c1,c2,c3<br/>
-f,--file             binlog path,multi file split by ',',do not support regex of path<br/>
-m,--mode             asc or desc,default is desc,recovering data need set mode=desc<br/>
-B,--database         schema name<br/>
-t,--table            table name<br/>
-c,--columns          all columns:c1,c2,c3...<br/>
-b,--begin-datetime   begin time,yyyy-MM-DD HH:mm:ss<br/>
-s,--end-datetime     end time,yyyy-MM-DD HH:mm:ss<br/>
-h,--help             usage<br/>
-v,--version          print version<br/>

#Limits:
1:the table must have a primary key,and no ddl changes of any columns,if column numbers changed between begin-datetime and end-datetime,program will exit(1)<br/>
2:only mysql 5.5 is supported so far<br/>
3:only check row format
4:enum set type is not supported so far
#Concact me:
Email:zhkeke2008@163.com<br/>
email me when you meet any error or any bug.<br/>
