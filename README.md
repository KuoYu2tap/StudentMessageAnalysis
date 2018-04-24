[TOC]
# 学生数据处理（完整版）
## 需求

- 1 学生成绩

  - A、以学生为单位：计算每个学生总的平均分（按年限分）

- 2 图书馆数据（按借阅为准）

  - A、以学生为单位：计算每个学生借阅总数（按年限分）
  - B、以学生为单位：计算每个学生借阅类型ABCD（按年限分）
  - C、只统计学生借阅次数，忽略学生还书数据
- 3 消费数据
  - A、以学生为单位：消费总额（按年限分）
  - B、删除学生充值记录
- 4 门禁数据

  - A、以学生为单位：9点后进校门次数，11点进校门（按年限分）


### 最终格式

序号	学号	年份	平均分	借阅总数	借书类型[ABCD]	消费总额	9点门禁刷卡	11点门禁刷卡

## 1.理清数据结构

### xueshengchengji.tsv 

> xueshengchengji.tsv: ISO-8859 text, with CRLF line terminators
>
> 字段: XH	XZB	DQSZJ	ZYMC	KCMC	CJ	BKCJ	CXCJ	CXBJ

```bash
# 学号 行政班  考试时间   考试成绩   补考成绩
XH	XZB	DQSZJ	ZYMC	KCMC	CJ	BKCJ	CXCJ	CXBJ
153070111	英语2015-04班	2015	英语	英语语音基础	73			0
153070112	会计2015-06班	2015	会计学	英语语音基础	74			0
153070113	英语2015-04班	2015	英语	英语语音基础	81			0

# 特殊情况
1.CXCJ会出现其他情况，优秀/良好/中等/及格/不及格/通过/不通过/缓考/缺考，需要替换清洗
$ cat xueshengchengji.tsv | awk -F '\t' '$6 ~ /^[^0-9]/{print $6}' |sort|uniq -u
2.补考成绩可能为空

# 清洗
如果第七列为空则为'NA'

# 提取： 学号 成绩 补考成绩
$ cat xueshengchengji.tsv | awk -F '\t' '{if($7=="")$7="NA";if($1 && $3) $1,$6,$7}'
```

### view.sql

> view.sql: UTF-8 Unicode text, with CRLF line terminators
>
> 字段:   XM(姓名) JSZH(学号) ZJM(主机名) CZLX(操作类型.借/还) TSCCH(??) TSTXM(??)  CLSJ(时间) SM(书名)

```bash
$ head -1 view.sql | awk -F '[()]' '{print $2}'
'刘莹', '050532057', '7414', '借书', '0034636', '0034636', '2007-03-23 15:38:38', '罗马戒指'

# 特殊情况
1.借书出现特殊字段
$ cat view.sql | awk -F '[()]' '{print $2}'|awk -F ',' '{print $4}'|sort -u|uniq -u

[space]
丢书赔款
借书
取消预约
并且超期天数大于0 图书超期罚款
续借
赔书
还书
退赔
预约

2.value中含有单引号

# 清洗
CZLX这一行只统计'借书'
文本中的"'"单引号需要使用sed -i "s/'//g" 替换掉


```

### file_*.csv

> file_0.csv: UTF-8 Unicode English text, with CRLF line terminators
> 
> file_1.csv: UTF-8 Unicode English text, with CRLF line terminators
> 
> file_2.csv: UTF-8 Unicode text, with CRLF line terminators
> 
> file_3.csv: UTF-8 Unicode text, with CRLF line terminators
>
> 字段 0,序号,索书号,题名,责任者,出版日期,标准编码,登录号,单价,出版者,借阅次数,索书号,单价

```bash
#0,序号,索书号,题名,责任者,出版日期,标准编码,登录号,单价,出版者,借阅次数,索书号,单价
1,1,TP332.3/WZY(2),可编程控制器教程,主编王兆义,2005,7-111-03758-8,0001896,35,机械工业出版社,0,TP332.3/WZY(2),35

# 特殊情况
'责任者'内部出现','号，与文件分隔码相同

# 清洗
'责任者'为了保证可以使用','，为整个责任者一栏增加了双引号

# 提取
head file_*.csv | awk -F '"' '{print $1,$3}' | awk -F ',' 'OFS=","{print $8,$3,$9}'
登录号,索书号,单价
0009710,F270/YZQ,20
0009839,TU723.3-44/S931,32.8
0013437,F272.9/DNL,29.8
0013479,F471.266/BLS,29.8
0013656,O159,55
```



### TYHXFSJZL_NEW.sql

> TYHXFSJZL_NEW.sql: ASCII text, with CRLF line terminators
>
> 字段(过多，显示有价值的字段)

```bash
# 时间 花销 学号 消费码 消费名目
$ tail YKTYHXFSJZL_NEW.sql |awk -F '[()]' '{print $2}'|awk -F',' 'OFS=","{if($13!=" NULL"&&$19!=" NULL")print $11,$13,$19,$20,$21}'
 
'2017/03/09 18:27:27.290'  2.50  '140710230'  '2042'  '商场消费' 
'2017/03/09 18:27:27.763'  3.50  '163100439'  '2042'  '商场消费'

# 特殊情况
1.有测试数据，value为' NULL'
2.消费码有特殊字段
$ cat YKTYHXFSJZL_NEW.sql |awk -F '[()]' '{print $2}'|awk -F',' '{print $20,$21}'|sort -t' ' -k 1|uniq -f1
'1001'  '联网售票'
 '1003'  '00'
 '1003'  '银行转入'
 '1004'  '支付宝转入'
 '1005'  '交行转入'
 '2032'  '联网售饭'
 '2033'  '可能消费'
 '2039'  '退票'
 '2041'  '结帐退卡'
 '2042'  '商场消费'
 '2054'  '图书收费'
 '2071'  '医疗收费'
 '3032'  '独立售饭'
 '3042'  '独立商场'
 '3054'  '独立图书'
 '3071'  '独立医疗'
 '6039'  '补助联网退票'
 'A001'  '开资金户'
 'A002'  '卡片挂失'
 'A003'  '取消挂失'
 'A004'  '帐户冻结'
 'A005'  '帐户解冻'
 'A006'  '帐户停用'
 'A007'  '帐户启用'
 'A008'  '原卡重写'
 'A009'  '补发新卡'
 'A010'  '开户发卡'
 'B001'  '销资金户'
 'B005'  '修改密码'
 'B006'  '修改限额'
 'B009'  '旧卡重写'
 'D003'  '单改资料'
 'D004'  '批改资料'
 NULL  NULL

# 清洗
有测试数据，学号为' NULL'则抛弃该行

# 提取：年份 学号 消费 （消费类型在[2032,2033]+[2042,3071])
$ cat YKTYHXFSJZL_NEW.sql | awk -F "[()/,']" 'OFS=","$33>2000&$33<4000&$33!=2039&$33!=2041{if($13&&$18$13 !="NULL"&& $13!=" NULL" && $18!=" NULL" &&$18 !="NULL")print $13,$18,$27}'

2017, 2.50, 140710230
2017, 3.50, 163100439
2017, 2.50, 163040736
2017, 10.80, 153050156
2017, 0.50, 130840158
2017, 10, 040560457
```

### MJXXB.sql

>MJXXB.sql: UTF-8 Unicode text, with CRLF line terminators
>
>字段: 学号 设备号 寝室区 设备口号 类型 日期 时间

```bash
# 提取 学号 日期 小时 
$ awk -F"[(),:]" 'OFS=","{gsub(/ /,"",$7); print $2,$7,$8}' MJXXB.sql 
000105541,20160408, 09
000105541,20160408, 09
000105541,20160408, 09
000105541,20160408, 09
000105541,20160408, 09
```

## 2.分析

先看需求格式

*序号	学号	年份	平均分	借阅总数	借书类型[ABCD]	消费总额	9点门禁刷卡	11点门禁刷卡*

我们可以分成

*序号  (==学号  年份==)	(平均分）（==借阅总数	借书类型[ABCD]==）	（消费总额）（==9点门禁刷卡	11点门禁刷卡==）*

按照(学号，年份)为双主键进行合并

xueshengchengji.tsv 是学生成绩的表，只需要提取 ((学号，年份)，分数)来计算平均分

view.sql是借阅表，需要提取的是((学号，年份)，书号，姓名)

file*.csv是书目表，需要提取的是（书号，索书号）

TYHXFSJZL_NEW.sql是一卡通消费表，需要提取(（学号，年份），消费额)

MJXXB.sql是门禁系统表，需要提取(（学号，年份），进出时间)

根据(学号，年份)双主键，可以连接起所有数据，但是因为只有view.sql借阅表中才有姓名，所以需要额外提取(学号，姓名)



## 3.数据提取

### XH_YEAR_CJ.csv (from xueshengchengji.tsv)

学号 年份 成绩 

```bash
root:~/Desktop/h_file$ tail -3 xueshengchengji.tsv | awk -F'\t' 'OFS=","{\
	if($1 && $3) # 学号和年份都存在，则可行\
	{\
		if(!$6) 	# 如果第一成绩为空则置0\
			$6=0;	\
		if($7!="")	# 如果第二成绩不为空则把第二成绩给第一成绩\
			$6=$7;	\
		print $1,$3,$6;\
	}\
}'
153090009,2015,64
153090010,2015,59
153090011,2015,78
```

### XH_SH_YEAR.csv 和 XH_XM.csv(from view.sql)

>  姓名 学号 登陆书号 借书年份

```bash
$ head view.sql | awk -F "[(),'-]" 'OFS=","{if($12=="借书")print $6,$15,$21}'
#学号  书号  年份
 050532057, 0034636, 2007
 050532047, 0034636, 2007

head view.sql | awk -F "[(),'-]" 'OFS=","{if($12=="借书")print $3,$6}'
# 姓名 学号  
刘莹, 050532057
陈妍汝, 050532047
```

### YEAR_COST_XH.csv(from YKTYHXFSJZL_NEW.sql) 

>  年份 花费 学号

```bash
root:~/Desktop/h_file$ tail -5 YKTYHXFSJZL_NEW.sql | awk -F "[()/,']" 'OFS=","{if($13&&$18$13 !="NULL"&& $13!=" NULL" && $18!=" NULL" &&$18 !="NULL"&&$33!="00")print $13,$18,$27}' 
# 年份 花费  学号
2017, 2.50, 140710230
2017, 3.50, 163100439
2017, 2.50, 163040736
2017, 10.80, 153050156
2017, 0.50, 130840158
2017, 10, 040560457
```

### XH_DATE_HOUR_MIN.csv (from MJXXB.sql )

>  学号 日期 小时 分钟

```bash
root:~/Desktop/h_file$ awk -F"[(),:]" 'OFS=","{gsub(/ /,"",$7); print $2,$7,$8,$9}' MJXXB.sql >msginput/XH_DATE_HOUR_MIN.csv
root:~/Desktop/h_file$ head msginput/XH_DATE_HOUR_MIN.csv 
# 学号 日期 小时 分钟
000105541,20160408, 09,36
000105541,20160408, 09,36
000105541,20160408, 09,36
000105541,20160408, 09,36
000105541,20160408, 09,36
1001,20150604, 18,56
1001,20150604, 18,56
```

### DLH_SSH_JG.csv (from file_*.csv )

> 登录号  索书号  价格

```bash
cat file_*.csv | awk -F '"' '{print $1,$3}' | awk -F ',' 'OFS=","{print $8,$3,$9}' >> DLH_SSH_JG.csv &
# 登录号 索书号 价格
0001234,H315/LXM,8
0001235,H315/LXM,8
0001236,H315/LXM,8
0001237,H315/LXM,8
0001238,H315/LXM,8
0001239,H315/LXM,8
0001240,H315/LXM,8
```

##4.最终文件结构 

| 最终文件                                   | 字段                 | 来源                |
| ------------------------------------------ | -------------------- | ------------------- |
|    XM_XH.csv                                     | 姓名，学号 | view.sql |
| XH_YEAR_CJ.csv                             | 学号，年份，分数 | xueshengchengji.tsv |
| XH_SH_YEAR.csv  | 学号，登陆号，年份 | view.sql |
| DLH_SSH_JG.csv | 登录号，索书号，价格 | file_*.csv |
| YEAR_COST_XH.csv | 年份，花销，学号 | YKTYHXFSJZL_NEW.sql |
| XH_DATE_HOUR_MIN.csv | 学号，数据，小时，分钟 | MJXXB.sql |

PS：XH_SH_YEAR.csv和DLH_SSH_JG.csv根据登录号相连接，获取书籍的索书号，可以得知书籍的类型

`awk -F ',' 'NR==FNR{a[$1]=$2;b[$1]=$3}NR!=FNR && a[$3]{print $1","$2","$4","a[$3]","b[$3]}' DLH_SSH_JG.csv XM_XH_SH_YEAR.csv > XM_XH_YEAR_SSH_CJ.csv`

## 5.具体实现

见代码

## 使用命令

| 命令      | 含义         | 扩展                                         |
| --------- | ------------ | -------------------------------------------- |
| file      | 查看文件编码 |                                              |
| awk       | 提取数据     | -F flag                                      |
| head/tail | 查看         | -[N] 查看N行                                 |
| sort      | 排序         | -t [flag] -k [N] 根据flag分割后按照第N个排序 |
| uniq      | 提取独特字段 | -u 去掉相邻行重复数据                        |
| icon      | 转换文件编码 | -f utf-8 -t gbk FILENAME                     |

## 6.遭遇的问题

Q：awk如何实现trim()效果?

A：使用gsub(/regx/,替换字符,变量)

Q：awk如何更改分割符

A：使用OFS=","

Q：如何代码指定Hadoop的input和output

A：

```java
Path baseIn = new Path("hdfs://h1m1:9000/msginput/");
Path baseOu = new Path("hdfs://h1m1:9000/msgoutput/");
Job avgJob = Job.getInstance(conf, avgMapper.class.getSimpleName());
FileInputFormat.setInputPaths(avgJob,
                         Path.mergePaths(baseIn, new Path("/FILENAME.csv")));
		FileOutputFormat.setOutputPath(avgJob,
				Path.mergePaths(baseOu, new Path("/avg/")));
```

Q：如何在mapper内转换文本编码？

A：写静态工具函数进行转换

```java
public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new Text(value);
	}
//使用
String line = transformTextToUTF8((Text)value, "GBK").toString();
```

Q：如何在reduce内获取mapper传入的文件路径？

A：传入map的Context context参数携带着文件信息

```java
//map内部
String filepath = ((FileSplit) context.getInputSplit()).getPath()
					.toString();//获取完整路径
String tag = filepath.split("/")[4];
//tag写入reduce的Context
context.write(new Text(XH_YEAR), new Text(tag + "-" + OTHERS));
```

Q：如何自定义Map的排序方式?

A：使用Entry将Map<type,type>转换为List<Entry<type,type>>

```java
List<Entry<Character, Integer>> list = 
    new ArrayList<Map.Entry<Character, Integer>>(numsMap.entrySet());
Collections.sort(list,
        new Comparator<Map.Entry<Character, Integer>>() 
    {	// 降序排序
        @Override
        public int compare(Entry<Character, Integer> o1, Entry<Character, Integer> o2)
        {return o2.getValue().compareTo(o1.getValue());}
    }
);
```

