# Profile Analytics


Profile is a collection of information about one user, which can a real person, a device, or an account. Profile analytics help people with an insight on profiles for a better understanding of their patterns and behaviors. The implementation of profile analytics includes nested data model and PQL (i.e. Profile Query Language). Before we get more into them, it is necessary to describe the system architecture. 


<!--
- Business Insight: all about profiles about user or device
- Nested Data Model
	* Save the information of a profile all together
	* Fully control life cycle 
		- Active v.s. historical
		- Retention
		- GDPR
	* Efficient analytics
		- Minimize JOINs
		- Faster unique profile counting
		- Profile-based operations, like sessionization, behavior analysis etc.
- Profile Query Language: a simple declarative language for better exploration
	* SQL-like Query
		- Simple: a compact derivation of SQL syntax
		- Declarative
		- Explicit table schema
	* Capability of better exploration
	* Why not?
		- SPARK-SQL: still a script or program, no clear table scheme support
		- Hive-SQL: general but probably verbose, following the Hadoop Map-Reduce framework
-->

## Architecture

There are three layers, illustrated in the diagram below. 

* Storage: the bottom layer depicts how the profile are stored. Basically it is a distributed store, which can be sequential, key-value, or both. 
* Data model: it defines the data structure or format of the profile. In other words, it describes how the information is organized given a profile.
* Applications: it provides APIs or query language through which analytics can be proceeded.  

![](imgs/stack.png)

In the post, we focus on the data model and query language. 
	
## Data Model

A profile has a collection of information about a user. Naturally a profile can be defined in the **nested data model**. Particularly, a profile has basic attributes, like ID, creation timestamp etc, more importantly its attribute can be of complex type, like a group or a list of records, etc. The nested data model can be depicted in different schemes, such as ProtoBuf [??], Avro [??] and RecordBuffer [??]. 
Here We use the RecordBuffer as an example to show how an audience profile of the video play activity data can be defined in a nested data model. 

Below a **profile** has four attributes: *profile_id*, *creation_time*, *last_modified_time*, and *plays*, wherein plays represent a nested data structure, i.e., a list of **play**. Each record of **play** depicts a play event, which is sorted by its attribute of *event_time*.
It supports the data evaluation through adding new attributes. 

#### Nested Data Schema: VideoPlayActivityProfile

```json
{
  "schema": "VideoPlayActivity",
  "table_list": [
  	{
      "table": "impression",
      "version": "1",
      "fields": [
        {"id": 1,"name": "id",			"type": "String",		    "isRequired": true}
      ]
    },
    {
      "table": "demographic",
      "version": "1",
      "fields": [
        {"id": 1,"name": "gender",			"type": "String",		    "isRequired": true}
      ]
    },
    {
      "table": "play",
      "version": "1",
      "fields": [
        {"id": 1,"name": "event_time",			"type": "Long",		    "isRequired": true, "isAscSortKey": true},
        {"id": 2,"name": "play_start_pos",			"type": "String",		"isRequired": true},
        {"id": 3,"name": "play_duration",	        "type": "Double",		"isRequired": true},
        {"id": 4,"name": "content_id",		    "type": "Long",		    "isRequired": true},
        {"id": 5,"name": "feed_url",	                "type": "String",		"isRequired": true},
        {"id": 6,"name": "demographics",	        "type": "demographic",		"isRequired": true},
        {"id": 7,"name": "impressions",	        "type": "List:impression",	"isRequired": true}
        {"id": 0,"name": "string_derived_column",     	"type": "String", 	"default": "Unknown", 	"isDerived": true}
      ]
    },
    {
      "table": "profile",
      "version": "1",
      "fields": [
        {"id": 1,	"name": "profile_id",			"type": "String",   	"isRequired": true},
        {"id": 2,   "name": "creation_time",		"type": "Long",		    "isRequired": true},
        {"id": 3,   "name": "last_modified_time",	"type": "Long",		    "isRequired": true},
        {"id": 4,	"name": "plays",                "type": "List:play",  "hasLargeList": true}
      ]
    }
  ]
}

```




## PQL Specification

The PQL (Profile Query Language) is an SQL-like query language. It is declarative, providing a simple way for user to describe her intention directly. Comparing with other SQL-like query languages, the PQL has more compact syntax, focusing on the profile analytics.

### Query Structure

A PQL query has three required clauses (i.e., SELECT, FROM and WHEN) and three optional clauses (e.g., WHERE, HAVING and ORDER BY) in the order as follows: 

* *SELECT*
    * It defines a list of expressions depicting what the user is interested in about the profile;
    * The expression can be an aggregation function, a basic field, or a non-aggregation function; 
    * The list must have at least one aggregation function, like count(*);
    * All non-aggregation expressions are treated as they are group-by columns, thus it is not necessary to use group-by clause in the PQL;
    * It does NOT support ‘*’ expression as it is encouraged to focus on a small set of fields in the analytics.
* *FROM*
    * It defines a list of tables that the user is interested in;
    * The top level table must be in the list, like profile;
    * There are never two nested table at the same level in the list.
* *WHEN*
    * A time range (both ends inclusively) is specified to filter the record based on eventTime;
    * The range can be a pair of dates with ‘[’ and ‘]’, like ‘[2018/01/01, 2018/01/20]’;
    * The range can be ‘*past X days*’, where X is a positive integer for a date range ending at yesterday;
    * The time zone is optional, starting with time_zone or tz and followed with time zone ID or a digital.
* *WHERE* (optional)
    * A predicate can be specified on the non-aggregation columns as a filter of data that user is interested in.
* *HAVING* (optional)
    * A predicate can be specified on the aggregation columns as a filter of output.
* *ORDER BY* (optional)
    * A sequence of columns from the *SELECT* clause to determine the order of output.

An example is below, given the table structure above.

```sql
select dateOf(plays.event_time), 
count(*), 
count(distinct profile_id), 
sum(plays.play_duration)
from profile, profile.plays
when [2017/01/01, 2017/03/01] tz -7
where plays.play_duration > 5
having count(*) > 100000
order by dateOf(plays.event_time)
```

The query would find out the number of play events, the number of unique devices, and the total play durations of events occurring every day between 2017/01/01 and 2017/03/01 inclusively, given the constraints that the play events must be longer than 5 seconds, and the number of play events at that day should be larger than 100k. The results should be sorted by the date string, by default in ascending order. 

Note that all words or keywords in the query are *case-insensitive*. 

### Operators

The PQL provides the following operators in the query.

| Operator                          | Description                                                                                                        | Result Type       | Examples                                                                                                                                                    |
|-----------------------------------|--------------------------------------------------------------------------------------------------------------------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ~, &&, \|\|                         | NOT, AND, OR                                                                                                       | Binary            | NOT id = ‘123’                                                                                                                                              |
| \=,  <>,  !\=,  >,  <,  >\=,  <\= | Boolean operator (equal, not equal, larger than, smaller than,larger than and equal to, smaller than and equal to) | Binary            | id = ‘123’, duration = 45                                                                                                                                   
| +, -, *, /, %                     | Plus, Minus, Multiply, Division and Mod                                                                            | Int, Long, Float  | 1 + 1, 20 / 4                                                                                                                                               |
| IN (…)                            | IN operation                                                                                                       | String, Int, Long | platform in (‘iPhone’, ‘iPod’), start_pos IN (23456, 23457, 23458)                                                                                          |
| like                              | String matching operation                                                                                          | String            | (1) all URL with ‘npr’ in it: feedurl like ‘%npr%’;<br> (2) all URL starting with ‘npr’: feedurl like ‘npr%’;<br>(3) all URL ending with ‘npr’: feed_url like ‘%npr’ |
| +                                 | String concatenation operation                                                                                     | String            | ‘iOS’ + ‘11.0’                                                                                                                                           |
| is null, is not null              | check if the value is null (or not)                                                                                | any column        | ip is not null                                                                                                                                              |


### Functions

The PQL defines a set of functions for a better user experience. Three types of functions are defined, including basic functions, aggregation function, and profile functions. 

#### Basic Functions

The PQL defines a set of temporal transformations from a value of *millisecond* into a calendar representation. 

```
operator (timeInMilliSeconds, timeZone?, pattern?)
```
where 

1. *timeInMilliSeconds* represents a value of millisecond, 
2. *timeZone* is optional, represented as time zone ID or a digital,
3. *pattern* is optional, specifying the format of the output.

Particularly there are six functions in the below table. 

| Operator   | Description                            | Input Type | Result Type |
|------------|----------------------------------------|------------|-------------|
| hourOf     | get the hour (0 - 23)                  | Long       | String      |
| dateOf     | get the date (e.g., 2012/01/01)        | Long       | String      |
| monthOf    | get the moth (1 to 12)                 | Long       | String      |
| yearOf    | get the year (e.g., 2011)               | Long       | String      |
| dayInWeek  | get the day in a week (e.g., Mon, Tue) | Long       | String      |
| weekInYear | get the week num in a year (e.g., 23)  | Long       | String      |

An example is like

```sql
select hourOf(plays.event_time), count(*), count(distinct profile_id)
from profile, profile.plays
when past 10 days
order by hourOf(plays.event_time)
```

Other functions are illustrated in the table below. 

| Operator | Syntax | Description                                                                             | Input Type               | Result Type        |
|----------|--------------------------------------|-----------------------------------------------------------------------------------------|--------------------------|--------------------|
| toLong   | tolong(exp) | cast the value into a long                                                              | Int, Long, Float, Double, String | Long               |
| hash     | hash(exp) | get the hash code of a string                                                           | String                   | Long               |
| bin      | BIN (start, duration, bucket_size) | return a tuple of starting bucket and ending bucket | Long, Int, Float, Double | a pair of integers |
| coalesce   | coalesce(exp1, exp2, ...) | return the first non-NULL expression among its arguments                                                            | Int, Long, Float, Double, String | Long (if all arguments are integers), Double (if all arguments are numeric and at least one is Double), String (at least one argument is String)               |
| concat     | concat(exp1, exp2, ...) | concatenate the input arguments as a string ('null' is applied if the value of an expression doesn't exist                                                         | String                   | String               |
| extStr     | ExtStr(exp1, exp2 [, index]) | extract a substring from a string (i.e., exp1) based on the regular expression (i.e., exp2); the first match returns unless index is specified                                                           | exp1, exp2: String, index: Int                   | String               |
| json_val     | json_val(exp1, exp2) | fetch the value in the JSON (i.e., exp1) of the key determined by a path query (i.e., exp2)                                                          | String                   | String               |
| ts     | ts(dateStr [, timeZone] [, pattern]) | get the value of millisecond of the input dateStr (e.g., 2018/10/10), based on the optional time zone (e.g., UTC) and pattern (e.g., yyyy/MM/dd)                                                          | String                   | Long               |
| substr     | substr(str, startPos [, endPos]) | get the substring of the input (i.e., str) starting from a position (i.e., startPos) and ending at the other (i.e., endPos) if exists                                                           | str: String, startPos, endPos: Int                   | String               |

On the other hand, the PQL also introduces a set of aggregation functions. 

#### Aggregation Functions

| Operator          | Description                                                                                              | Input Type               | Result Type  | Examples                                                 |
|-------------------|----------------------------------------------------------------------------------------------------------|--------------------------|--------------|----------------------------------------------------------|
| min/max           | find the minimum or maximum of column                                                                    | Int, Long, Float, Double | Long, Double | max(plays.play_duration), min(plays.play_start_position) |
| sum               | calculate the sum of columns                                                                             | Long, Int, Float, Double | Long, Double | sum(plays.play_duration)                                 |
| count(\*)          | count the number of events                                                                               | N/A                      | Long         | count(\*)                                                 |
| count(distinct X) | count the number of distinct profiles; note that the column X must identify the profile, e.g., profile_id | String, Long             | Long         | count(distinct profile_id)                                |
<!--
| sum(distinct X) | sum up the number of distinct profiles; note that the column X must identify the profile, e.g., profile_id | String, Long             | Long         | count(distinct profile_id)  
-->

#### Histogram

The PQL introduces a special aggregation functions, to calculate the histogram given a bin function. The syntax is specified below. 

```
hist_count(bin(...), [DISTINCT expression] [',' MIN_FREQ integer])
```

Basically the function uses the BIN function to determine a set of buckets for aggregation. The optional parameters include 

* *expression* calculates a value to be counted distinctively; when absent, the number of events is counted;
* *integer* indicates an integer as the minimal frequency, with the default being 1; a value is considered for counting only if its frequency is not smaller than this threshold.


Below are examples to explain how this function can help us compute a histogram in different senarios. 

##### Count Frequency

```sql
select 
	hist_count(bin(plays.play_start_position, plays.play_duration, 5))
from profile, profile.plays
when [2018/01/01, 2018/01/31]
where plays.content_id = '123'
``` 
The query above tries to find out the distribution of play events from the content (id='123'). The **bin** function (i.e., ```bin(plays.play_start_position, plays.play_duration, 5)```),  detemines the buck size (i.e., 5 seconds) and the buackets covered by a play event based on its start position and duration.  

An example of the query result is a list of numbers, each of which is the number of play events covering the corresponding bucket. 

```
......
(2,2,2,2,2,2,2,2,2,2,...,2,2,2) 
......
```


##### Unique profiles

```sql
select 
	hist_count(bin(plays.play_start_position, plays.play_duration, 5), distinct profile_id)
from profile, profile.plays
when [2018/01/01, 2018/01/31]
where plays.content_id = '123'
``` 

The query above has an optional parameter, (i.e., 'distinct profile_id'). Thus the result of **hist_count** is a list of numbers for unique profiles, describing the distribution of audiences of the content (id='123'). 


##### Frequency constraint

```sql
select 
	hist_count(bin(plays.play_start_position, plays.play_duration, 5), distinct profile_id, min_freq 2)
from profile, profile.plays
when [2018/01/01, 2018/01/31]
where plays.content_id = '123'
``` 

Adding the optional parameter for minimal frequency (i.e., min_freq 2) filters those profiles with only one play events within a bucket. In other words, we only consider those profiles repeating playing along the histogram. 


#### Profile Functions 

The profile function is applied to the nested table in the profile. It can be treated as a UDF, which takes a profile as input and returns a value. The syntax can be described as 

```sql
pf_X (
	[distinct] nesting column 
	[, condition] 
	[, limit integer]
)
```
1. *X* can one of the functions: min, max, sum, or count; the **distinct** keyword is optional and can be applied when *X* is a function of *count*.
2. *nesting column* must be a field with full column name (e.g., profile.plays.content_id), and the host nesteed table (e.g., profile.plays) may not be in the FROM clause.
3. *condition* is optional, specifying the restriction that the function is applied; note that *nesting column* can be used in the condition.
4. *limit* provides a tradeoff between accuracy and cost, indicating the number of events in the profile to consider while evaluating the profile function. If not specified, all events are used for evaluation. 


The **functions** can be summarized in the following table. 

| Operator        | Description           | Input Type | Result Type| Examples |
| ------------- |:-------------:| :--------- | :--------- |:--------- |
| pf\_min/pf\_max    | find the minimum or maximum in the profile  | Int, Long, Float, Double | Long, Double | max(plays.play_duration), min(plays.play\_start\_position) |
| pf_sum      | calculate the sum of columns | Long, Int, Float, Double | Long, Double | sum(plays.play_duration) |
| pf_count    | count the number of events | N/A | Long | count(\*) |
| pf_count(distinct X) | count the number of distinct profiles; note that the column X **must** identify the profile, e.g., profile_id | String, Long | Long | count(distinct profile_id) |

Next, we use **examples** to shed light on the usage.


**Case 1**: Find out the number of duplicates (any plays with the same profile ID and event time are treated as duplicates) in the past 10 days:

```sql
select count(*), count(distinct profile_id)
from profile, profile.plays
when past 10 days
where pf_count(profile.plays.event_time, profile.plays.event_time = plays.event_time) > 1
```

Different from other functions, the profile function is applied to the entire profile to get a value. 

**Case 2**: Find out the number of unique profile consuming more than 10 contents in the past 10 days

```sql
select count(*)
from profile
when past 10 days
where pf_count(distinct profile.plays.content_id) > 10
```

**Case 3**: Find out the distribution of unique profiles based on the number of consumed contents in the past 10 days

```sql
select case when pf_count(distinct profile.plays.content_id) > 100 then '(100, ~)'
when pf_count(distinct profile.plays.content_id) > 10 then '(10, 100]'
when pf_count(distinct profile.plays.content_id) > 5 then '(5, 10]'
else '(0, 5]' end, count(distinct profile_id)
from profile
when past 10 days
```

#### Session Function

Session is a commonly used concept in the Internet world, to measure the user's behavior taken within a period of time or with regard to completion of a task. The session of play activities measures the audience activities while consuming the content. Particularly a session of play activity is determined by content, play start time, duration, given a profile. A new session can be created by a timeout (or long interval), or content changing. 


![](imgs/session-1.png)

![](imgs/session-2.png)


The PQL introduces the session function, to help count the number of sessions in the profile. 


The syntax can be depicted as follows:

```sql
play_session_count (
    nesting column,                             # a column of content
    [start_time_in_ms] nesting column,          # a column of play_start_time in ms 
    [duration_time_in_sec] nesting column       # a column of play_duration in sec
    [, min_duration integer]                    # optional: minimal duration in a valid session
    [, condition]                               # optional: predicates to filter events
    [, timeout integer]                         # optional: time out in ms
    [, limit integer]                           # optional: limit to the number of events to consider per profile for approximation
)
```

An example below is to find the number of sessions in the past 10 days, where a session is composed of play events if:

* they are from the same content, 
* these events occur in sequence continuously, 
* no play events from other contents interrupt the sequence, and 
* any consecutive events in the sequence don't have an interval longer than 1 minute (i.e., 60000 milliseconds).

```
selectsum(play_session_count(
            profile.plays.content_id, 
            profile.plays.play_start_time, 
            profile.plays.play_duration, 
            timeout 60000))
from profile
when past 10 days
```

### Case-When Statement

The Case-When statement evaluates conditions and returns a value when the first condition is satisfied (like an IF-THEN-ELSE statement).

Its syntax is: 

```sql
CASE 
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
   ...
    WHEN conditionN THEN resultN
    ELSE result
END
```

The example below tries to find the listening pattern w.r.t the duration. 

```sql
select case 
		when plays.play_duration >=3600 and plays.play_duration < 7200 then '[3600,7200)'
		when plays.play_duration >=1800 and plays.play_duration < 3600 then '[1800,3600)'
		when plays.play_duration >=600 and plays.play_duration < 1800 then '[600,1800)'
		when plays.play_duration >=300 and plays.play_duration < 600 then '[300,600)'
		when plays.play_duration >= 5 and plays.play_duration < 300 then '[5,300)' 
		when plays.play_duration >= 0 and plays.play_duration < 5 then '[0,5)' 
		else '[7200,~)' end, 
count(*), count(distinct profile_id)
from profile, profile.plays
when past 100 days
where plays.event_time is not null
```

## Effective PQL

### Simple 

### Complex

## Appendix

### PQL Gramma

```sql
grammar PQL;

select_statement :
    SELECT select_list
    FROM table_list
    TIME_WINDOW '[' time_range ']'
    (TIME_ZONE time_zone)?
    (WHERE where=search_condition)?
    (HAVING having=search_condition)?
    (ORDER BY order_list)?
    ;
.......
```
### Query Assistant

To help issue the query in a friendly way, a GUI tool, query assistant, is developed for user to check the table schema, write and run the query, and look into the result. 

![](imgs/query-assistant-1.png)

### PQL Gramma

Check [the gramma definition.]()