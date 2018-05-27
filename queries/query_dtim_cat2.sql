select * from counter_table
where COUNTER_1 > :FILTER_1 and COUNTER_1 <= :FILTER_2
group by DATETIME, CATEGORY_1
order by DATETIME
