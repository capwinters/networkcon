select SUM(COUNTER_1) AS SUM_COUNTER_1, 1.0*sum(COUNTER_3)/sum(COUNTER_2) as PERCENTAGE, CATEGORY_1 from counter_table 
where COUNTER_1 < :FILTER
group by CATEGORY_1
order by DATETIME