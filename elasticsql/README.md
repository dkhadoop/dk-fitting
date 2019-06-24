elasticsearch sql support

elasticsearch version 5.6.8

支持的功能有：
SQL Select 单列 多列 *
SQL Where and <> < <= > >=
SQL Order By asc desc
SQL Between and
SQL Is Null
SQL And Or
SQL In
SQL Like _ ,%(除了左模糊如：%ish)
SQL LOWER UPPER
SQL ABS  CEIL FLOOR
SQL Count(name),count(*)只支持SELECT count(*)  from Product情况
SQL AVG SUM MIN MAX
SQL Distinct
SQL Group By having  group by 时，第一列必须是分组的项，例: SELECT prod_price,COUNT(prod_price) from Products GROUP BY prod_price
SQL Limit 支持limit 3，但不支持limit 3,3情况，可以用OFFSET 1 FETCH NEXT 3 ROW ONLY 代替 ，相当于1,3
SQL


