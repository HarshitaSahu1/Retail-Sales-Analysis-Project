select * from orders
select * from Customers
select * from OrderPayments
select * from OrderReview_Ratings
select * from ProductsInfo
select * from [Stores Info]
select * from product_info_clean

-- There is no duplication in ProductsInfo
select count(distinct product_id) from ProductsInfo

select count(distinct order_id) from Orders

select * from orders   --- 112650 rows orignally 


with total_payments  as (    ----------------88,627 rows 
select op.order_id,sum(op.payment_value) as total_payment_value from OrderPayments as op
group by op.order_id),

total_amt as (
select o.order_id,sum(o.[Total Amount]) as total_amt_value,max(o.Quantity)as qty from orders_clean as o
group by o.order_id)

select *,total_amt.order_id from total_payments 
join total_amt 
on total_payments.order_id = total_amt.order_id 
where round(total_payment_value,2) <> round(total_amt_value,2)
------------------------------------------------------------------------------------------------------------------------------
----- 10,038 rows mismatch after aggregating 
with total_payments  as (
select op.order_id,sum(op.payment_value) as total_payment_value from OrderPayments as op
group by op.order_id),

total_amt as (
select o.order_id,sum(o.[Total Amount]) as total_amt_value from orders as o
group by o.order_id)

select * from total_payments 
join total_amt 
on total_payments.order_id = total_amt.order_id 
where round(total_payment_value,2)<>round(total_amt_value,2)


---- there is no duplicate records in order 
select count(*),Customer_id,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp,Quantity,mrp,Discount,[Total Amount] from Orders
group by Customer_id,order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp,Quantity,mrp,Discount,[Total Amount]
having count(*)>1

select DISTINCT payment_type from OrderPayments ---- 4 Categories 

select * from ProductsInfo

select product_id from ProductsInfo --- no duplicates 
group by product_id
having count(*)>1 

select count(distinct product_id) from ProductsInfo -- No duplicates

select * from OrderReview_Ratings

with duplicates_ratings_records as ( ---- 700 records are duplicated here in orderreview_ratings 
select order_id,Customer_Satisfaction_Score,count(*)as duplicate_records_count 
from OrderReview_Ratings 
group by order_id,Customer_Satisfaction_Score
having count(*)>1 )

select OrderReview_Ratings.order_id,OrderReview_Ratings.Customer_Satisfaction_Score  from OrderReview_Ratings 
join duplicates_ratings_records as duplicate_rating
on OrderReview_Ratings .order_id = duplicate_rating.order_id
order by order_id asc

select count(distinct Custid) from Customers --- No duplicates


select * from Orders where Quantity <1



select * from orders


select * from OrderPayments
where payment_value = 0

select *from orders
   ---- no. columns with same ids and same qty but diff total_amp and qty
   -----8847 rows are there 
with duplicates as (
select order_id,customer_id,product_id,bill_date_timestamp ,[Cost Per Unit]  ,
mrp,Discount
from orders                                                                                                 
group by 
order_id,customer_id,product_id,bill_date_timestamp,[Cost Per Unit],mrp,Discount
having count(*)>1

)
select o.order_id,o.[Total Amount],o.Quantity,o.product_id,o.Customer_id,o.Delivered_StoreID,o.MRP,o.[Cost Per Unit] from orders as o
join duplicates as d
on o.order_id = d.order_id and o.Bill_date_timestamp = d.Bill_date_timestamp and o.Customer_id = d.Customer_id
and o.product_id = d.product_id and
o.[Cost Per Unit] = d.[Cost Per Unit]
and o.MRP = d.MRP and o.Discount= d.Discount
order by o.order_id



select * from OrderPayments --- 830 rows are there who are available in orders_payment but not available in orders table
left join orders
on OrderPayments.order_id = orders.order_id
where orders.order_id is null

select * from OrderPayments

select * from orders  ----- there are no discrepancy in orders table related to customer as there is no cust which are present in order but not in cust
left join Customers
on orders.Customer_id = Customers.Custid
where Customers.Custid is null

select * from orders
where [Total Amount]<1

select * from [Stores Info] as s  ---- there are no discrepancy in orders table related to store in store_table which are present in order but not in storeid_info
right join orders as o
on s.StoreID = o.Delivered_StoreID
where s.StoreID is null
 

select * from orders
select * from OrderReview_Ratings

select Customer_id,orders.order_id,product_id,Channel,Delivered_StoreID,Bill_date_timestamp,Quantity,[Cost Per Unit],mrp,Discount,--- there are 24,703 rows with different total_amount and payment_value
[Total Amount],payment_type,payment_value 
from orders 
left join OrderPayments
on orders.order_id = OrderPayments.order_id
where round(orders.[Total Amount],2)<>round(cast(OrderPayments.payment_value as decimal(10,2)),2)



---- reduced to 12,685 rows where the total_amount sum is different by payment_value

select round(sum([Total Amount]),2)as total_amt,orders.order_id,round(sum(payment_value),2)as total_payment_value from orders left join OrderPayments
on orders.order_id = OrderPayments.order_id
group by orders.order_id
having round(sum([Total Amount]),2) <> round(sum(payment_value),2)


--- total amt = qty*(mrp-discount)

select * from orders  --- 110389 rows
where [Total Amount] = Quantity*(MRP- Discount)

---- there is no rows as such who not following the correct total_amount
select *,Quantity*(MRP- Discount) from orders 
where ([Total Amount] - Quantity*(MRP- Discount))>0.5


with duplicates_rows as (
select count(*)as cnt,order_id from OrderPayments ---- 2961 duplicates 
group by order_id
having count(*)>1)

select * from OrderPayments
right join duplicates_rows as d
on orderpayments.order_id = d.order_id
order by OrderPayments.order_id


with duplicates_payments as ( ----- 3667 rows are duplicates in orders_payment by order_id and payment_type
select count(*)as cnt,order_id from OrderPayments
group by order_id,payment_type
having count(*)>1)
select OrderPayments.order_id,payment_type,payment_value,cnt from OrderPayments
join duplicates_payments
on OrderPayments.order_id = duplicates_payments.order_id
order by OrderPayments.order_id


select distinct payment_type from OrderPayments --------- 4 categories

select * from ( ------------------------ there are 3 records where there are payment_value is null and also not available in orders table
select order_id,sum(case when payment_type = 'credit_card' then coalesce( payment_value,0) else 0 end)as Credit_Card,
sum(case when payment_type = 'UPI/Cash' then coalesce(payment_value,0) else 0 end)as UPI_Cash,
sum(case when payment_type = 'debit_card' then coalesce(payment_value,0) else 0 end)as Debit_Card,
sum(case when payment_type = 'voucher' then coalesce(payment_value,0) else 0 end)as Voucher
from OrderPayments 
group by order_id) as a 
where Credit_Card=0 and UPI_Cash=0 and Debit_Card=0 and Voucher = 0


select * from Orders
where order_id in( '00b1cb0320190ca0daa2c88b35206009','4637ca194b6387e2d538dc89b124b0ee','c8c528189310eaa44a745b8d9d26908b')

---------------------------------------------- Removing all the 3 records where all the payment_types are null-------------99,437 records 
select *
into orders_payment_clean
from ( 
select order_id,sum(case when payment_type = 'credit_card' then coalesce(payment_value,0) else 0 end)as Credit_Card,
sum(case when payment_type = 'UPI/Cash' then coalesce(payment_value,0) else 0 end)as UPI_Cash,
sum(case when payment_type = 'debit_card' then coalesce(payment_value,0) else 0 end)as Debit_Card,
sum(case when payment_type = 'voucher' then coalesce(payment_value,0) else 0 end)as Voucher,
sum(payment_value)as total_payment_value
from OrderPayments 
group by order_id) as a 
where total_payment_value !=0

----------------------------------------------- Cleaning Bill_Date_Timestamp--------------------------------------

select * into orders_clean from orders 

UPDATE orders_clean
SET Bill_date_timestamp =
    COALESCE(
        -- Attempt Try converting using Style 101 (mm/dd/yyyy)
        -- This covers the format: 5/16/2023 20:57
        TRY_CONVERT(DATETIME, Bill_date_timestamp, 101),
        
        --Try converting using Style 121 (yyyy-mm-dd hh:mi:ss.mmm)
        -- This covers the format: 2022-09-10 18:14:00
        TRY_CONVERT(DATETIME, Bill_date_timestamp, 121)
        -- COALESCE closes here, having two valid arguments.
    );

--- permanently updated the column of date 


---------------------------------------------------------------------- there are 9,815 records having difference between order_value and total_amt---------

with agg_total_amt as(
select order_id,round(sum([Total Amount]),2)as total_amt from orders_clean
group by order_id)
,
total_pymt_value as (
select order_id,round(sum(total_payment_value),2) as total_pmt_value from orders_payment_clean
group by order_id)

select a.order_id ,(a.total_amt -t.total_pmt_value) as diff from  agg_total_amt as a
join total_pymt_value as t
on a.order_id = t.order_id
where (a.total_amt-t.total_pmt_value) >1
;
-----------------------------------------------------------Now there are 1,276 rows only where there is the mismatch after considering cumulative qty----------

with remove_cumulative as (
select *,ROW_NUMBER()over(partition by order_id ,product_id,customer_id order by quantity desc)as cumulative_qty
from orders_clean),

agg_total_amt as(
select order_id,round(sum([Total Amount]),2)as total_amt from remove_cumulative
where cumulative_qty = 1
group by order_id,product_id,Customer_id)
,
total_pymt_value as (
select order_id,round(sum(total_payment_value),2) as total_pmt_value from orders_payment_clean
group by order_id)

select a.order_id ,(a.total_amt -t.total_pmt_value) as diff from  agg_total_amt as a
join total_pymt_value as t
on a.order_id = t.order_id
where (a.total_amt-t.total_pmt_value) >1
; 

------------------------------ remove those rows here 
;with remove_cumulative as (select *,ROW_NUMBER()over ------- 10,225 rows 
(partition by order_id,product_id order by Quantity desc)as cumulative_qty
from orders_clean)

delete from remove_cumulative where cumulative_qty>1

select * from orders_payment_clean
left join orders_clean
on orders_payment_clean.order_id = orders_clean.order_id
where orders_clean.order_id is null

delete orders_payment_clean where order_id not in (select order_id from orders_clean) ----- 772 rows affected 



select * from orders_clean
-----------------------------------------------------------------------
;with delivered_rows as ( --------------------------------------6270 rows affected 
select order_id ,Delivered_StoreID,[Total Amount],ROW_NUMBER()over(partition by order_id order by [Total Amount] desc)as row_
from orders_clean
where Channel = 'Instore'),

row_greater as (
select order_id,Delivered_StoreID from delivered_rows where row_=1
 )
update orders_clean
set orders_clean.Delivered_StoreID = row_greater.Delivered_StoreID
from orders_clean
join delivered_rows
on orders_clean.order_id = delivered_rows.order_id 
join row_greater
on orders_clean.order_id = row_greater.order_id
where delivered_rows.row_>1
and orders_clean.Channel = 'Instore'


-----------------------------------------------One order is from different dates or timings if the channel is instore-----

select * from( ---------------- 2,641 records are such 
select count(*)as count_,Bill_date_timestamp,order_id from orders_clean
where Channel = 'Instore'
group by Bill_date_timestamp,order_id)as a where count_>1

;with duplicate_orders_timings as( ------------------------- 6270 rows affected 
select count(*)as count_,min(Bill_date_timestamp)as bill_date_timestamp,order_id from orders_clean
where Channel = 'Instore'
group by order_id
having count(*)>1)

update orders_clean
set orders_clean.Bill_date_timestamp = duplicate_orders_timings.bill_date_timestamp
from orders_clean
join duplicate_orders_timings
on orders_clean.order_id = duplicate_orders_timings.order_id 
where channel = 'Instore' and count_>1

-------------------------------Single order mapped to multiple customers----------------

select count(*),Customer_id from orders_clean ---- 3269 records 
group by Customer_id
having count(*)>1

;with customer_duplicated_orders as(  -------------------6995 rows affected 
select order_id,customer_id ,ROW_NUMBER()over(partition by order_id order by [Total Amount] desc)as row_no from orders_clean
),

row_top as (
select order_id,Customer_id from customer_duplicated_orders
where row_no=1)

update orders_clean
set orders_clean.Customer_id = row_top.Customer_id 
from orders_clean 
join row_top
on orders_clean.order_id = row_top.order_id
join customer_duplicated_orders
on orders_clean.order_id = customer_duplicated_orders.order_id
where row_no>1
-----------------------------------------------------------------------mismatch btw total_amt and total_payment -------------
with total_payments  as (    ----------------3,252 rows 
select op.order_id,sum(op.total_payment_value) as total_payment_value from orders_payment_clean as op
group by op.order_id),

total_amt as (
select o.order_id,sum(o.[Total Amount]) as total_amt_value,max(o.Quantity)as qty from orders_clean as o
group by o.order_id)

--select avg(diff) from ( ----------------- there is avg difference of 62
select total_amt.order_id,sum(total_amt.total_amt_value)-sum(total_payments.total_payment_value)as diff from total_payments 
join total_amt 
on total_payments.order_id = total_amt.order_id 
where (total_amt_value - total_payment_value)>1
group by total_amt.order_id
having round(sum(total_payment_value),2) <> round(sum(total_amt_value),2)
--)as a

---------------------------------------------------------------Cleaning Store_Info_Table-----------------------------------
select StoreID from [Stores Info]
group by StoreID
having count(*)>1


select distinct * into store_info_clean from [Stores Info] ------- 534 rows 

select * from [Stores Info]
where [Stores Info].StoreID = 'ST410'

---------------------------------------------------------------Cleaning product_Info_Table----------------------------

select * from ProductsInfo --- 623 rows having null categories and respective product_name_length and product_description and product_photos_qty
where Category = '#N/A'

update ProductsInfo ------623 rows affected
set Category = 'Others'
where Category = '#N/A'

update ProductsInfo  ---------------- 32951 rows affected 
set product_name_lenght = coalesce(product_name_lenght,0),
product_description_lenght = coalesce(product_description_lenght,0),
product_photos_qty = coalesce(product_photos_qty,0),
product_weight_g = coalesce(product_weight_g,0),
product_height_cm = coalesce(product_height_cm,0),
product_width_cm = coalesce(product_width_cm,0)

select product_id , count(*)from ProductsInfo ---------- no duplication
group by product_id
having count(*)>1

select product_id , Category into product_info_clean from ProductsInfo ----------32951 rows

-----------------------------------------------------Orders_Review_Ratings ------------------------------------

select * from OrderReview_Ratings --- 100000 orignal records 

select order_id,avg(customer_Satisfaction_Score)as avg_satisfaction_score into order_review_clean from OrderReview_Ratings ----- 99441 rows affected 
group by order_id
order by avg_satisfaction_score 

select product_id, Category into product_info_clean from ProductsInfo

delete from orders_clean -----------------------3 records get deleted 
WHERE CAST(Bill_date_timestamp AS DATETIME) 
      not BETWEEN '2021-09-01' AND '2023-10-31';

select * from orders_clean ---- 1,02,422 rows
select * from orders_payment_clean---- 99,437 rows
select * from store_info_clean --- 534 rows 
select * from product_info_clean --- 32,951 rows
select * from order_review_clean ------ 99,441 rows 
select * from Customers--- 99,441 rows 

------------------------------------------------------------------Created a Aggregating_Table --------------------------------------------

select distinct order_id from orders_clean
select * from orders_clean
select * from orders_payment_clean
select distinct channel from orders_clean
------ 98663 rows 
--------------------------------------------------------------------------- Orders_360 ------------------------------------------

with days_since_last_orders as(
select order_id,
datediff(day,
max(cast(bill_date_timestamp as datetime)),
(select max(cast(bill_date_timestamp as datetime)) from orders)) as days_since_last_orders
from orders_clean
group by order_id)

SELECT orders_clean.order_id,
 sum([Cost Per Unit]*Quantity) as Total_Cost,
count(Category)as no_categories_per_order,
max(Category) as Category,
Max(Delivered_StoreID) as Store_ids,
avg(Quantity) as avg_Basket_size,
sum(Discount) as Total_Discount,
sum([Total Amount])-sum([Cost Per Unit]*Quantity) as Total_Profit, 
(sum(Discount)*100)/sum([Total Amount]) as discount_percent_per_order,
round((sum([Total Amount])- sum([Cost Per Unit]*Quantity))/sum([Total Amount])*100,2) as Profit_percent_per_order,
sum(orders_clean.[Total Amount]) as Total_Revenue ,
count(case when datepart(weekday,Bill_date_timestamp)in (1,7)then 1 end) as  'Weekend',
count(case when datepart(weekday,Bill_date_timestamp)not in (1,7)then 1 end) as  'Weekdays',
sum(Quantity) as Total_Qty,
max(days_since_last_orders)as last_order_day,
case when sum(days_since_last_orders)>=90 then 'Churn_Customer' else 'Active_Customers' end as 'Churn/Active',
count(orders_clean.customer_id) as Count_customers,
count(case when DATEPART(hour,Bill_date_timestamp)between 0 and 11 then 1 end) as Morning,
count(case when DATEPART(hour,Bill_date_timestamp)between 12 and 17 then 1 end) as  Afternoon,
count(case when DATEPART(hour,Bill_date_timestamp)between 18 and 21 then 1 end) as Evening,
count(case when DATEPART(hour,Bill_date_timestamp)>21 then 1 end) as Night ,
sum(Credit_Card) as Credit_Card, sum(UPI_Cash) as UPI_Cash, sum(Debit_Card) as Debit_Card,
sum(Voucher) as Voucher,
avg(avg_satisfaction_score)as average_satisfaction_score,
max(datepart(QUARTER,cast(bill_date_timestamp as datetime))) as Quarters,
count(case when channel = 'Online' then 1 end) as Online_store,
count(case when channel = 'Phone Delivery' then 1 end) as Phone_Delivery,
count(case when channel = 'Instore' then 1 end) as Instore,
case 
when count(case when credit_card>0 then 1 end)>=count(case when debit_card>0 then 1 end) and 
count(case when credit_card>0 then 1 end)>=count(case when upi_cash>0 then 1 end)
and count(case when credit_card>0 then 1 end)>=count(case when voucher>0 then 1 end) then 'Credit_card' 
when  count(case when debit_card>0 then 1 end)>=count(case when credit_card >0 then 1 end) and 
count(case when debit_card>0 then 1 end)>=count(case when upi_cash >0 then 1 end)
and count(case when debit_card>0 then 1 end)>=count(case when voucher >0 then 1 end)
then 'Debit_card' 
when count(case when voucher>0 then 1 end)>=count(case when credit_card>0 then 1 end) and 
count(case when voucher>0 then 1 end)>=count(case when upi_cash>0 then 1 end) 
and count(case when voucher>0 then 1 end)>=count(case when debit_card>0 then 1 end)
then 'Voucher' 
else 'UPI/CASH' end as prefered_payments,
sum(case when product_info_clean.category = 'Auto' then [Total Amount] else 0 end  )as 'Auto_Category_Revenue',
sum(case when product_info_clean.category = 'Baby' then [Total Amount] else 0  end )as 'Baby_Category_Revenue',
sum(case when product_info_clean.category = 'Computers & Accessories' then [Total Amount] else 0  end )as 'Computers & Accessories_Category_Revenue',
sum(case when product_info_clean.category = 'Construction_Tools' then [Total Amount] else 0 end )as 'Construction_Tools_Category_Revenue',
sum(case when product_info_clean.category = 'Electronics' then [Total Amount] else 0 end )as 'Electronics_Category_Revenue',
sum(case when product_info_clean.category = 'Fashion' then [Total Amount] else 0 end )as 'Fashion_Category_Revenue',
sum(case when product_info_clean.category = 'Food & Beverages' then [Total Amount] else 0  end )as 'Food & Beverages_Category_Revenue',
sum(case when product_info_clean.category = 'Luggage_Accessories' then [Total Amount] else 0 end )as 'Luggage_Accessories_Category_Revenue',
sum(case when product_info_clean.category = 'Others' then [Total Amount] else 0 end )as 'Others_Category_Revenue',
sum(case when product_info_clean.category = 'Furniture' then [Total Amount] else 0 end )as 'Furniture_Category_Revenue',
sum(case when product_info_clean.category = 'Pet_Shop' then [Total Amount] else 0 end )as 'Pet_Shop_Category_Revenue',
sum(case when product_info_clean.category = 'Stationery' then [Total Amount] else 0 end )as 'Stationery_Category_Revenue',
sum(case when product_info_clean.category = 'Toys & Gifts' then [Total Amount] else 0  end )as 'Toys & Gifts_Category_Revenue',
sum(case when product_info_clean.category = 'Home_Appliances' then [Total Amount] else 0 end )as 'Home_Appliances_Category_Revenue',
count(case when Channel = 'Instore' then 1 end) as Instore_Channel,
count(case when Channel = 'Phone Delivery' then 1 end) as Phone_Delivery_Channel,
count(case when Channel = 'Online' then 1 end) as Online_Channel
into orders_360
from orders_clean
left join days_since_last_orders
on orders_clean.order_id = days_since_last_orders.order_id
left join product_info_clean
on orders_clean.product_id = product_info_clean.product_id
left join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
left join store_info_clean 
on orders_clean.Delivered_StoreID= store_info_clean.StoreID
left join Customers
on orders_clean.Customer_id = Customers.Custid
left join orders_payment_clean
on orders_clean.order_id = orders_payment_clean.order_id
group by orders_clean.order_id


-------------------------------------------------------------------------Customer_360_Table ------------------------------

select count(distinct customer_id) from orders_clean ---- 98572 records 


with discount_Non_discount_seeker as (
select Customer_id  from(
select Customer_id,count(
case when Discount > 0 then 1 else 0 end ) as discount_taken from orders_clean
group by Customer_id) as a
where discount_taken>2
),

categories_purchased as (
select count(Category) as Total_Categories,orders_clean.product_id,Category
from orders_clean 
left join product_info_clean
on orders_clean.product_id = product_info_clean.product_id 
group by orders_clean.product_id,Category
)

---select sum(Total_spend) as total_amt from(
select orders_clean.Customer_id,
max(customer_state) as cust_states,
max(customer_city) as cust_city,
sum([Total Amount]) as Total_spend,
datediff(day,max(cast(Bill_date_timestamp as datetime)),
(select max(cast(bill_date_timestamp as datetime)) from orders_clean))as Inactive_days
,count(distinct orders_clean.order_id) as distinct_transactions,
sum([Total Amount])-sum([Cost Per Unit]*Quantity) as Profit ,sum(discount)as total_discount,
sum(Quantity)as total_qty,
max(Category) as category,
case when sum([Total Amount])<= PERCENTILE_CONT(0.33)within group (order by sum([Total Amount]))
over() then 'Low'
when sum([Total Amount])<= PERCENTILE_CONT(0.66)within group (order by sum([Total Amount])) over() then 'Medium'
else 'High'
end as 'Customer_level',
datediff(day,max(cast(bill_date_timestamp as datetime)),
(select max(cast(bill_date_timestamp as datetime)) from orders_clean)) as Recency ,
count(orders_clean.order_id) as frequency,sum([Total Amount]) as Monetary,
case when (datediff(day,max(cast(bill_date_timestamp as datetime)),
(select max(cast(bill_date_timestamp as datetime)) from orders_clean))+count(orders_clean.order_id)+sum([Total Amount]))
>=7000 then 'Premium_Cust'
when (datediff(day,max(cast(bill_date_timestamp as datetime)),
(select max(cast(bill_date_timestamp as datetime)) from orders_clean))+count(orders_clean.order_id)+sum([Total Amount]))>=5000 then 'Gold_Cust'
when (datediff(day,max(cast(bill_date_timestamp as datetime)),
(select max(cast(bill_date_timestamp as datetime)) from orders_clean))+count(orders_clean.order_id)+sum([Total Amount])) >=3000 then 'Silver_Cust'
else 'Standard_Cust' end as Customer_Segment,
(case when max(discount_Non_discount_seeker.Customer_id) = orders_clean.Customer_id then 'Discount_Seeker'
else ' Non_Discount_Seeker' end) as discount_type,
case when min(cast(bill_date_timestamp as datetime))<> max(cast(bill_date_timestamp as datetime)) then 'Repeated_Customer' else 'One_Time_Customer' end as 'Repeated/One_time_Cust',
min(cast(bill_date_timestamp as datetime)) as First_transaction_date,
max(cast(bill_date_timestamp as datetime)) as Last_transaction_date,sum(Total_Categories) as Total_Categories
,string_agg(DATEPART(hour,Bill_date_timestamp),',')as hour_of_day,
count(case when datepart(hour,Bill_date_timestamp)between 0 and 5 then 1 end) as late_Night_orders ,
count(case when datepart(hour,Bill_date_timestamp)between  6 and 11 then 1 end) as Morning_orders,
count(case when datepart(hour,Bill_date_timestamp)between  12 and 17 then 1 end) as Afternoon_orders,
count(case when datepart(hour,Bill_date_timestamp)between 18 and 21 then 1 end) as Evening_orders,
count(case when datepart(hour,Bill_date_timestamp)>21 then 1 end) as Night_orders
,
count(distinct Channel)as distinct_channel,
count(distinct Delivered_StoreID)as  count_store_ids,
string_agg(Delivered_StoreID,',') as store_ids,
case 
when count(case when credit_card>0 then 1 end)>=count(case when debit_card>0 then 1 end) and 
count(case when credit_card>0 then 1 end)>=count(case when upi_cash>0 then 1 end)
and count(case when credit_card>0 then 1 end)>=count(case when voucher>0 then 1 end) then 'Credit_card' 
when  count(case when debit_card>0 then 1 end)>=count(case when credit_card >0 then 1 end) and 
count(case when debit_card>0 then 1 end)>=count(case when upi_cash >0 then 1 end)
and count(case when debit_card>0 then 1 end)>=count(case when voucher >0 then 1 end)
then 'Debit_card' 
when count(case when voucher>0 then 1 end)>=count(case when credit_card>0 then 1 end) and 
count(case when voucher>0 then 1 end)>=count(case when upi_cash>0 then 1 end) 
and count(case when voucher>0 then 1 end)>=count(case when debit_card>0 then 1 end)
then 'Voucher' 
else 'UPI/CASH' end as prefered_payments,
count(case when datepart(weekday,Bill_date_timestamp)in (1,7) then 1 end) as Weekends,
count(case when datepart(weekday,Bill_date_timestamp)not in (1,7) then 1 end) as Weekdays
,sum(case when category = 'Auto' then [Total Amount] else 0 end )as 'Auto_Category',
sum(case when category = 'Baby' then [Total Amount] else 0 end )as 'Baby_Category',
sum(case when category = 'Computers & Accessories' then [Total Amount] else 0 end )as 'Computers & Accessories_Category',
sum(case when category = 'Construction_Tools' then [Total Amount] else 0 end )as 'Construction_Tools_Category',
sum(case when category = 'Electronics' then [Total Amount] else 0 end )as 'Electronics_Category',
sum(case when category = 'Fashion' then [Total Amount] else 0 end )as 'Fashion_Category',
sum(case when category = 'Food & Beverages' then [Total Amount] else 0 end )as 'Food & Beverages_Category',
sum(case when category = 'Furniture' then [Total Amount] else 0 end )as 'Furniture_Category',
sum(case when category = 'Home_Appliances' then [Total Amount] else 0 end )as 'Home_Appliances_Category',
sum(case when category = 'Luggage_Accessories' then [Total Amount] else 0 end )as 'Luggage_Accessories_Category',
sum(case when category = 'Others' then [Total Amount] else 0 end )as 'Others_Category',
sum(case when category = 'Pet_Shop' then [Total Amount] else 0 end )as 'Pet_Shop_Category',
sum(case when category = 'Stationery' then [Total Amount] else 0 end )as 'Stationery_Category',
sum(case when category = 'Toys & Gifts' then [Total Amount] else 0 end )as 'Toys & Gifts_Category'
into customer_360
from orders_clean
left join discount_Non_discount_seeker
on orders_clean.Customer_id= discount_Non_discount_seeker.Customer_id
left join Customers
on orders_clean.Customer_id = Customers.Custid
left join categories_purchased
on orders_clean.product_id = categories_purchased.product_id
left join orders_payment_clean
on orders_clean.order_id = orders_payment_clean.order_id
left join store_info_clean
on orders_clean.Delivered_StoreID = store_info_clean.StoreID
group by orders_clean.Customer_id

-------------------------------------------------------------------Store_360----------------------------------------------

select * from Customers
select * from orders_payment_clean
select * from orders_clean
select * from product_info_clean
select * from store_info_clean
select * from order_review_clean

with weekend_weekdays as(
select Delivered_StoreID,sum(case when Date_Type = 'WEEKDAY' then 1 else 0 end) as Weekday_counts,
sum(case when Date_Type = 'WEEKEND' then 1 else 0 end) as Weekend_counts from(
select Delivered_StoreID
,case when datepart(WEEKDAY,cast(bill_date_timestamp as datetime)) in (1,7) then 'WEEKEND' else 'WEEKDAY' end as 'Date_Type'
 from orders_clean 
group by Delivered_StoreID,bill_date_timestamp) as a
group by Delivered_StoreID),

------ Online,Phone_Delivery,Instore
channels as(
select Delivered_StoreId,sum(case when Channel = 'Instore' then 1 else 0 end) as Instore_order,
sum(case when Channel = 'Phone Delivery' then 1 else 0 end) as Phone_Delivery_order,
sum(case when Channel = 'Online' then 1 else 0 end) as Online_order
from orders_clean
group by Delivered_StoreId),

top_5_categories as(
select string_agg(categories,',') as Categories ,Delivered_StoreID from(
select category as categories,sum([Total Amount]) as total_amt,ROW_NUMBER() over(partition by Delivered_StoreID order by sum([Total Amount]) desc) as row_no
, Delivered_StoreID
from orders 
left join product_info_clean
on orders.product_id = product_info_clean.product_id
group by Delivered_StoreID,category
) 
as a
where row_no<=5
group by Delivered_StoreID),

time_bin as(
select delivered_storeid,
sum(case when TIME_Bin = 'Morning' then 1 else 0 end) as Morning_counts,
sum(case when TIME_Bin = 'Afternoon' then 1 else 0 end) as Afternoon_counts,
sum(case when TIME_Bin = 'Evening' then 1 else 0 end) as Evening_counts,
sum(case when TIME_Bin = 'Night' then 1 else 0 end) as Night_counts from(
select delivered_storeid,
case when DATEPART(hour,Bill_date_timestamp)between 0 and 11 then 'Morning'
when DATEPART(hour,Bill_date_timestamp)between 12 and 17 then 'Afternoon'
when DATEPART(hour,Bill_date_timestamp)between 18 and 21 then 'Evening'
else 'Night' end  as TIME_Bin
from orders_clean
group by delivered_storeid,Bill_date_timestamp) as a
group by delivered_storeid
),

sales_by_month as(
select a.delivered_Storeid,
sum(January_Sales) as January_Sales,sum(Feburary_Sales) as Feburary_Sales,sum(March_Sales) as March_Sales,
 sum(April_Sales) as April_Sales, sum(May_Sales) as May_Sales, sum(June_Sales) as June_Sales,
 sum(July_Sales) as July_Sales, sum(August_Sales) as August_Sales, sum(Sept_Sales) as Sept_Sales
 ,sum(October_Sales)as October_Sales 
 ,sum(November_Sales) as November_Sales
 , sum(December_Sales) as December_Sales from(
 select Delivered_StoreID,
(case when datepart(month,Bill_date_timestamp) in (1) then sum([Total Amount]) else 0 end) as 'January_Sales',
(case when datepart(month,Bill_date_timestamp) in (2) then sum([Total Amount]) else 0 end) as 'Feburary_Sales',
(case when datepart(month,Bill_date_timestamp) in (3) then sum([Total Amount]) else 0 end) as 'March_Sales',
(case when datepart(month,Bill_date_timestamp) in (4) then sum([Total Amount]) else 0 end) as 'April_Sales',
(case when datepart(month,Bill_date_timestamp) in (5) then sum([Total Amount]) else 0 end) as 'May_Sales',
(case when datepart(month,Bill_date_timestamp) in (6) then sum([Total Amount]) else 0 end) as 'June_Sales',
(case when datepart(month,Bill_date_timestamp) in (7) then sum([Total Amount]) else 0 end) as 'July_Sales',
(case when datepart(month,Bill_date_timestamp) in (8) then sum([Total Amount]) else 0 end) as 'August_Sales',
(case when datepart(month,Bill_date_timestamp) in (9) then sum([Total Amount]) else 0 end) as 'Sept_Sales',
(case when datepart(month,Bill_date_timestamp) in (10) then sum([Total Amount]) else 0 end) as 'October_Sales',
(case when datepart(month,Bill_date_timestamp) in (11) then sum([Total Amount]) else 0 end) as 'November_Sales',
(case when datepart(month,Bill_date_timestamp) in (12) then sum([Total Amount]) else 0 end) as 'December_Sales'
--sum([Total Amount]) over (partition by datepart(month,Bill_date_timestamp))
,datepart(month,cast(Bill_date_timestamp as datetime)) as months,datepart(Year,cast(Bill_date_timestamp as datetime)) as years
from orders_clean
group by Delivered_StoreID,Bill_date_timestamp) as a
group by a.delivered_Storeid),

new_customer_analysis as(
select Delivered_StoreID,
round(sum(case when datepart(month,first_orders) in (1) then 1 else 0 end),2)as Jan_new_cust_count,
round(sum(case when datepart(month,first_orders) in (2) then 1 else 0 end),2)as Feb_new_cust_count,
round(sum(case when datepart(month,first_orders) in (3) then 1 else 0 end),2)as Mar_new_cust_count,
round(sum(case when datepart(month,first_orders) in (4) then 1 else 0 end),2)as Apr_new_cust_count,
round(sum(case when datepart(month,first_orders) in (5) then 1 else 0 end),2)as May_new_cust_count,
round(sum(case when datepart(month,first_orders) in (6) then 1 else 0 end),2)as June_new_cust_count,
round(sum(case when datepart(month,first_orders) in (7) then 1 else 0 end),2)as July_new_cust_count,
round(sum(case when datepart(month,first_orders) in (8) then 1 else 0 end),2)as Aug_new_cust_count,
round(sum(case when datepart(month,first_orders) in (9) then 1 else 0 end),2)as Sep_new_cust_count,
round(sum(case when datepart(month,first_orders) in (10) then 1 else 0 end),2) as Oct_new_cust_count,
round(sum(case when datepart(month,first_orders) in (11) then 1 else 0 end),2) as Nov_new_cust_count,
round(sum(case when datepart(month,first_orders) in (12) then 1 else 0 end),2) as Dec_new_cust_count
from (																	 
select 
min(cast(Bill_date_timestamp as datetime)) as first_orders,count(*) as new_cust,
Customer_id,Delivered_StoreID
from orders
group by Customer_id,Delivered_StoreID) as a
group by Delivered_StoreID)

select sum(total_Revenue) as total from(
 select orders_clean.delivered_storeid,seller_city,seller_state ,max(Categories) as top_5_categories,
 round(count(distinct orders_clean.order_id),2) as total_orders,
 round(sum([Total Amount])/count(distinct orders_clean.order_id),2) as avg_order_value, round(sum([Total Amount]) - sum([Cost Per Unit]*Quantity),2) as Profit,
 round(sum(cast(Weekday_counts as bigint)),2) as Weekday_orders , 
 round(sum(cast(Weekend_counts as bigint)),2) as weekend_orders,round(sum(Quantity)/ count(distinct orders_clean.order_id),2) as avg_qty_order,
 round(sum(Discount)/count(distinct orders_clean.order_id),2) as avg_Discount_per_order ,round(((sum([Total Amount]) - sum([Cost Per Unit]*Quantity)) / sum([Total Amount]))*100,2) as profit_margin,
 round(sum(Instore_order),2) as Instore_orders,round(sum(Phone_Delivery_order),2) as Phone_Delivery_orders,round(sum(Online_order),2) as Online_orders,
 round(sum(Quantity),2) as total_qty
 ,round(sum([Cost Per Unit]*Quantity-Discount),2) as Total_cost,round(sum([Total Amount]),2) as total_Revenue
,round(sum(Morning_counts),2) as Morning_counts,
 round(sum(Afternoon_counts),2) as Afternoon_counts,round(sum(Evening_counts),2) as Evening_counts,round(sum(Night_counts),2) as Night_counts
 ,round(sum(discount),2) as total_distount,round(count(orders_clean.Customer_id),2) as count_customer_vists
 ,round(sum(January_Sales),2) as January_Sales,round(sum(Feburary_Sales),2) as Feburary_Sales,round(sum(March_Sales),2) as March_Sales,
 round(sum(April_Sales),2) as April_Sales, round(sum(May_Sales),2) as May_Sales, round(sum(June_Sales),2) as June_Sales,
 round(sum(July_Sales),2) as July_Sales, round(sum(August_Sales),2) as August_Sales, round(sum(Sept_Sales),2) as Sept_Sales,round(sum(October_Sales),2)as October_Sales ,round(sum(November_Sales),2) as November_Sales
 , round(sum(December_Sales),2) as December_Sales,
round(sum(Jan_new_cust_count),2) as Jan_new_cust_count,round(sum(Feb_new_cust_count),2) as Feb_new_cust_count,
round(sum(Mar_new_cust_count),2) as Mar_new_cust_count, round(sum(Apr_new_cust_count),2) as Apr_new_cust_count,
round(sum(May_new_cust_count),2) as May_new_cust_count,round(sum(June_new_cust_count),2) as June_new_cust_count,
round(sum(July_new_cust_count),2) as July_new_cust_count,round(sum(Aug_new_cust_count),2) as Aug_new_cust_count,
round(sum(Sep_new_cust_count),2) as Sep_new_cust_count,round(sum(Oct_new_cust_count),2) as Oct_new_cust_count,round(sum(Nov_new_cust_count),2) as Nov_new_cust_count,
round(sum(Dec_new_cust_count),2) as Dec_new_cust_count
--into Store_360
 from orders_clean 
left join store_info_clean
on orders_clean.Delivered_StoreID = store_info_clean.StoreID
left join weekend_weekdays
on orders_clean.Delivered_StoreID = weekend_weekdays.Delivered_StoreID
left join channels
on orders_clean.Delivered_StoreID = channels.Delivered_StoreId
left join top_5_categories
on orders_clean.Delivered_StoreID = top_5_categories.Delivered_StoreID
left join time_bin
on orders_clean.Delivered_StoreID = time_bin.Delivered_StoreID
left join sales_by_month
on orders_clean.Delivered_StoreID = sales_by_month.Delivered_StoreID
left join orders_payment_clean
on orders_clean.order_id = orders_payment_clean.order_id
left join product_info_clean
on orders_clean.product_id = product_info_clean.product_id
left join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
left join Customers
on orders_clean.Customer_id = Customers.Custid
left join new_customer_analysis
on orders_clean.Delivered_StoreID = new_customer_analysis.Delivered_StoreID
group by orders_clean.delivered_storeid,seller_city,seller_state
--order by Delivered_StoreID desc
) as a

select top 5 * from orders_360_clean
select sum(Total_Revenue)/count(order_id) from orders_360_clean
select count(distinct order_id)/ count(distinct customer_id)from orders_clean

select Region,sum([Total Amount]) as total_amt from store_info_clean
join orders_clean
on store_info_clean.StoreID = orders_clean.Delivered_StoreID
group by Region 
order by total_amt desc

select distinct(categories) from Customer_360

select (sum(Discount)/count(Distinct order_id)) from orders_clean
select (sum(distinct order_id)/count(distinct Delivered_StoreID)) from orders_clean

select (sum(Total_Profit)*100/sum(Monetary)) from orders_360

 select (sum(Monetary)/count(Order_id))*100.00/sum(Monetary) from orders_360


 select * from orders_360_clean


select (sum(Total_Discount)*100/sum(Monetary)) from orders_360
select sum(Total_Profit) from orders_360
select count(distinct order_id)/count(distinct customer_id) from orders_clean
----------------------------------------------------------EDA -----------------------------------------------------
-------------------------------------------------------------------------------------------------------------------

 --------------------------------------------------Orders-------------------------------------------------
 select sum(total_Revenue) from orders_360_clean
 select sum([Total Amount]) from orders_clean

------------------------------------------------------High_level_Metrics-----------------------------------------
---------------------------------------------------------------------------------------------------------------

 select count(Order_id) as Total_orders,round(sum(Total_Revenue),2) as Total_Revenue,
 round(sum(Total_Cost),2)as Total_Cost, round(sum(Total_Qty),2) as Total_Qty,
 round(sum(Total_Profit),2)as Total_Profit,round(avg(Total_discount),2) as Avg_discount
 from orders_360
  
  -----------------------------------Profit_Margin--------------------------------------------------------
 select sum(Total_Profit)*100/sum(Total_Revenue) as profit_margin from orders_360

 -----------------------------------Discount_Percent------------------------------------------------------
 select sum(Total_Discount)*100/sum(Total_Revenue) as discount_percent from orders_360

 ----------------------------------Month_wise_Analysis----------------------------------------------------
 select month(cast(bill_date_timestamp as datetime)) as months
 ,format(cast(bill_date_timestamp as datetime),'MMMM') as Monthnames,
 round(count(orders_360.order_id),2) as transactions,
 round(sum(no_categories_per_order),2) as Total_categories_purchased,
 round(sum(Total_Revenue),2) as Total_Revenue
 from orders
 join orders_360
 on orders.order_id = orders_360.order_id
 group by month(cast(bill_date_timestamp as datetime))
 ,format(cast(bill_date_timestamp as datetime),'MMMM')
 order by Total_Revenue desc
 
 -------------------------------- Time_Bin_based -----------------------------------------------------------
select count(orders_360.order_id) as total_orders,
sum(Morning)as no_orders_placed_morning,
sum(Afternoon) as no_orders_placed_Afternoon,
sum(Evening) as no_orders_placed_Evening,
sum(Night) as no_orders_placed_Night 
from orders_360

--------------------------------- weekdays/weekend----------------------------------------------------------
select year(cast(bill_date_timestamp as datetime)) as years ,
sum(case when datepart(weekday,cast(bill_date_timestamp as datetime))in (1,7) then [Total Amount] end)
as Revenue_Weekend,
sum(case when datepart(weekday,cast(bill_date_timestamp as datetime)) not in (1,7) then [Total Amount] end)
as Revenue_Weekdays
,count(distinct orders_360.order_id) as total_orders,sum(Quantity) as Total_qty,
 sum(weekend) as weekend_Orders_Total,
 sum(Weekdays) as Weekdays_orders
 from orders_360
 join orders_clean
 on orders_360.order_id = orders_clean.order_id
 group by year(cast(bill_date_timestamp as datetime))

 -------------------------------payments_Type(counts and revenue)-----------------------------------------
 select sum(Credit_Card) as payment_credit_card,
 sum(UPI_Cash) as payment_UPI_Cash,
 sum(Debit_Card) as payment_Debit_Card,
 sum(Voucher) as payment_Voucher,
 sum(case when Credit_Card > 0 then 1 end) as Credit_card_counts,
 sum(case when UPI_Cash > 0 then 1 end) as UPI_Cash_counts,
 sum(case when Debit_Card > 0 then 1 end) as Debit_Card_counts,
 sum(case when Voucher > 0 then 1 end) as Voucher_counts
 from orders_360

 ------------------------------- Contribution by Churn/Active Customers------------------------------------
 select 
 (select count(Distinct customer_id) as Distinct_cust_count from orders_clean) as Total_Customers,
 sum(case when [Churn/Active] = 'Churn_Customer' then 1 else 0 end) as Churn_cust_count,
 sum(case when [Churn/Active] = 'Active_Customers' then 1 else 0 end) as Active_cust_count,
 sum(case when [Churn/Active] = 'Churn_Customer' then Total_Revenue else 0 end) as Churn_cust_revenue,
 sum(case when [Churn/Active] = 'Active_Customers' then Total_Revenue  else 0 end) as Active_cust_revenue,
 sum(Total_Revenue) as Total_Revenue
 from orders_360


 ------------------------------- Contribution by One_Time/Repeat Customers-----------------------------------
  select 
 sum(case when [Repeated/One_Time_cust] = 'One_Time_Customer' then 1 end) as One_Time_Customer_count,
 sum(case when [Repeated/One_Time_cust] = 'Repeated_Customer' then 1 end) as Repeat_Customer_count,
 sum(case when [Repeated/One_Time_cust] = 'One_Time_Customer' then Monetary end) as One_Time_Custome_revenue,
 sum(case when [Repeated/One_Time_cust] = 'Repeated_Customer' then Monetary end) as Repeat_Customer_revenue,
 sum(Monetary) as Total_Revenue
 from Customer_360


 ------------------------------------ Total_Stores-------------------------------------------------------
 select count(distinct Store_ids) as distinct_store_id from orders_360

 --------------------------- Most_orders_coming from which store--------------------------------------------
 select Store_ids,
 round(sum(Total_Revenue),2) as Total_Revenue,round(sum(Total_Qty),2) as Total_Qty,
 round((sum(Total_Revenue)/(select sum([Total_Revenue]) from orders_360))*100,2)as store_revenue_percent,
 round((sum(Total_Qty)/(select count(Distinct order_id)  from orders_360))*100,2)as Avg_Basket_Size
 from orders_360
 group by Store_ids
 order by Total_Revenue desc

 --------------------------- Customer_Analysis-------------------------------------------------------
select count(customer_id)as total_customers,sum(Monetary) as Total_Revenue,sum(Profit) as Total_Profit,
sum(tOtal_Qty) as Total_qty ,sum(distinct_transactions) as Total_transactions
,(sum(Monetary)*1.0/count(customer_id)) as avg_customer_value
,sum(total_discount)*1.0/count(customer_id) as avg_discount_per_cust
from customer_360


---------------------------average_inactive_days--------------------------------------------------------
select avg(Inactive_days) as Avg_inactive_days from Customer_360


--------------------------- New/Exsisting_Cust-----------------------------------------------------------

select round(sum(Monetary),2) as Total_Revenue,round(sum(total_qty),2) as total_qty,round(sum(Profit),2) as Total_Profit,
round(sum(total_discount),2)as total_discount,[Repeated/One_time_Cust],count(customer_id) as cust_count
from Customer_360
group by [Repeated/One_time_Cust]

--------------------------- Customer_Type---------------------------------------------------------------
select round(sum(Monetary),2) as Total_Revenue,round(sum(total_qty),2) as total_qty,round(sum(Profit),2) as Total_Profit,
round(sum(total_discount),2)as total_discount,discount_type,count(customer_id) as cust_counts
from Customer_360
group by discount_type
select * from customer_360

-------------------------- Customer_level---------------------------------------------------------------------
select round(sum(Monetary),2) as Total_Revenue,round(sum(total_qty),2) as total_qty,round(sum(Profit),2) as Total_Profit,
round(sum(total_discount),2)as total_discount,customer_level,count(customer_id) as Total_Customers
from Customer_360
group by customer_level

--------------------------Customer_Segment-------------------------------------------------------------------
select round(sum(Monetary),2) as Total_Revenue,round(sum(total_qty),2) as total_qty,round(sum(Profit),2) as Total_Profit,
round(sum(total_discount),2)as total_discount,Customer_Segment
from Customer_360
group by Customer_Segment

-------------------------- month wise new_cust_count ----------------------------------------------------------
select MONTH(min_months) as Months , count(customer_id) as cust_count,years from(
select min(cast(bill_date_timestamp as datetime))as min_months,Customer_id,year(cast(bill_date_timestamp as datetime))as years from orders_clean
group by Customer_id,cast(bill_date_timestamp as datetime)) as a
group by MONTH(min_months),years
order by years,months

------------------------------Customer_Value_2022-------------------------------------------------------------
select sum(Monetary)*1.0/count(Customer_360.Customer_id)as avg_customer_value,
year(cast(bill_date_timestamp as datetime))
from Customer_360
join orders_clean
on Customer_360.Customer_id = orders_clean.Customer_id
where year(cast(bill_date_timestamp as datetime)) = 2022
group by year(cast(bill_date_timestamp as datetime))

--------------------------------Customer_Value_2023-----------------------------------------------------------
select sum(Monetary)*1.0/count(Customer_360.Customer_id)as avg_customer_value,
year(cast(bill_date_timestamp as datetime))
from Customer_360
join orders_clean
on Customer_360.Customer_id = orders_clean.Customer_id
where year(cast(bill_date_timestamp as datetime)) = 2023
group by year(cast(bill_date_timestamp as datetime))

-------------------------------Customer_Counts_2022-----------------------------------------------------------
select count(Customer_360.customer_id),year(cast(bill_date_timestamp as datetime)) from Customer_360
join orders_clean
on Customer_360.Customer_id = orders_clean.Customer_id
where year(cast(bill_date_timestamp as datetime)) = 2022
group by year(cast(bill_date_timestamp as datetime))

------------------------------Customer_Count_2023-------------------------------------------------------------
select count(Customer_360.customer_id),year(cast(bill_date_timestamp as datetime)) from Customer_360
join orders_clean
on Customer_360.Customer_id = orders_clean.Customer_id
where year(cast(bill_date_timestamp as datetime)) = 2023
group by year(cast(bill_date_timestamp as datetime))

---------------------------- Repeat_Rate_2023------------------------------------------------------------------
select sum(transactions)*1.0/count(customer_id) from(
select Customer_id,year(cast(bill_date_timestamp as datetime)) as years,count(*)as transactions from orders_clean
where year(cast(bill_date_timestamp as datetime)) = 2023
group by Customer_id,
year(cast(bill_date_timestamp as datetime))) as a

----------------------------Repeat_Rate_2022------------------------------------------------------------------
select sum(transactions)*1.0/count(customer_id) from(
select Customer_id,year(cast(bill_date_timestamp as datetime)) as years,count(*)as transactions from orders_clean
where year(cast(bill_date_timestamp as datetime)) = 2022
group by Customer_id,
year(cast(bill_date_timestamp as datetime))) as a

---------------------------Customer_Purchases>2000 as mrp in 2023-----------------------------------------------------
select count(distinct customer_id),year(cast(bill_date_timestamp as datetime)) as years,sum(Quantity)as total_qty  from orders_clean
where mrp >2000 and year(cast(bill_date_timestamp as datetime))= 2023

--------------------------Customer_Purchases<2000 as mrp in 2022-----------------------------------------------
select count(distinct customer_id),year(cast(bill_date_timestamp as datetime)) as years,sum(Quantity)as total_qty from orders_clean
where mrp <2000 and year(cast(bill_date_timestamp as datetime))= 2022
group by year(cast(bill_date_timestamp as datetime))

-------------------------Customer_Purchases<2000 as mrp in 2023------------------------------------------------
select count(distinct customer_id),year(cast(bill_date_timestamp as datetime)) as years,sum(Quantity)as total_qty from orders_clean
where mrp <2000 and year(cast(bill_date_timestamp as datetime))= 2023
group by year(cast(bill_date_timestamp as datetime))

------------------------Customer_Purchases>2000 as mrp in 2022------------------------------------------------
select count(distinct customer_id),year(cast(bill_date_timestamp as datetime)) as years,sum(Quantity)as total_qty from orders_clean
where mrp >2000 and year(cast(bill_date_timestamp as datetime))= 2022
group by year(cast(bill_date_timestamp as datetime))

-----------------------Discount_Percent in 2022---------------------------------------------------------------
select sum(Discount)*1.0/count(distinct customer_id),year(cast(bill_date_timestamp as datetime))
as years from orders_clean
where year(cast(bill_date_timestamp as datetime)) =2022
group by year(cast(bill_date_timestamp as datetime))

------------------------Discount_Percent in 2023-------------------------------------------------------------
select sum(Discount)*1.0/count(distinct customer_id),year(cast(bill_date_timestamp as datetime))
as years from orders_clean
where year(cast(bill_date_timestamp as datetime)) =2023
group by year(cast(bill_date_timestamp as datetime))

--------------------------Revenue in 2022 ------------------------------------------------------------------
select sum([Total Amount]),
year(cast(bill_date_timestamp as datetime)) as years
from orders_clean
where year(cast(bill_date_timestamp as datetime))=2022
group by year(cast(bill_date_timestamp as datetime))

----------------------------Revenue in 2023------------------------------------------------------------------
select sum([Total Amount]),
year(cast(bill_date_timestamp as datetime)) as years
from orders_clean
where year(cast(bill_date_timestamp as datetime))=2023
group by year(cast(bill_date_timestamp as datetime))

select (repeat_cust*100.0/total_cust) from(
select sum(case when [Repeated/One_time_Cust] = 'Repeated_Customer' then 1 else 0 end) as repeat_cust ,count(*) as total_cust
from customer_360) as a

select * from customer_360
where [Repeated/One_time_Cust] = 'Repeated_Customer'

--------------------- Most orders from which customer_city and customer_state--------------------------------
select top 10 count(distinct_transactions)as total_transactions
,Customers.customer_city
,Customers.customer_state 
from Customers
right join Customer_360
on Customers.Custid = Customer_360.Customer_id
group by Customers.customer_city
,Customers.customer_state
order by total_transactions desc

----------------------- Top 1 city and state based on transactions--------------------------------------------
select top 1 count(distinct_transactions)as total_transactions
,Customers.customer_city,Customers.customer_state from Customers
right join Customer_360
on Customers.Custid = Customer_360.Customer_id
group by Customers.customer_city,Customers.customer_state
order by total_transactions desc

------------------------ Top 1 customer_city and state Based of revenue---------------------------------------
select top 1 round(sum(Monetary),2)as Total_Revenue
,Customers.customer_city,Customers.customer_state from Customers
right join Customer_360
on Customers.Custid = Customer_360.Customer_id
group by Customers.customer_city,Customers.customer_state
order by Total_Revenue desc

--------------------------- peak hour------------------------------------------------------------------------
select sum(total_customers)as total_customers,hour_of_day from(
select count(customer_360.customer_id)as total_customers
, DATEPART(HOUR, TRY_CAST(bill_date_timestamp AS datetime)) as hour_of_day,
customer_360.customer_id
from customer_360
join orders_clean
on customer_360.Customer_id = orders_clean.Customer_id
group by DATEPART(HOUR, TRY_CAST(bill_date_timestamp AS datetime))
,customer_360.customer_id
)as a
group by hour_of_day
order by total_customers desc

-------------------------------Category Penetration Analysis by month on month-----------------------------------
----(Category Penetration = number of orders containing the category/number of orders)---------------------------

with Monthly_orders_count as (
select month(cast(bill_date_timestamp as datetime))as months,count(orders_360.order_id) as orders_count
from orders_clean
join orders_360
on
orders_clean.order_id = orders_360.order_id
group by month(cast(bill_date_timestamp as datetime))),

Monthly_categories_count as (
select month(cast(bill_date_timestamp as datetime))as months,count(distinct no_categories_per_order) as categories, category
from orders_clean
join orders_360
on
orders_clean.order_id = orders_360.order_id
group by month(cast(bill_date_timestamp as datetime)), category)

select a.months,categories,((b.categories)*100.00/(a.orders_count)) as Category_Penetration,
(category)as Total_categories from 
Monthly_orders_count a
join Monthly_categories_count b
on a.months = b.months
order by a.months,Total_categories desc

---------------------number of orders containing the category/number of orders------------------
select category,(sum(cat_orders)*100.00/sum(orders))as categories_contribute from(
select count(a.order_id)as cat_orders,a.category,(select count(order_id) from orders_360)as orders from orders_360 a
group by a.category) as a
group by category
order by categories_contribute desc

------------------------ month_wise top_categories and there counts------------------------------------------
with Monthly_orders_count as (
select month(cast(bill_date_timestamp as datetime))as months,count(orders_360.order_id) as orders_count
from orders_clean
join orders_360
on
orders_clean.order_id = orders_360.order_id
group by month(cast(bill_date_timestamp as datetime))),

Monthly_categories_count as (
select month(cast(bill_date_timestamp as datetime))as months,sum(no_categories_per_order) as categories, category
from orders_clean
join orders_360
on
orders_clean.order_id = orders_360.order_id
group by month(cast(bill_date_timestamp as datetime)), category)

select * from(
select Months,category,Total_categories as Total_Categories_bought
,Rank()over(partition by Months order by Total_Categories desc) as ranks from(
select a.months,category,((b.categories)*100.00/(a.orders_count)) as Category_Penetration,
(categories)as Total_categories from 
Monthly_orders_count a
join Monthly_categories_count b
on a.months = b.months
) as a) as ab
where ranks = 1

----------------------------------Gender Wise Category contribution----------------------------------------------
select gender,count(customer_id)as cust_count,category  from Customers
right join orders_clean
on Customers.Custid = orders_clean.Customer_id
join product_info_clean
on orders_clean.product_id = product_info_clean.product_id
group by gender,category

---------------------Most popular category during first purchase of customer-----------------------------------
select top 1 count(Total_Categories) as Categories_count,category from(
select count(category) as Total_Categories,category,min(first_Transaction_Date)as first_purchase,
Customer_id
from Customer_360
group by category,Customer_id) as a
group by category
order by Categories_count desc

---------------Which categories (top 10) are maximum rated & minimum rated and average rating score? -------------

select 
top 10 
Category,avg(avg_satisfaction_score) as avg_satisfaction_score
from orders_clean
left join product_info_clean
on orders_clean.product_id = product_info_clean.product_id
left join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
group by Category
order by avg_satisfaction_score desc

------------------------ Minimum_Rated_Category ------------------------------------------------------------
select top 1 Category,avg(avg_satisfaction_score) as avg_satisfaction_score
from orders_clean
left join product_info_clean
on orders_clean.product_id = product_info_clean.product_id
left join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
group by Category
order by avg_satisfaction_score 

------------------------- Maximum_Rated_Category -----------------------------------------------------------

select top 1 Category,avg(avg_satisfaction_score) as avg_satisfaction_score
from orders_clean
left join product_info_clean
on orders_clean.product_id = product_info_clean.product_id
left join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
group by Category
order by avg_satisfaction_score desc

--- Average rating by location, store, product, category, month, etc.

---------------------------------- Store------------------------------------------------------------------
select round(avg(avg_satisfaction_score),2) as Average_Rating,Delivered_StoreID
from orders_clean
join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
group by Delivered_StoreID

-----------------------------------Product ----------------------------------------------------------------
select round(avg(avg_satisfaction_score),2) as Average_Rating,product_id
from orders_clean
join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
group by product_id

----------------------------------Month-------------------------------------------------------------------
select round(avg(avg_satisfaction_score),2) as Average_Rating,month(cast(bill_date_timestamp as datetime)) as Months
from orders_clean
join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
group by month(cast(bill_date_timestamp as datetime))

-------------------------------Category ----------------------------------------------------------------------
select Category,round(avg(avg_satisfaction_score),2) as Average_Rating from orders_clean
left join product_info_clean
on orders_clean.product_id = product_info_clean.product_id
left join order_review_clean
on orders_clean.order_id = order_review_clean.order_id
group by Category

----Customers who started in each month and understand their behavior in the respective months
------------------------------- Cohort_Analysis------------------------------------------------------------

select First_Months ,Customers,lag(Customers) over(order by First_Months)as prev_month_cust,
Customers- lag(Customers) over(order by First_Months) as diff
from(
select first_months ,count(Customer_id) as Customers from(
select customer_id,month(min(first_Transaction_Date)) as First_Months from Customer_360
group by customer_id) as a
group by first_months) as ab

------------------------------- Month on Month Growth Customers----------------------------------------------
select First_Months,
lag(Customers) over(order by First_Months)as prev_month_cust,
Customers- lag(Customers) over(order by First_Months) as diff,
(round((Customers- lag(Customers) over(order by First_Months))*100.,2)
/ lag(Customers) over(order by First_Months))
as [%Growth]
from(
select first_months ,count(Customer_id) as Customers
from(
select customer_id,month(min(first_Transaction_Date)) as First_Months from Customer_360
group by customer_id ) as a
group by first_months) as ab

-----------------------------Retention of customers on month on month basis-----------------------------------

with first_transaction_count as (
select format(min(first_transaction_date),'yyyy-MM') as First_mon,
Customer_id as cust_id_first_month,count(customer_id) as cust_count_first,year(min(first_transaction_date)) as year
from Customer_360
group by customer_id),

orders_clean_month as(
select format(cast(bill_date_timestamp as datetime),'yyyy-MM')as order_clean_month
,count(customer_id) as order_count_month,Customer_id from orders_clean
group by format(cast(bill_date_timestamp as datetime),'yyyy-MM'),Customer_id )


select First_mon,case when order_clean_month is not null then order_clean_month end as repeat_cust_month ,
count(first_transaction_count.cust_count_first)as cust_first_purchase,
count(case when orders_clean_month.Customer_id is not null 
then orders_clean_month.Customer_id end)as retained_cust
, round((count(orders_clean_month.Customer_id)*100.0/count(first_transaction_count.cust_count_first)),2) as Retention_percent
from first_transaction_count
left join orders_clean_month
on first_transaction_count.cust_id_first_month = orders_clean_month.Customer_id
and first_transaction_count.First_mon < orders_clean_month.order_clean_month
group by First_mon,order_clean_month
order by First_mon,order_clean_month

-------------------------------------Retention Percentage-------------------------------------------------
with first_transaction_count as (
select format(min(first_transaction_date),'yyyy-MM') as First_mon,
Customer_id as first_month_cust_count,count(customer_id) as cust_count_first,year(min(first_transaction_date)) as year
from Customer_360
group by customer_id),

orders_clean_month as(
select format(cast(bill_date_timestamp as datetime),'yyyy-MM')as order_clean_month
,count(customer_id) as order_count_month,Customer_id from orders_clean
group by format(cast(bill_date_timestamp as datetime),'yyyy-MM'),Customer_id )

select (sum(retained_cust)*100/sum(cust_first_purchase)) from (
select First_mon,case when order_clean_month is not null then order_clean_month end as repeat_cust_month ,
count(first_transaction_count.cust_count_first)as cust_first_purchase,
count(case when orders_clean_month.Customer_id is not null 
then orders_clean_month.Customer_id end)as retained_cust
, round((count(orders_clean_month.Customer_id)*100.0/count(first_transaction_count.cust_count_first)),2) as Retention_percent
from first_transaction_count
left join orders_clean_month
on first_transaction_count.first_month_cust_count = orders_clean_month.Customer_id
and first_transaction_count.First_mon < orders_clean_month.order_clean_month
group by First_mon,order_clean_month) as a
-------------------------------------------------------------Monthly_Retention_Rate --------------------------

with first_transaction_count as (
select month(min(first_transaction_date)) as First_mon,
Customer_id as first_month_cust_count,count(customer_id) as cust_count_first,year(min(first_transaction_date)) as year
from Customer_360
group by customer_id),

orders_clean_month as(
select month(cast(bill_date_timestamp as datetime))as order_clean_month
,count(customer_id) as order_count_month,Customer_id from orders_clean
group by month(cast(bill_date_timestamp as datetime)),Customer_id )
 
select sum(retained_cust) as total_retained_cust,first_mon,sum(cust_first_purchase)as Total_New_customer
,(sum(retained_cust)*100.0/sum(cust_first_purchase))as retention_rate 
from (
select First_mon,case when order_clean_month is not null then order_clean_month end as repeat_cust_month ,
count(first_transaction_count.cust_count_first)as cust_first_purchase,
count(case when orders_clean_month.Customer_id is not null 
then orders_clean_month.Customer_id end)as retained_cust
, round((count(orders_clean_month.Customer_id)*100.0/count(first_transaction_count.cust_count_first)),2) as Retention_percent
from first_transaction_count
left join orders_clean_month
on first_transaction_count.first_month_cust_count = orders_clean_month.Customer_id
and first_transaction_count.First_mon < orders_clean_month.order_clean_month
group by First_mon,order_clean_month) as a
group by First_mon
order by retention_rate desc

----------------------------------------------------- Retention_cust month and year wise ------------------------------------

with first_purchase_cust as (
select customer_id,month( min(cast(bill_date_timestamp as datetime))) as months,year(min(cast(bill_date_timestamp as datetime))) 
as years  from orders_clean
group by customer_id),

monthly_customers as (
select Customer_id,month(cast(bill_date_timestamp as datetime)) as months,
year(cast(bill_date_timestamp as datetime))as years,count(distinct Customer_id) as Total_Customers
from orders_clean
group by Customer_id,month(cast(bill_date_timestamp as datetime)),year(cast(bill_date_timestamp as datetime)))

select first_purchase_cust.months,
first_purchase_cust.years ,count(distinct first_purchase_cust.customer_id) as Total_cust,
count(distinct monthly_customers.customer_id) as customers_retained,
(count(distinct monthly_customers.customer_id)*100.0/count(distinct first_purchase_cust.customer_id)) as retained_cust_ratio
into retention_cust 
from first_purchase_cust 
left join monthly_customers
on first_purchase_cust.customer_id = monthly_customers.customer_id
and  
(first_purchase_cust.years = monthly_customers.years and first_purchase_cust.months < monthly_customers.months
or (first_purchase_cust.years < monthly_customers.years))
group by 
first_purchase_cust.months,
first_purchase_cust.years 

select * from retention_cust


--- which cohorot month having maximum retention

with first_transaction_count as (
select format(min(first_transaction_date),'yyyy-MM') as First_mon,
Customer_id as first_month_cust_count,count(customer_id) as cust_count_first,year(min(first_transaction_date)) as year
from Customer_360
group by customer_id),

orders_clean_month as(
select format(cast(bill_date_timestamp as datetime),'yyyy-MM')as order_clean_months
,count(customer_id) as order_count_month,Customer_id from orders_clean
group by format(cast(bill_date_timestamp as datetime),'yyyy-MM'),Customer_id )

select First_mon,
count(first_transaction_count.cust_count_first)as cust_first_purchase,
count(case when orders_clean_month.Customer_id is not null 
then orders_clean_month.Customer_id end)as retained_cust
, round((count(orders_clean_month.Customer_id)*100.0/count(first_transaction_count.cust_count_first)),2) as Retention_percent
from first_transaction_count
left join orders_clean_month
on first_transaction_count.first_month_cust_count = orders_clean_month.Customer_id
and first_transaction_count.First_mon < orders_clean_month.order_clean_months
group by First_mon
order by retained_cust desc,Retention_percent desc, First_mon

----- Which months have had the least sales, what is the sales amount and contribution in percentage?  
----Sales trend by month   
----Is there any seasonality in the sales (weekdays vs. weekends, months, days of week, weeks etc.)?
----Total Sales by Week of the Day, Week, Month, Quarter, Weekdays vs. weekends etc.

-------------------------------- Least sales and its sales_percent-----------------------------------------

select format(cast(bill_date_timestamp as datetime),'yyyy-MM')as months_wise,
sum(Total_Revenue)as Total_Revenue,
round((sum(Total_Revenue)*100.00/(select sum(Total_Revenue) from orders_360)),6)as sales_percent
from  orders_360
join orders_clean
on orders_360.order_id 
= orders_clean.order_id
group by format(cast(bill_date_timestamp as datetime),'yyyy-MM')
order by Total_Revenue ,sales_percent

------------------------ Most sales and its sales_percent----------------------------------------------
select format(cast(bill_date_timestamp as datetime),'yyyy-MM')as months_wise,
sum(Total_Revenue)as Total_Revenue,
round((sum(Total_Revenue)*100.0/(select sum(Total_Revenue) from orders_360_clean)),6)as sales_percent
from  orders_360_clean
join orders_clean
on orders_360_clean.order_id 
= orders_clean.order_id
group by format(cast(bill_date_timestamp as datetime),'yyyy-MM')
order by Total_Revenue desc,sales_percent desc

---- Sales trends on basis of month
select month(cast(bill_date_timestamp as datetime))as months_wise,
sum(Total_Revenue)as Total_Revenue,
round((sum(Total_Revenue)/(select sum(Total_Revenue) from orders_360_clean)),6)as sales_percent
from  orders_360_clean
join orders_clean
on orders_360_clean.order_id 
= orders_clean.order_id
group by month(cast(bill_date_timestamp as datetime))
order by month(cast(bill_date_timestamp as datetime))

----- Total sales by week of the day,week,month,quarters,weekend V/S weekdays

------------------------ Weekend/Weekdays--------------------------------------------------------------
select (case when datepart(weekday,cast(bill_date_timestamp as datetime)) in (1,7) then 'Weekend' else ' Weekdays'
end)as 'Date_Type',year(cast(bill_date_timestamp as datetime))as Years
,sum(Total_Revenue) as Total_Revenue
from orders_360
join orders_clean
on orders_360.order_id = orders_clean.order_id
group by  case when 
datepart(weekday,cast(bill_date_timestamp as datetime)) in (1,7) then 'Weekend' else ' Weekdays' end,
year(cast(bill_date_timestamp as datetime))

-----------------------Months------------------------------------------------------------------------------
select month(cast(orders_clean.bill_date_timestamp as datetime)) as Months
,sum(Total_Revenue) as Total_Revenue
from orders_360
join orders_clean
on orders_360.order_id = orders_clean.order_id
group by  
month(cast(bill_date_timestamp as datetime))
order by month(cast(orders_clean.bill_date_timestamp as datetime))

------------------------- Years----------------------------------------------------------------------------
select year(cast(orders_clean.bill_date_timestamp as datetime)) as Years
,sum([Total Amount]) as Total_Revenue
from orders_clean
group by  
year(cast(bill_date_timestamp as datetime))

----------------------- Week of the day-------------------------------------------------------------------
select datepart(WEEK,cast(bill_date_timestamp as datetime)) as weeks,
year(cast(bill_date_timestamp as datetime)),
sum([Total Amount]) as Total_Revenue from orders_clean
group by datepart(WEEK,cast(bill_date_timestamp as datetime)),
year(cast(bill_date_timestamp as datetime))
order by weeks

------------------------- Quarters ------------------------------------------------------------------------
select datepart(quarter,cast(bill_date_timestamp as datetime)) 
as quarters,year(cast(bill_date_timestamp as datetime))
,sum([Total Amount]) as Total_Revenue
from orders_clean
group by datepart(quarter,cast(bill_date_timestamp as datetime))
,year(cast(bill_date_timestamp as datetime))
order by quarters

---------- Top 10-performing & worst 10 performance stores in terms of sales--------------------------------

-------------------Worst_performing-----------------------------------------------------------------------
select top 10 delivered_storeid,total_Revenue
from Store_360
order by total_Revenue

------------------ Best Perfoming --------------------------------------------------------------------------
select top 10 delivered_storeid,total_Revenue
from Store_360
order by total_Revenue desc

---Find out the number of customers who purchased in all channels and find the key metrics.

select * from Customer_360
where distinct_channel >1

--- Cross_Selling
---We need to find which of the top 10 combinations
--of products are selling together in each transaction.  (combination of 2 or 3 buying together) 


select top 10 concat(a.product_id,' , ',b.product_id) as product_pair,count(a.order_id) as orders_count
into product_combo
from
orders_clean a
inner join orders_clean b
on a.order_id = b.order_id and 
a.product_id<b.product_id
group by  concat(a.product_id,' , ',b.product_id) 
order by orders_count desc 

 ------------------------ Categories_contribution-----------------------------------------------------------
 select Category,count(*) as categories_counts,
 round(sum(Total_Revenue),2)as Total_Revenue,round(sum(Total_Qty),2) as Total_Qty,round(sum(Total_Profit),2) as Total_Profit,
 round((sum(Total_Profit)/ sum(Total_Revenue))*100,2) as profit_percent
 into categories_contribution
 from orders_360
 group by Category
 order by Total_Revenue desc

 ----------------------- Most Profitable Categories-------------------------------------------------------
 select top 1 Category,count(*) as categories_counts,
 round(sum(Total_Revenue),2)as Total_Revenue,round(sum(Total_Qty),2) as Total_Qty,round(sum(Total_Profit),2) as Total_Profit,
 round((sum(Total_Profit)/ sum(Total_Revenue))*100,2) as profit_percent
 from orders_360
 group by Category
 order by Total_Revenue desc

 --------------------------- Most Popular Categories----------------------------------------------------------
 select Category,sum(customer_count) as customer_count from(
 select Category,orders_360.order_id,count(customer_id)as customer_count from orders_360
 join orders_clean
 on orders_360.order_id = orders_clean.order_id
 group by Category,orders_360.order_id) as a
 group by Category
 order by customer_count desc

 --------------------------- Most Popular products by state region, store-------------------------------------
 select product_id,count(orders_clean.customer_id) as customers_count,
 Delivered_StoreID,seller_city,seller_state
 from orders_clean
 join orders_360 
 on orders_clean.order_id = orders_360.order_id
 left join  Customers
 on orders_clean.Customer_id = Customers.Custid
 left join store_info_clean
 on orders_clean.Delivered_StoreID= store_info_clean.StoreID
 group by Delivered_StoreID,seller_city,seller_state,product_id
 order by customers_count desc


 --- Understand preferences of customers 
 ---(preferred channel, Preferred payment method, preferred store, discount preference, preferred categories etc.)
 
 ---- Preferred_payment_method
 select 
 prefered_payments,count(Customer_id)as count_cust from Customer_360
 group by prefered_payments
 order by count_cust desc

 select top 10 * from Customer_360

 ---- preferred_channels
 select Channel,count(Customer_360.Customer_id) as cust_count
 ---,Delivered_StoreID,Category
 from Customer_360
 join orders_clean 
 on Customer_360.Customer_id = orders_clean.Customer_id
 left join product_info_clean
 on orders_clean.product_id = product_info_clean.product_id
 group by Channel
 --,Delivered_StoreID,Category
 order by cust_count desc

 --- preferred Delivered_Store_Id
  select count(Customer_360.Customer_id) as cust_count
 ,Delivered_StoreID
 --,Category
 from Customer_360
 join orders_clean 
 on Customer_360.Customer_id = orders_clean.Customer_id
 left join product_info_clean
 on orders_clean.product_id = product_info_clean.product_id
 group by 
 Delivered_StoreID
 --,Category
 order by cust_count desc


 ---- Preferred store_id 
   select count(Customer_360.Customer_id) as cust_count
 ,Delivered_StoreID
 --,Category
 from Customer_360
 join orders_clean 
 on Customer_360.Customer_id = orders_clean.Customer_id
 left join product_info_clean
 on orders_clean.product_id = product_info_clean.product_id
 group by 
 Delivered_StoreID
 --,Category
 order by cust_count desc


  ---- popular product on the basis of store 
  select count(Customer_360.Customer_id) as cust_count
 ,Delivered_StoreID,orders_clean.product_id
 --,Category
 from Customer_360
 join orders_clean 
 on Customer_360.Customer_id = orders_clean.Customer_id
 left join product_info_clean
 on orders_clean.product_id = product_info_clean.product_id
 group by 
 Delivered_StoreID,orders_clean.product_id
 --,Category
 order by cust_count desc


 -----Understand the behavior of customers who purchased one category and purchased multiple categories

 ------------------------------------------------customer who purchased multiple categories------------
with multiple_categories_cust as (
select Customer_id from(
select Category,count(orders_clean.customer_id) as cust_count,Customer_id from orders_clean
left join product_info_clean
on orders_clean.product_id
= product_info_clean.product_id
group by Category,Customer_id) as a
where cust_count > 1)

select format(cast(bill_date_timestamp as datetime),'yyyy-MMM')as year_mon
,count(multiple_categories_cust.Customer_id) as cust_count,
sum(Quantity) as qty,Sum([Total Amount])as total_revenue
from orders_clean
right join multiple_categories_cust
on orders_clean.customer_id  = multiple_categories_cust.customer_id
group by format(cast(bill_date_timestamp as datetime),'yyyy-MMM')
order by format(cast(bill_date_timestamp as datetime),'yyyy-MMM')

----------------------------------------------------- customer behaviour who bought single categories ----------
with single_categories_cust as (
select Customer_id from(
select Category,count(orders_clean.customer_id) as cust_count,Customer_id from orders_clean
left join product_info_clean
on orders_clean.product_id
= product_info_clean.product_id
group by Category,Customer_id) as a
where cust_count = 1)

select format(cast(bill_date_timestamp as datetime),'yyyy-MMM')as year_mon
,count(single_categories_cust.Customer_id) as cust_count,
sum(Quantity) as qty,Sum([Total Amount])as total_revenue
from orders_clean
right join single_categories_cust
on orders_clean.customer_id  = single_categories_cust.customer_id
group by format(cast(bill_date_timestamp as datetime),'yyyy-MMM')
order by format(cast(bill_date_timestamp as datetime),'yyyy-MMM')


-- Top 10 most expensive proucts stored by price and there contribution in sales 
select top 10 product_id,sum(mrp)as mrp,sum([Total Amount]) as Total_Revenue from orders_clean
group by product_id
order by mrp desc

select * from customer_360
select * from orders_360
select * from Store_360
select * from orders_clean
select * from orders_payment_clean
select * from product_info_clean
select * from order_review_clean
select * from store_info_clean


with first_months as(
select order_id,min(month(cast(bill_date_timestamp as datetime)))as months,
min(datename(month,cast(bill_date_timestamp as datetime))) as months_name
from orders_clean
group by order_id)

select orders_360.*,first_months.months,months_name 
into orders_360 from orders_360
left join first_months 
on orders_360.order_id = first_months.order_id

alter table orders_360
add months int,
monthnames Nvarchar(20)

alter table orders_360
add Years int

select * from orders_360

update orders_360
set 
orders_360.months = fm.months,
orders_360.monthnames = fm.months_name,
orders_360.Years = fm.years
from orders_360
left join (
select order_id,min(month(cast(bill_date_timestamp as datetime)))as months,
min(datename(month,cast(bill_date_timestamp as datetime))) as months_name,
min(year(cast(bill_date_timestamp as datetime))) as years
from orders_clean
group by order_id) as fm
on orders_360.order_id = fm.order_id

alter table orders_360
drop column month,monthname
select * from orders_360

select * from customer_360

alter table customer_360
add years int,
Monthsname nvarchar(20),
months int 


update customer_360
set 
customer_360.months = fm.months,
customer_360.Monthsname = fm.months_name,
customer_360.Years = fm.years
from customer_360
left join (
select customer_id,min(month(cast(bill_date_timestamp as datetime)))as months,
min(datename(month,cast(bill_date_timestamp as datetime))) as months_name,
min(year(cast(bill_date_timestamp as datetime))) as years
from orders_clean
group by customer_id) as fm
on customer_360.Customer_id = fm.customer_id

select seller_city,seller_state,
(sum(Profit)*100/(select sum(profit) from Store_360)
) as profit_percent
from Store_360
group by seller_city,seller_state

select * from Store_360

alter table store_360
drop column month_years,Months,Years

select * from orders_clean

alter table store_360
add month_years varchar(20),
months int,
years int


update Store_360
set month_years = store.month_year,
months =  store.months,
years = store.years from
store_360
left join (
select delivered_storeid, format(cast(bill_date_timestamp as datetime) ,'MMM-yyyy')as month_year,
month(cast(bill_date_timestamp as datetime)) as months,year(cast(bill_date_timestamp as datetime)) as years from orders_clean
) as store
on store_360.delivered_storeid = store.Delivered_StoreID

select * from orders_clean

select orders_clean.Delivered_StoreID, format(cast(bill_date_timestamp as datetime) ,'MMM-yyyy')as month_year,
month(cast(bill_date_timestamp as datetime)) as months,year(cast(bill_date_timestamp as datetime)) as years,
sum([Total Amount]) as Total_amt,
count(order_id) as orders_count
into store_monthly_records
from orders_clean
left join 
Store_360
on orders_clean.Delivered_StoreID = Store_360.delivered_storeid
group by orders_clean.Delivered_StoreID,format(cast(bill_date_timestamp as datetime) ,'MMM-yyyy'),month(cast(bill_date_timestamp as datetime)),
year(cast(bill_date_timestamp as datetime))

select * from orders_360



------------------------------ Cohort_Analysis (Non_Waterfall)-------------------

with first_month_customers as(
select Customer_id,month(cast(First_transaction_date as datetime)) as first_month,
cast(First_transaction_date as datetime)as first_transaction_date from customer_360),


next_purchase_month as (
select orders_clean.customer_id,first_month ,
first_transaction_date,cast(bill_date_timestamp as datetime) as bill_date,
datediff(month,first_transaction_date,bill_date_timestamp) as month_index
from orders_clean
join first_month_customers
on orders_clean.Customer_id = first_month_customers.customer_id
)
,

first_purchase_month_cust as (
---select * from(
select first_month,count(distinct customer_id) as cust_count,month_index
--ROW_NUMBER() over(partition by first_month order by month_index) as row_no
from next_purchase_month
where month_index = 0
group by first_month,month_index
--) as a
--where row_no = 1 
)

select next_purchase_month.first_month,next_purchase_month.month_index,max(first_purchase_month_cust.cust_count) as first_purchase_cust,
(count(distinct next_purchase_month.customer_id)*1.0/max(first_purchase_month_cust.cust_count))as retention_rate
--,month_index
--,count(customer_id)/(select count(cust_count) from first_purchase_month_cust group by first_month)\
--,round((count(distinct next_purchase_month.customer_id)*1.0/max(first_purchase_month_cust.cust_count)),2)
from next_purchase_month 
left join first_purchase_month_cust
on next_purchase_month.first_month = first_purchase_month_cust.first_month
group by 
next_purchase_month.first_month,next_purchase_month.month_index
order by 
next_purchase_month.first_month,
next_purchase_month.month_index

------------------------------ Cohort_Analysis (Waterfall)-------------------


with first_month_customers as(
select Customer_id,month(cast(First_transaction_date as datetime)) as first_month,
cast(First_transaction_date as datetime)as first_transaction_date from customer_360),

next_purchase_month as ( 
select orders_clean.customer_id,first_month ,
first_transaction_date,cast(bill_date_timestamp as datetime) as bill_date,
datediff(month,first_transaction_date,bill_date_timestamp) as month_index
from orders_clean
join first_month_customers
on orders_clean.Customer_id = first_month_customers.customer_id
)
,

previous_month_cont as (
select count(a.customer_id) as cust_count,a.first_month,previous_mon from(
select first_month,month_index,customer_id,
lag(first_month,1)over 
(partition by customer_id order by month_index) as previous_mon from next_purchase_month
---order by first_month,month_index
)as a
group by a.first_month,previous_mon
--order by a.first_month
)

