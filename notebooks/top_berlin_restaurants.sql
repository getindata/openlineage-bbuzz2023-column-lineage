  SELECT r.id, r.name, r.address, count(order_id) as orders
  FROM public.restaurants r
  JOIN public.orders_7_days o ON (o.restaurant_id = r.id) 
  GROUP BY r.id, r.name, r.address