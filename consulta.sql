SELECT 
    b.title AS titulo,
 -- ma.name AS categoria,
    b.visible_author AS autor,
    MAX(dpd.stock) AS stock, 
    CEIL(MAX(pr.price)) AS precio

FROM "public".book b
JOIN "public".price pr ON pr.book_id = b.book_id
JOIN "public".book_isbn bi ON bi.book_id = b.book_id
JOIN "public".dn_product_depot_stock_historic dpd ON dpd.product_id = b.book_id

WHERE b.active = true
AND pr.price >= 500
AND dpd.stock >= 0

GROUP BY b.book_id, b.title, b.visible_author
ORDER BY MAX(dpd.date_to) DESC
-- LIMIT 500;

