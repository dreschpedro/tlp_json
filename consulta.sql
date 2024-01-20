SELECT 
    b.title AS l,
    b.visible_author AS a,
    --    ma.name AS categoria,
    dpd.stock AS s, 
    CEIL(pr.price) AS p
    --     bi.isbn AS ISBN

FROM "public".book b

--JOIN "public".book_materia bm ON bm.book_id = b.book_id
-- JOIN "public".materia ma ON ma.materia_id = bm.materia_id
JOIN "public".price pr ON pr.book_id = b.book_id
JOIN "public".book_isbn bi ON bi.book_id = b.book_id
JOIN (
    SELECT 
        product_id,
        stock,
        MAX(date_to) OVER (PARTITION BY product_id) AS max_date_to
    FROM "public".dn_product_depot_stock_historic
) AS dpd ON dpd.product_id = b.book_id

WHERE b.active = true
AND pr.price >= 500
AND dpd.stock >= 0

ORDER BY dpd.max_date_to DESC
--LIMIT 100;
