SELECT
  t5.s_suppkey,
  t5.s_name,
  t5.s_address,
  t5.s_phone,
  t5.total_revenue
FROM (
  SELECT
    *
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *
      FROM "supplier" AS t0
      INNER JOIN (
        SELECT
          t1.l_suppkey,
          SUM(
            (
              t1.l_extendedprice
            ) * (
              (
                CAST(1 AS TINYINT)
              ) - (
                t1.l_discount
              )
            )
          ) AS total_revenue
        FROM "lineitem" AS t1
        WHERE
          (
            t1.l_shipdate
          ) >= (
            MAKE_DATE(1996, 1, 1)
          )
          AND (
            t1.l_shipdate
          ) < (
            MAKE_DATE(1996, 4, 1)
          )
        GROUP BY
          1
      ) AS t2
        ON (
          t0.s_suppkey
        ) = (
          t2.l_suppkey
        )
    ) AS t3
    WHERE
      (
        t3.total_revenue
      ) = (
        SELECT
          MAX(t1.total_revenue) AS "Max(total_revenue)"
        FROM (
          SELECT
            t0.l_suppkey,
            SUM(
              (
                t0.l_extendedprice
              ) * (
                (
                  CAST(1 AS TINYINT)
                ) - (
                  t0.l_discount
                )
              )
            ) AS total_revenue
          FROM "lineitem" AS t0
          WHERE
            (
              t0.l_shipdate
            ) >= (
              MAKE_DATE(1996, 1, 1)
            )
            AND (
              t0.l_shipdate
            ) < (
              MAKE_DATE(1996, 4, 1)
            )
          GROUP BY
            1
        ) AS t1
      )
  ) AS t4
  ORDER BY
    t4.s_suppkey ASC
) AS t5