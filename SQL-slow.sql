SELECT DISTINCT m1 AS m1,m1_TYP AS m1_TYP FROM (SELECT entry AS a5,QS5.a1 AS a1,QS5.a4 AS a4,QS5.m1 AS m1,m1_TYP AS m1_TYP
FROM "TABLE1".TABLE1 AS T,
    (SELECT entity AS m1,typ AS m1_typ,elem AS a5,QS4.a1 AS a1,QS4.a4 AS a4
    FROM "TABLE2".TABLE2 AS T,
        (SELECT entry AS a2,QS3.a1 AS a1,QS3.a4 AS a4,QS3.m1 AS m1,m1_TYP AS m1_TYP
        FROM "TABLE1".TABLE1 AS T,
            (SELECT entry AS a4,QS2.a1 AS a1,QS2.a2 AS a2,QS2.m1 AS m1,m1_TYP AS m1_TYP
            FROM "TABLE1".TABLE1 AS T,
                (SELECT entry AS a3,QS1.a1 AS a1,QS1.a2 AS a2,QS1.a4 AS a4,QS1.m1 AS m1,m1_TYP AS m1_TYP
                FROM "TABLE1".TABLE1 AS T,
                    (SELECT a1 AS a1,m1 AS m1,m1_TYP AS m1_TYP,COALESCE(S3.elem,val3) AS a3, COALESCE(S4.elem,val4) AS a4,COALESCE(S2.elem,val2) AS a2
                    FROM
                        (SELECT entry AS a1, T.val2 AS m1,T.typ2 AS m1_TYP,T.val6 AS VAL3,T.val6 AS VAL4,T.val8 AS VAL2
                        FROM "TABLE1".TABLE1 AS T,
                            (SELECT elem AS a1
                            FROM "TABLE2".TABLE2 AS T
                            WHERE entity = '3' AND typ = 5001
                            AND (prop = '1oh~#some_prop1')) AS QS0
                        WHERE entry = QS0.a1
                        AND (T.prop0 = '4xm~#type' AND T.prop8 = '1oh~#some_prop2' AND T.prop6 = '1oh~#some_prop3' AND T.prop6 = '1oh~#some_prop3' AND T.prop2 = '1oh~#is_atom_of')
                            AND  T.val0 = '7a~') AS Q1
                    LEFT OUTER JOIN "TABLE3".TABLE3 AS S2 ON Q1.VAL2 = S2.list_id
                    LEFT OUTER JOIN "TABLE3".TABLE3 AS S3 ON Q1.VAL3 = S3.list_id
                    LEFT OUTER JOIN "TABLE3".TABLE3 AS S4 ON Q1.VAL4 = S4.list_id
                    WHERE ((a1 <> COALESCE(S4.elem,val4)))) AS QS1
                WHERE entry = QS1.a3
                    AND (T.prop0 = '4xm~#type' AND T.prop5 = '1oh~#some_prop1' AND T.prop8 = '1oh~#some_prop4')
                    AND  T.val0 = '562~' AND T.val5 = '1' AND T.val8 = '6o7~') AS QS2
            WHERE entry = QS2.a4
                AND (T.prop0 = '4xm~#type' AND T.prop5 = '1oh~#some_prop1')
                AND T.val0 = '7a~' AND T.val5 = '1') AS QS3
        WHERE entry = QS3.a2 AND (T.prop0 = '4xm~#type' AND T.prop5 = '1oh~#some_prop1')
            AND T.val0 = '562~' AND T.val5 = '1') AS QS4
    WHERE entity = QS4.m1 AND typ = QS4.m1_TYP
    AND (prop = '1oh~#isome_prop5')) AS QS5
WHERE entry = QS5.a5
AND (T.prop0 = '4xm~#type' AND T.prop5 = '1oh~#some_prop1')
AND T.val0 = '1eg~' AND T.val5 = '0') AS QS6
LIMIT 100;