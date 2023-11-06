SELECT
    T2.DocDate as 'Receipt Date',
    T2.CardCode,
    T2.CardName,
    T2.docnum as 'Receipt No.',
    T1.sumapplied,
    CASE
        Doc.ObjType
        when '14' then T1.SumApplied * -1
        WHEN '13' Then T1.SumApplied
        ELSE Doc.ObjType
    END AS ' Amount',
    CASE
        Doc.objtype
        when '13' then 'Invoice'
        When '14' then 'CN'
        else Doc.ObjType
    END AS 'Type',
    Doc.DocType,
    T1.Docentry as 'Inv Int #',
    Doc.Docnum as 'Inv. Num',
    Doc.DocDate as 'Inv Date',
    Doc.doctotal as 'Inv. Amt',
    Doc.GrosProfit as 'Inv. GP',
    T3.SlpName as 'Salesperson',
    Doc.PymntGroup as 'Payment Term',
    T2.Canceled
FROM
    (
        Select
            Y1.Docentry,
            Y1.objtype,
            Y1.DocType,
            Y1.docnum,
            Y1.DocDate,
            Y1.NumAtCard,
            Y1.doctotal,
            Y1.Grosprofit,
            Y1.slpcode,
            Y2.[PymntGroup]
        from
            OINV Y1
            left join OCTG Y2 on Y1.[GroupNum] = Y2.[GroupNum]
        UNION
        ALL
        Select
            Y1.Docentry,
            Y1.objtype,
            Y1.DocType,
            Y1.docnum,
            Y1.docdate,
            Y1.NumAtCard,
            Y1.Doctotal,
            Y1.grosprofit,
            Y1.SlpCode,
            Y2.[PymntGroup]
        From
            ORIN Y1
            left join OCTG Y2 on Y1.[GroupNum] = Y2.[GroupNum]
    ) as DOC
    LEFT JOIN RCT2 T1 ON DOC.DocEntry = T1.DocEntry AND DOC.ObjType = T1.InvType
    LEFT JOIN ORCT T2 ON T1.docnum = T2.docentry
    INNER JOIN OSLP T3 ON Doc.SlpCode = T3.slpcode
WHERE
    T2.DocDate >= [%0]
    AND T2.DocDate <= [%1]
    AND T2.Canceled = 'N'
UNION
ALL
SELECT
    ORCT.DocDate AS 'Receipt Date',
    ORCT.CardCode,
    OCRD.CardName,
    ORCT.DocNum AS 'Receipt No.',
    RCT2.SumApplied * -1 AS 'payment line item sumapplied',
    RCT2.SumApplied * -1 AS 'Amount',
    'Invoice' AS 'Type',
    'I' AS 'DocType',
    RCT2.DocEntry AS 'Inv Int #',
    ORCT.DocEntry AS 'Inv. Num',
    OITR.ReconDate AS 'Inv Date',
    OITR.Total AS 'reconciliation amount Inv. Amt',
    NULL AS 'Inv. GP',
    OSLP.SlpName AS 'Salesperson',
    OCTG.PymntGroup AS 'Payment Term',
    OITR.Canceled,
    ORCT.DocTotal AS 'net money transferred'
FROM
    OITR
    LEFT JOIN ORCT ON OITR.InitObjAbs = ORCT.DocEntry
    LEFT JOIN OCRD ON ORCT.CardCode = OCRD.CardCode
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    LEFT JOIN OSLP ON OCRD.SlpCode = OSLP.SlpCode
    LEFT JOIN RCT2 ON ORCT.DocEntry = RCT2.DocNum
    LEFT JOIN OINV ON RCT2.DocEntry = OINV.DocEntry
WHERE
    InitObjTyp = 24
    AND OITR.Canceled = 'N'
    AND ORCT.Canceled = 'N'
    AND ORCT.DocDate >= [%0]
    AND ORCT.DocDate <= [%1]
    AND OINV.DocEntry IS NULL;