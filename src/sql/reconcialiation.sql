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
    AND OINV.DocEntry IS NULL;