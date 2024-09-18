SELECT
    OVPM.DocDate AS 'Payment Date',
    OVPM.DocNum AS 'Payment Document Number',
    OVPM.CardCode AS 'Card Code',
    OVPM.CardName AS 'Card Name',
    OVPM.DocTotal AS 'Total Payment Amount',
    ReceiptNum AS 'Payment Receipt Number',
    OPCH.DocDate AS 'AP Invoice Date',
    OPCH.DocNum AS 'AP Invoice Number',
    OPCH.DocTotal AS 'AP Invoice Amount'
FROM
    OVPM
    LEFT JOIN OPCH ON OVPM.DocEntry = OPCH.ReceiptNum
WHERE
    OVPM.DocDate > [%0]
    AND [%1]
    AND OVPM.Canceled = 'N'
    AND OPCH.Canceled = 'N'