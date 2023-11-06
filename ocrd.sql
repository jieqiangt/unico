SELECT
    CONVERT(char, OCRD.CreateDate, 112) AS 'create_date',
	CONVERT(char, OCRD.UpdateDate, 112) AS 'update_date',
    CardCode AS 'bp_code',
    CardName AS 'bp_name',
    (
        CASE
            WHEN CardType = 'S' THEN 'vendor'
            ELSE 'customer'
        END
    ) AS 'bp_type',
    CmpPrivate AS 'bp_entity_type',
    Address AS 'billing_address',
    ZipCode AS 'billing_zip_code',
    MailAddres AS 'mailing_address',
    MailZipCod AS 'mailing_zip_cod',
    OCTG.PymntGroup AS 'terms_of_payment',
    OCTG.ExtraDays AS 'extra_payment_days',
    OCRG.GroupName AS 'category',
    FatherCard AS 'parent_bp',
    FatherType AS 'parent_type',
    (
        CASE
            WHEN validFor = 'N' THEN 0
            ELSE 1
        END
    ) AS 'active_ind',
    OIDC.Name AS 'indicator_to_be_set',
    ConnBP AS 'connected_bp',
    Industry AS 'industry_group',
    Business AS 'business_type',
    (
        CASE
            WHEN IsDomestic = 'N' THEN 0
            ELSE 1
        END
    ) AS 'domestic_ind',
    (
        CASE
            WHEN IsResident = 'N' THEN 0
            ELSE 1
        END
    ) AS 'resident_ind',
    OOND.IndName AS 'industry',
    OOND.IndDesc AS 'industry_desc',
    OVTP.VendorType AS 'vendor_type',
    OVTP.Descript AS 'vendor_type_desc'
FROM
    OCRD
    LEFT JOIN OCTG ON OCRD.GroupNum = OCTG.GroupNum
    LEFT JOIN OCRG ON OCRD.GroupCode = OCRG.GroupCode
    LEFT JOIN OIDC ON OCRD.Indicator = OIDC.Code
    LEFT JOIN OOND ON OCRD.IndustryC = OOND.IndCode
    LEFT JOIN OVTP ON OCRD.VendTID = OVTP.ABSEntry