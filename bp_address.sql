SELECT
    CardCode AS 'bp_code',
    CardName as 'bp_name',
    CardType AS 'bp_type',
    validFor AS 'is_active',
    frozenFor AS 'is_frozen',
    Deleted AS 'is_deleted',
    PaymBlock AS 'is_payment_block',
    [Address] AS 'sap_address',
    [Block] AS 'sap_block_street',
    Building AS 'sap_building',
    StreetNo AS 'sap_street_no',
    City AS 'sap_city',
    ZipCode AS 'sap_zipcode'
FROM
    OCRD