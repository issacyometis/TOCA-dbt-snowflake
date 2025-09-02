with 
    cte_source as (select Distinct billiteminfos, siteID  from {{ source('epos', 'sales_api') }}),

    cte_epos_detail as (

    select 
    f.value:EPOSTransactionItemID::int as EPOS_Transaction_Item_ID,
    f.value:EPOSTransactionID::int as EPOS_Transaction_ID,
    f.value:QtySold::float as Qty_Sold,
    f.value:UnitQuantity::float as Unit_Quantity,
    f.value:OfSku::float as OF_SKU,
    f.value:CostPrice::float as Cost_Price,
    f.value:ExpectedGrossAmt::float as Expected_Gross_Amt,
    f.value:ActualGrossAmt::float as Actual_Gross_Amt,
    f.value:TaxableNetAmt::float as Taxable_Net_Amt,
    f.value:TaxAmt::float as Tax_Amt,
    f.value:ProductID::int as Product_ID,
    f.value:ProductName::String as Product_Name,
    f.value:ProductGroupID::int as Product_Group_ID,
    f.value:ProductGroupName::String as Product_Group_Name,
    f.value:ProductTypeID::int as Product_Type_ID,
    f.value:ProductTypeName::String as Product_Type_Name,
    f.value:PriceGroupID::int as Price_Group_ID,
    f.value:PriceGroupName::String as Price_Group_Name,
    f.value:CostCenter::String as Cost_Center,
    f.value:SessionName::String as Session_Name,
    f.value:EmployeeWhoSoldID::int as Employee_Who_Sold_ID,
    f.value:EmployeeWhoSoldName::string as Employee_Who_Sold_Name,
    f.value:EnterDateTime::DateTime as Enter_Date_Time,
    f.value:IsVoided::string as Is_Voided,
    f.value:VoidReason::int as Void_Reason,
    f.value:EmployeeWhoVoidedID::int as Employee_Who_Voided_ID,
    f.value:EmployeeWhoVoidedName::string as Employee_Who_Voided_Name,
    f.value:VoidDateTime::DateTime as Void_Date_Time,
    f.value:IsErrorCorrect::string as Is_Error_Correct,
    f.value:IsOption::string as Is_Option,
    f.value:IsSold::string as Is_Sold,
    f.value:IsRefund::string as Is_Refund,
    f.value:IsRefundReturnToStock::string as Is_Refund_Return_To_Stock,
    --f.value:DiscountInfos::string as Discount_Infos 
    cte_source.siteID  
     from cte_source, table(flatten(billiteminfos)) f

    )

  

select 
*
 from cte_epos_detail

