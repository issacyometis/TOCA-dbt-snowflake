with 
    cte_source as (select * from {{ source('epos', 'sales_api') }}),

    cte_epos_discount as (

select 
  
    f.value:EPOSTransactionItemID::int as EPOS_Transaction_Item_ID,
    f.value:EPOSTransactionID::int as EPOS_Transaction_ID,
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
    replace(replace(f.value:DiscountInfos::string, '[', ''),']','') as DiscountInfos
     from cte_source, table(flatten(billiteminfos)) f 
    where  f.value:DiscountInfos::string  <> '[]'
    ) , 
cte_main as (
    
    select  
    EPOS_Transaction_ID, 
    EPOS_Transaction_Item_ID, 
    Product_ID, 
    try_parse_json(DiscountInfos):DiscountReasonID::int as Discount_Reason_ID,
    try_parse_json(DiscountInfos):DiscountReasonDesc:string as Discount_Reason,
    try_parse_json(DiscountInfos):DiscountAmt::float as Discount_Amt,
    try_parse_json(DiscountInfos):DiscountApprovedByEmployee::string as Discount_Approved_By_Employee,
    try_parse_json(DiscountInfos):DiscountApprovalDateTime::DAteTime as Discount_Approval_Date_Time
    from cte_epos_discount

) 

select * from cte_main 
