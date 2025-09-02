with 
    cte_source as (select * from {{ source('epos', 'sales_api') }}),

    cte_epos_header as (
 select Distinct
        H.EPOSTransactionID as EPOS_Transaction_ID,
        CASE WHEN to_time(H.SaleDate) < '05:00:00' then to_date(H.SaleDate)-1 else to_date(H.SaleDate) end as Business_Date,
        to_date(H.SaleDate) as Sale_Date,
        to_time(H.SaleDate) as Sale_time,
        H.CoverCount as Cover_Count,
        H.TableNumber as Table_Number,
        H.RevenueCenter as Revenue_Center,
        H.ActualGrossAmt as Actual_Gross_Amt,
        H.ActualNetAmt as Actual_Net_Amt,
        H.TaxableNetAmt as Taxable_Net_Amt,
        H.TaxAmt as Tax_Amt,
        H.ServiceChargeAmt as Service_ChargeAmt,
        H.GratuityAmt as Gratuity_Amt,
        H.TotalDiscountAmount as Total_Discount_Amount,
        H.OpenDateTime as Open_Date_Time,
        H.ClosedDateTime as Closed_Date_Time,
        TIMEDIFF(minute, H.OpenDateTime, H.ClosedDateTime) as Transaction_Taken_Time,
        H.EmployeeOpenedBillID as Employee_Opened_Bill_ID,
        H.EmployeeOpenedBill as Employee_Opened_Bill,
        H.EmployeeClosedBillID as Employee_Closed_Bill_ID,
        H.EmployeeClosedBill as Employee_Closed_Bill,
        H.BillPaid as Bill_Paid,
        H.BillTransferred as H_Bill_Transferred,
        H.EmployeeWhoTransferredBillID as Employee_Who_Transferred_Bill_ID,
        H.EmployeeWhoTransferredBillName as Employee_Who_Transferred_Bill_Name,
        H.TransferDateTime as Transfer_Date_Time,
        H.CRMMemberID as CRM_Member_ID,
        ifnull(H.siteID, 3957) as siteID
        from cte_source as H
    )

    select * from cte_epos_header 