import marimo

__generated_with = "0.9.20"
app = marimo.App(width="medium")


@app.cell
def __():
    import duckdb
    import marimo as mo
    import polars as pl
    return duckdb, mo, pl


@app.cell
def __(mo):
    mo.md(
        r"""
        <h1>Introduction and Notes</h1>

        Hi! I'm Tyler - this is my submission of the take-home assessment for a senior analytics engineering position at Fetch.

        This work was done largely using DuckDB as my SQL engine, with a tiny bit of polars for some quick EDA in the data quality assessment section.

        Thanks for reviewing!

        <h2>Notes</h2>
        Question 1 - For the users gzipped json, I'm not sure if this was intentionally the case or not, but the json was malformed so I manually unzipped that one and removed the malformed characters at the beginning and end of the file. That's why you'll notice it's being imported from an unzipped version.

        Question 2 - Some thoughts are left in code comments per query - I also did not spend any time prettying the output of these queries (I wouldn't share them with business stakeholders in this form).

        Question 3 - Since I feel like it'd be really easy to go overboard on this section, I time gated myself to forty five minutes to work on it and write some thoughts.

        Question 4 - Similar to the above, I time gated myself to thirty minutes to work on this.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        <h1>Question 1</h1>
        <b>First: Review Existing Unstructured Data and Diagram a New Structured Relational Data Model</b>


        Review the 3 sample data files provided below. 

        Develop a simplified, structured, relational diagram to represent how you would model the data in a data warehouse. The diagram should show each table’s fields and the joinable keys. You can use pencil and paper, readme, or any digital drawing or diagramming tool with which you are familiar. If you can upload the text, image, or diagram into a git repository and we can read it, we will review it!
        """
    )
    return


@app.cell
def __(mo):
    mo.md("""<h2>ER Diagram</h2>""")
    return


@app.cell
def __(mo):
    mo.image("Fetch_ReceiptsData.drawio.png")
    return


@app.cell
def __(mo):
    mo.md(r"""<h1>Brand</h1>""")
    return


@app.cell
def __(duckdb, read_json_auto):
    duckdb.sql("select * from read_json_auto('raw_data/brands.json.gz')").show(max_width=1000)
    return


@app.cell
def __(brand, duckdb):
    ctas_brand = (
        """
        create or replace table brand as 
            select 
                _id['$oid'] as brand_id,
                barcode as barcode,
                brandCode as brand_code,
                category as category,
                categoryCode as category_code,
                cpg['$id']['$oid'] as cpg_id,
                topBrand as top_brand,
                name as name
            from read_json_auto('raw_data/brands.json.gz')
        """
    )

    duckdb.sql(ctas_brand)
    duckdb.sql("select * from brand").show(max_width=1000)
    return (ctas_brand,)


@app.cell
def __(mo):
    mo.md(r"""<h1>User</h1>""")
    return


@app.cell
def __(duckdb, read_json_auto):
    duckdb.sql("select * from read_json_auto('raw_data/users.json')").show(max_width=1000)
    return


@app.cell
def __(duckdb, user):
    #the make_timestamp function uses microseconds but it looks like mongodb (or wherever this data is coming from) is using milliseconds - need to multiply by 10^3
    ctas_user = (
        """
        create or replace table user as 
            select 
                _id['$oid'] as user_id,
                state as state,
                make_timestamp(createdDate['$date'] * 1000) as created_datetime,
                make_timestamp(lastLogin['$date'] * 1000) as last_login_datetime,
                role as role,
                active as active,
                signUpSource as sign_up_source
            from read_json_auto('raw_data/users.json')
        """
    )


    duckdb.sql(ctas_user)

    duckdb.sql("select * from user").show(max_width=1000)
    return (ctas_user,)


@app.cell
def __(mo):
    mo.md(r"""<h1>Receipt</h1>""")
    return


@app.cell
def __(duckdb, read_json_auto):
    duckdb.sql("select * from read_json_auto('raw_data/receipts.json.gz')").show(max_width=1000)
    return


@app.cell
def __(duckdb, receipt):
    #the make_timestamp function uses microseconds but it looks like mongodb (or wherever this data is coming from) is using milliseconds - need to multiply by 10^3
    ctas_receipt = (
        """
        create or replace table receipt as 
            select 
                _id['$oid'] as receipt_id,
                bonusPointsEarned as bonus_points_earned,
                bonusPointsEarnedReason as bonus_points_earned_reason,
                make_timestamp(createDate['$date'] * 1000) as created_datetime,
                make_timestamp(dateScanned['$date'] * 1000) as scanned_datetime,
                make_timestamp(finishedDate['$date'] * 1000) as finished_datetime,
                make_timestamp(modifyDate['$date'] * 1000) as modified_datetime,
                make_timestamp(pointsAwardedDate['$date'] * 1000) as points_awarded_datetime,
                pointsEarned::float as points_earned,
                make_timestamp(purchaseDate['$date'] * 1000) as purchased_datetime,
                purchasedItemCount as purchased_item_count,
                rewardsReceiptStatus as rewards_receipt_status,
                totalSpent::float as total_spent,
                userId as user_id
            from read_json_auto('raw_data/receipts.json.gz')
        """
    )


    duckdb.sql(ctas_receipt)

    duckdb.sql("select * from receipt").show(max_width=1000)
    return (ctas_receipt,)


@app.cell
def __(mo):
    mo.md(r"""<h1>Receipt Item</h1>""")
    return


@app.cell
def __(duckdb, read_json_auto):
    duckdb.sql("""
        create or replace temporary table receiptitems_unnested as
            select 
                _id, 
                unnest(rewardsReceiptItemList) as individual_item 
            from read_json_auto('raw_data/receipts.json.gz')
    """)
    return (receiptitems_unnested,)


@app.cell
def __(duckdb, receiptitem):
    duckdb.sql("create or replace sequence receiptitemid_seq start 1")

    #the make_timestamp function uses microseconds but it looks like mongodb (or wherever this data is coming from) is using milliseconds - need to multiply by 10^3
    ctas_receiptitem = (
        """
        create or replace table receiptitem as 
            select
                nextval('receiptitemid_seq') as receipt_item_id,
                _id['$oid'] as receipt_id,
                individual_item['barcode'] as barcode,
                individual_item['description'] as description,
                individual_item['finalprice']::float as final_price,
                individual_item['itemprice']::float as item_price,
                individual_item['targetprice']::float as target_price,
                individual_item['discounteditemprice']::float as discounted_item_price,
                individual_item['priceaftercoupon']::float as price_after_coupon,
                individual_item['originalfinalprice']::float as original_final_price,
                individual_item['needsfetchreview'] as needs_fetch_review,
                individual_item['needsfetchreviewreason'] as needs_fetch_review_reason,
                individual_item['partneritemid'] as partner_item_id,
                individual_item['preventtargetgappoints'] as prevent_target_gap_points,
                individual_item['quantitypurchased'] as quantity_purchased,
                individual_item['userflaggedbarcode'] as user_flagged_barcode,
                individual_item['userflaggednewitem'] as user_flagged_new_item,
                individual_item['userflaggedprice']::float as user_flagged_price,
                individual_item['userflaggedquantity'] as user_flagged_quantity,
                individual_item['userflaggeddescription'] as user_flagged_description,
                individual_item['pointsearned'] as points_earned,
                individual_item['pointsnotawardedreason'] as points_not_awarded_reason,
                individual_item['pointspayerid'] as points_payer_id,
                individual_item['rewardsgroup'] as rewards_group,
                individual_item['rewardsproductpartnerid'] as rewards_product_partner_id,
                individual_item['originalmetabritebarcode'] as original_metabrite_barcode,
                individual_item['originalmetabritedescription'] as original_metabrite_description,
                individual_item['originalmetabritequantitypurchased'] as original_metabrite_quantity_purchased,
                individual_item['originalmetabriteitemprice']::float as original_metabrite_item_price,
                individual_item['metabritecampaignid'] as metabrite_campaign_id,
                individual_item['brandcode'] as brand_code,
                individual_item['competitorrewardsgroup'] as competitor_rewards_group,
                individual_item['originalreceiptitemtext'] as original_receipt_item_text,
                individual_item['itemnumber'] as item_number,
                individual_item['competitiveproduct'] as competitive_product,
                individual_item['deleted'] as deleted
            from receiptitems_unnested
        """
    )


    duckdb.sql(ctas_receiptitem)
    duckdb.sql("select * from receiptitem").show(max_width=1000)
    return (ctas_receiptitem,)


@app.cell
def __(mo):
    mo.md(r"""<h2>Save to parquet</h2>""")
    return


@app.cell
def __(duckdb):
    #save them to parquet

    tables = ['receipt', 'receiptitem', 'brand', 'user']

    for item in tables: 
        copy_query = (f"""
            copy 
                (select * from {item})
                to 'processed_data/{item}.parquet'
                (format 'parquet');
        """)
        duckdb.sql(copy_query)
    return copy_query, item, tables


@app.cell
def __(mo):
    mo.md(
        r"""
        <h1>Question 2</h1>

        <b>Second: Write queries that directly answer predetermined questions from a business stakeholder</b>


        Write SQL queries against your new structured relational data model that answer at least two of the following bullet points below of your choosing. Commit them to the git repository along with the rest of the exercise.

        Note: When creating your data model be mindful of the other requests being made by the business stakeholder. If you can capture more than two bullet points in your model while keeping it clean, efficient, and performant, that benefits you as well as your team.

        - What are the top 5 brands by receipts scanned for most recent month?
        - How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
        - When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
        - When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
        - Which brand has the most spend among users who were created within the past 6 months?
        - Which brand has the most transactions among users who were created within the past 6 months?
        """
    )
    return


@app.cell
def __(mo):
    mo.md("""<h2> 1. What are the top 5 brands by receipts scanned for most recent month? </h2>""")
    return


@app.cell
def __(duckdb, receipt, receiptitem):
    duckdb.sql("select max(scanned_datetime) from receipt") #March 1st 2021 - I'm going use February as "most recent month" since it's the most recent full month

    duckdb.sql("""
        select 
            ri.brand_code,
            count(distinct r.receipt_id) as number_receipts_scanned_with_brand
        from receiptitem as ri
        inner join receipt as r 
            on r.receipt_id = ri.receipt_id
        --left join brand as b
        --    on b.brand_code = ri.brand_code
        where 
            r.scanned_datetime between '2021-02-01' and '2021-02-28'
            --and ri.brand_code is not null
        group by 1
        order by 2 desc
    """)
    return


@app.cell
def __(mo):
    mo.md(r"""<h2>2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?</h2>""")
    return


@app.cell
def __(duckdb, receipt, receiptitem):
    duckdb.sql("""
        select 
            ri.brand_code,
            count(distinct r.receipt_id) as number_receipts_scanned_with_brand
        from receiptitem as ri
        inner join receipt as r 
            on r.receipt_id = ri.receipt_id
        --left join brand as b
        --    on b.brand_code = ri.brand_code
        where 
            r.scanned_datetime between '2021-01-01' and '2021-01-31'
            --and ri.brand_code is not null
        group by 1
        order by 2 desc
        limit 5
    """)
    return


@app.cell
def __(mo):
    mo.md(r"""<h2>3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?</h2>""")
    return


@app.cell
def __(duckdb, receipt):
    duckdb.sql("""
        select 
            avg(total_spent)
        from receipt
        where rewards_receipt_status = 'Accepted'
    """).show()#There are no receipts with value of 'Accepted', but I think 'FINISHED' would be the same idea, right? At least report on both

    duckdb.sql("""
        select 
            avg(total_spent)
        from receipt
        where rewards_receipt_status = 'FINISHED'
    """).show()#$80.85


    duckdb.sql("""
        select 
            avg(total_spent)
        from receipt
        where rewards_receipt_status = 'REJECTED'
    """).show()#$23.33

    #Finished is higher!
    return


@app.cell
def __(mo):
    mo.md(r"""<h2>4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?</h2>""")
    return


@app.cell
def __(duckdb, receipt):
    duckdb.sql("""
        select 
            avg(purchased_item_count)
        from receipt
        where rewards_receipt_status = 'Accepted'
    """).show() #There are no receipts with value of 'Accepted', but I think 'FINISHED' would be the same idea, right? At least report on both

    duckdb.sql("""
        select 
            avg(purchased_item_count)
        from receipt
        where rewards_receipt_status = 'FINISHED'
    """).show() #$~16


    duckdb.sql("""
        select 
            avg(purchased_item_count)
        from receipt
        where rewards_receipt_status = 'REJECTED'
    """).show() #~2

    #Finished is MUCH higher!
    return


@app.cell
def __(mo):
    mo.md(r"""<h2>5. Which brand has the most spend among users who were created within the past 6 months?</h2>""")
    return


@app.cell
def __(duckdb, receipt, receiptitem, user):
    # Again, using "2021-03-01" as the anchor for "past 6 months"

    duckdb.sql("""
        select 
            ri.brand_code,
            round(sum(ri.final_price),2) as total_spend_among_users_created_within_last_six_months
        from receiptitem as ri
        inner join receipt as r
            on r.receipt_id = ri.receipt_id
        inner join user as u
            on u.user_id = r.user_id
            and u.created_datetime >= '2020-09-01'
        group by 1
        order by 2 desc
    """)
    return


@app.cell
def __(mo):
    mo.md(r"""<h2>6. Which brand has the most transactions among users who were created within the past 6 months?</h2>""")
    return


@app.cell
def __(duckdb, receipt, receiptitem, user):
    # Again, using "2021-03-01" as the anchor for "past 6 months"
    # I feel like there's a few ways to reasonably define "transaction" - for my purposes, I just defined a transaction as a receipt


    duckdb.sql("""
        select 
            ri.brand_code,
            count(distinct ri.receipt_id) as total_transactions_among_users_created_within_last_six_months
        from receiptitem as ri
        inner join receipt as r
            on r.receipt_id = ri.receipt_id
        inner join user as u
            on u.user_id = r.user_id
            and u.created_datetime >= '2020-09-01'
        group by 1
        order by 2 desc
    """)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        <h1>Question 3</h1>

        <b>Third: Evaluate Data Quality Issues in the Data Provided</b>


        Using the programming language of your choice (SQL, Python, R, Bash, etc...) identify as many data quality issues as you can. We are not expecting a full blown review of all the data provided, but instead want to know how you explore and evaluate data of questionable provenance.

        Commit your code and findings to the git repository along with the rest of the exercise.
        """
    )
    return


@app.cell
def __(mo):
    mo.md(r"""<h2>Brief EDA - null checks</h2>""")
    return


@app.cell
def __(duckdb, receipt):
    receipt_df = duckdb.sql("select * from receipt").pl()
    (receipt_df.null_count() / receipt_df.height * 100)
    return (receipt_df,)


@app.cell
def __(duckdb, receipt):
    duckdb.sql("""
        select
            sum(case 
                when bonus_points_earned is null and bonus_points_earned_reason is not null
                    or bonus_points_earned is not null and bonus_points_earned_reason is null
                then 1 else 0 end) as dq_issue_count_bonus_points,
            sum(case 
                when points_earned is not null and points_awarded_datetime is null
                then 1 else 0 end) as dq_issue_points_awarded --this may not be a DQ issue, but given I don't have defs for awarded vs earned, I'm unsure
        from receipt   
    """)
    return


@app.cell
def __(duckdb, receipt, user):
    duckdb.sql("""
        select count(*)
        from receipt 
        left join user
            on user.user_id = receipt.user_id
        where user.user_id is null
        """)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        Thoughts on Receipt Data re: Quality - 

        - It's odd to me the high null % for purchased item count as well as purchased datetime - I can kind of assume that it's because of poor quality scans from customer receipts? But it's still odd that customers wouldn't be required to manually input that information if the data couldn't be parsed from their image or if there was low confidence in the data parsed via scan. 
        - Not sure without definitions for how things are defined at Fetch, but the 72 receipt instances where there are points earned but no points awarded datetime associated seems worth investigating further
        - There are 148 users associated with reciepts that are not found in the user data - that's not good!

        """
    )
    return


@app.cell
def __(brand, duckdb):
    brand_df = duckdb.sql("select * from brand").pl()
    (brand_df.null_count() / brand_df.height * 100)
    return (brand_df,)


@app.cell
def __(brand, duckdb):
    duckdb.sql("""
        select 
            distinct category_code 
        from brand
    """).show()

    duckdb.sql("""
        select 
            distinct category
        from brand
    """).show()
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        Thoughts on Brand Data re: Quality - 

        - It's very odd to me that the brand_code field has over 20% nulls given that it appeared to be the best way to link brand data with items.
        - It's odd that `top_brand` has so many nulls despite being a simple boolean (might just be a case where it'd make sense to impute false).
        - The fact that `category_code` is more sparsely populated vs. `category` despite seemingly being a superset of the available categories is concerning.
        """
    )
    return


@app.cell
def __(duckdb, user):
    user_df = duckdb.sql("select * from user").pl()
    (user_df.null_count() / user_df.height * 100)
    return (user_df,)


@app.cell
def __(duckdb, user):
    duckdb.sql("""
        select 
            count(*)
        from user
        where role = 'consumer'
        and sign_up_source is null
    """)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        Thoughts on User Data re: Quality - 

        - The 9 cases where a consumer user doesn't have a `sign_up_source` suggests that there might be something wrong in the registration/sign-up funnel for new users. 
        """
    )
    return


@app.cell
def __(duckdb, receiptitem):
    receiptitem_df = duckdb.sql("select * from receiptitem").pl()
    (receiptitem_df.null_count() / receiptitem_df.height * 100)
    return (receiptitem_df,)


@app.cell
def __(duckdb):
    duckdb.sql("""
        select 
        from receiptitem
        where barcode is null or barcode = '4011'
    """)
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        Thoughts on Receipt Item Data re: Quality - 

        - The ~55% null on `barcode` is really high - especially when there seems to be a value for item not found (4011).
        - There's a crazy amount of seemingly deprecated fields and, going based on field names alone, it can be difficult to understand which fields are being populated today and what is worth reporting on.
        - The lack of `brand_code` data suggests either the majority of items being submitted are "low quality" (i.e., hard to define) or are outside of the Fetch's "supported" brands
        - I find it odd that the `deleted` field is largely null - likely another case where the default might be that it's ok to impute false?
        """
    )
    return


@app.cell
def __(mo):
    mo.md(r"""<h2>Investigating issues/questions arising from Question 2</h2>""")
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        <h3> What's the deal with the connection between brand and receipt item?</h3>
        Am I supposed to join via `brand_code` or `barcode`? (`brand_code` makes conceptually a lot more sense)

        From the output below, it seems like `brand_code` is best but it still isn't great (~10% of all receipt items). It's also weird that only 41 out of 227 total brand codes found on a reciept item are actually found in the brand data. Makes me think these data are incomplete, outdated, or being used here incorrectly. 
        """
    )
    return


@app.cell
def __(brand, duckdb, receiptitem):
    duckdb.sql("""
        select count(*)
        from receiptitem as ri
        inner join brand as b
            on b.brand_code = ri.brand_code
    """) #635 rows
    return


@app.cell
def __(brand, duckdb, receiptitem):
    duckdb.sql("""
        select 
            count(distinct receiptitem.brand_code) as total_brand_codes,
            count(distinct case when brand.brand_code is null then receiptitem.brand_code else null end) as total_brand_codes_without_brand_info
        from receiptitem
        left join brand 
            on brand.brand_code = receiptitem.brand_code
    """)
    return


@app.cell
def __(brand, duckdb, receiptitem):
    duckdb.sql("""
        select count(*)
        from receiptitem as ri
        inner join brand as b
            on b.barcode = ri.barcode
            or b.barcode = ri.user_flagged_barcode
    """) #89 rows
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        <h1>Question 4</h1>

        <b>Fourth: Communicate with Stakeholders</b>


        Construct an email or slack message that is understandable to a product or business leader who isn’t familiar with your day to day work. This part of the exercise should show off how you communicate and reason about data with others. Commit your answers to the git repository along with the rest of your exercise.

        - What questions do you have about the data?
        - How did you discover the data quality issues?
        - What do you need to know to resolve the data quality issues?
        - What other information would you need to help you optimize the data assets you're trying to create?
        - What performance and scaling concerns do you anticipate in production and how do you plan to address them?
        """
    )
    return


@app.cell
def __(mo):
    mo.md(
        r"""
        In this slack message below, I'm communicating with a fictional PM named Pam.

        ---

        Hey Pam - wanted to get back to you on the data one of your engineers had sent over for inclusion in our data warehouse.

        I spent some time analyzing what was provided and had a few questions:

        - Do you know where we're pulling brand data from currently? Is there someone on the business ops team that collates this data from our partners - I'd love to better understand how this data is generated today and used in production. I ask because the data received from your engineer seemed either outdated or incomplete as there were a large number of brands (186) found in receipt scans that were not found in our brand data. I'm wondering if there might be a process we could come up whereby this brand data is automatically updated in production as well as downstream for analytics use.
        - I noticed while looking at the data associated with individual items found on receipts that there are a number of fields that seemed to be old and perhaps deprecated on the product side - I was wondering if you might have a list of fields that are still being populated today or if I could pair with you or an engineer on your team to get a better idea of how the data is generated by a user in the source system. This also makes me wonder what how important historical reporting is for your team's use cases - if it's possible that we could ignore some of these deprecated fields based on the time windows your team is interested it, that could help us reduce the complexity on our end and potentially increase performance. This potential performance/scaling issue is also augmented by the fact that there can be a very large number of items associated per receipt.
        - When looking at the receipt and user data, I did notice that there was a seemingly large number (148) of users associated with receipts that did not exist in the user data - I'm curious how that might have occurred, perhaps the export provided for user data was outdated? It's probably worth investigating to see if this issue exists in the production system as well. Additionally, there was a small number of cases (9) where a consumer user didn't have a correctly populated sign-up source, which suggested to me that there might be/have been an issue in the registration funnel for new users that might need to be investigated - curious if there was a past issue that might explain this or who I should reach out to on your team to dig a bit deeper.
        - More generally, I'm also curious how best we could use these data to support you and your team - what metrics or user behavior are most relevant to what's coming up on your team's product roadmap? I want to make sure that we take these into account to build data models that can be optimized to what will help propel your team forward.

        Let me know if you have any insights on the above and, if needed, happy to set up time to talk through it more in depth with you and any engineers on your team!

        Thanks!
        """
    )
    return


if __name__ == "__main__":
    app.run()
