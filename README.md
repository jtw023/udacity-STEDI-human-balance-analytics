# Udacity STEDI Human Balance Analytics Project

### Explanation

The first thing I wanted to do was familiarize myself with the data.
I immediately discovered that these files are not actually valid JSON.
Apparently everything is fine upon uploading one to S3 and doing a test glue job.
Everything else looked fine.

I then uploaded all of the json into my s3 bucket at accelerometer/landing,
customers/landing, and step_trainer/landing.

Onto my next step which was creating glue databases and tables
for this newly uploaded landing data. Here is where I encountered something
that I will change on future ETL jobs. I decided that in an effort to keep
everything organized I would create different databases for each file set.
For example, one for customers/landing, one for accelerometer/landing, and one
for step_trainer/landing. Same for trusted and curated. In total, I have 8 databases
and tables in glue right now. This means that I can't check the join logic as easily.
With simple datasets like these I lucked out but production datasets are not so straightforward.

After creating databases, tables, and getting the landing stuff queryable in Athena I
moved forward to start transforming the data. customer_landing to customer_trusted is
just finding users who have allowed their data to be shared with research.
I decided to use a filter where `shareWithResearchAsOfDate != 0`. Next was the
accelerometer data. I accomplished this one by inner joining customer landing
and accelerometer landing on `customer_landing.email = accelerometer_landing.user`,
then moved on to transform with a SQL statement before putting it into my  bucket.
```SQL
    SELECT
        myDataSource.user,
        myDataSource.timestamp,
        x,
        y,
        z
    FROM myDataSource
    WHERE sharewithresearchasofdate IS NOT NULL
```

There was an interesting excerpt in the middle that I wasn't sure exactly why it was there.
> Data Scientists have discovered a data quality issue with the Customer Data.
> The serial number should be a unique identifier for the STEDI Step Trainer they purchased.
> However, there was a defect in the fulfillment website, and it used the same 30
> serial numbers over and over again for millions of customers! Most customers have not
> received their Step Trainers yet, but those who have, are submitting Step Trainer
> data over the IoT network (Landing Zone).
> The data from the Step Trainer Records has the correct serial numbers.
> The problem is that because of this serial number bug in the fulfillment data (Landing Zone),
> we don’t know which customer the Step Trainer Records data belongs to.

I figured there was something more that I needed to do here but found nothing amiss.

From Athena, I went to the customer_landing and step_trainer_landing tables
which both have serial numbers and I did a `SELECT COUNT(DISTINCT serialnumber) FROM table`.

customer_landing returns 956:
```SQL
SELECT COUNT(DISTINCT serialnumber) FROM customer_landing
```

step_trainer_landing also returns 956:
```SQL
SELECT COUNT(DISTINCT serialnumber) FROM step_trainer_landing
```

Now I'm very confused and don't understand the point of that last statement but I press on.

The next task was sanitizing customer_trusted further to only include those with accelerometer data.
For this I took the customer_trusted data and the accelerometer_trusted data from above
and inner joined them on `customer_trusted.email = accelerometer_trusted.user`.
I then did a drop fields transform to drop the fields from the accelerometer_trusted table.
Finally, the last step of the transformation process was a drop duplicates to only return
1 row per customer before putting this into my customer_curated s3 directory.

The last two Glue jobs requested are where it got a bit weird for me. Despite the validating
that I did above, step_trainer_landing has no way to be joined up to accelerometer_landing or customer_landing.
Everytime I try joining, it returns nothing. I made a Glue job to test all the joins
and even looked at the screenshot diagram in the instructions to verify my findings but nothing works.
I tried joining step_trainer_landing to accelerometer_landing via `step_trainer.sensorreadingtime = accelerometer.timestamp`.
I also tried joining step_trainer_landing to customer_landing via `step_trainer.serialnumber = customer.serialnumber`.

Since neither of the step_trainer_landing joins worked, I was unable to get a step_trainer_trusted or a machine_learning_curated.
However, all the other checks were correct.

In the "Check your work!" section it says
After each stage of your project, check if the row count in the produced table is correct. You should have the following number of rows in each table:

    * Landing
        * Customer: 956
        * Accelerometer: 81273
        * Step Trainer: 28680
    * Trusted
        * Customer: 482
        * Accelerometer: 40981
        * Step Trainer: 14460
    * Curated Customer: 482
    * Machine Learning: 43681

Going back to Athena I ran:
```SQL
SELECT COUNT(*) FROM customer_landing
```
    Output: 956

```SQL
SELECT sharewithresearchasofdate FROM customer_landing WHERE sharewithresearchasofdate IS NULL
```
    Output: 474

```SQL
SELECT COUNT(*) FROM accelerometer_landing
```
    Output: 81273

```SQL
SELECT COUNT(*) FROM step_trainer_landing
```
    Output: 28680

```SQL
SELECT COUNT(*) FROM customer_trusted
```
    Output: 482

```SQL
SELECT * FROM customer_trusted WHERE sharewithresearchasofdate IS NULL
```
    Output: 0

```SQL
SELECT COUNT(*) FROM accelerometer_trusted
```
    Output: 40981

```SQL
SELECT COUNT(*) FROM customer_curated
```
    Output: 482


# PII Discusions

Emails are PII but since they are being used to join the
customers table with the accelerometer table I can't really
filter that in any way. Normally I would filter an email field
down to just the domain if it's unencrypted in a database but
that makes it unjoinable. I did, however, show just first and
last initial instead of the full name.

This is horrible database design. At least hash the email and join on that.
This way you can keep a hash in your landing table as well as the encrypted
email so that you can still find it in plain text if needed but for analytics
there will only ever be the hash and that hash can be used to join.
