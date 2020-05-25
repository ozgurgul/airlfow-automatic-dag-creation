# airlfow-automatic-dag-creation

## Automatic Airflow DAG creation
This process will automate the DAG creation where the need is to put all the tasks of the analysis in a DAG which will act as a template. These tasks are the same every time. This situation most certainly arises when you are analysing/querying the data and you must store the output into a table for the views towards your dashboard or you just want to append the data into the already present table as a part of periodic batch processing. By the way, DAG is a Direct Acyclic Graph. It is a collection of tasks in a workflow that needs to be executed sequentially or simultaneously.
To begin with, there are 3 types of files associated in this process:
- DAG definition (.json)
- DAG template (.py)
- DAG creator (.py)


<b>load_rule_definition</b>: This will load the DAG definition from [IV_RuleDefinitions] table
<b>list_transcripts</b>: list the (UID) transcripts of last 2 days and this is the SLA. [Historical Data Processing] Override it with the Airflow variable: ‘module_conversion_look_back_days’
<b>compute_next_gather</b>: Based on list (rule definition) keys, this will figure it out which next runs are to be executed and hence will push ‘from_date_time’ and ‘to_date_time’ to further (categorisation) tasks
<b>create_staging_table</b>: Create the staging table (and immediately tsvector index them)
<b>run_queries</b>: Execute the categorisation queries (based on date+time computed above)
<b>unload_staging</b>: Unload the staging table into [ods_va_textcategorisation] table
<b>extract_last_batch_date_time</b>: From [voa] schema, this will calculate max(batch_date+time)
<b>list_transcripts_for_table</b>: List (categorisied) transcripts
<b>transcript_to_final</b>: Based on list transcript keys and last batch_date+time, this will figure it out what transcript values to be pushed into the final table
<b>voiceanalytics_analyze</b>: Execute VoiceAnalytics analyze



https://towardsdatascience.com/automatic-airflow-dag-creation-for-data-scientists-and-analysts-17d24a9884ba
