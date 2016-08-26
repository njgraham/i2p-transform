''' Airflow DAG definition
'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from time import sleep


def run_sql(sql_file):
    ''' Pretend to run the .sql file for now
    '''
    print 'Starting %s' % sql_file
    sleep(5)
    s = 'Finished %s' % sql_file
    print s
    return s  # Return is printed in the log

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG('PCORNetCDM', default_args=default_args, schedule_interval=None)

common = PythonOperator(
    task_id='common',
    python_callable=run_sql,
    op_kwargs=dict(sql_file='PCORNetLoader_common.sql'),
    dag=dag)

demographic = PythonOperator(
    task_id='demographic',
    python_callable=run_sql,
    op_kwargs=dict(sql_file='PCORNetLoader_demographic.sql'),
    dag=dag)

demographic.set_upstream(common)

encounter = PythonOperator(
    task_id='encounter',
    python_callable=run_sql,
    op_kwargs=dict(sql_file='PCORNetLoader_encounter.sql'),
    dag=dag)

encounter.set_upstream(demographic)

harvest = PythonOperator(task_id='harvest',
                         python_callable=run_sql,
                         op_kwargs=dict(sql_file='PCORNetLoader_harvest.sql'),
                         dag=dag)

dep_encounter = [('condition', 'PCORNetLoader_condition.sql'),
                 ('death', 'PCORNetLoader_death.sql'),
                 ('death_cause', 'PCORNetLoader_death_cause.sql'),
                 ('diagnosis', 'PCORNetLoader_diagnosis.sql'),
                 ('dispensing', 'PCORNetLoader_dispensing.sql'),
                 ('enrollment', 'PCORNetLoader_enrollment.sql'),
                 ('labresultcm', 'PCORNetLoader_labresultcm.sql'),
                 ('pcornet_trial', 'PCORNetLoader_pcornet_trial.sql'),
                 ('prescribing', 'PCORNetLoader_prescribing.sql'),
                 ('procedure', 'PCORNetLoader_procedure.sql'),
                 ('pro_cm', 'PCORNetLoader_pro_cm.sql'),
                 ('vital', 'PCORNetLoader_vital.sql')]

for task_id, sql_file in dep_encounter:
    task = PythonOperator(task_id=task_id,
                          python_callable=run_sql,
                          op_kwargs=dict(sql_file=sql_file),
                          dag=dag)
    encounter.set_downstream(task)
    task.set_downstream(harvest)

report = PythonOperator(task_id='report',
                        python_callable=run_sql,
                        op_kwargs=dict(sql_file='PCORNetLoader_report.sql'),
                        dag=dag)

report.set_upstream(harvest)
